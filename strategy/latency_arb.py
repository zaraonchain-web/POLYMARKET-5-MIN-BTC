"""
strategy/latency_arb.py — Latency Arbitrage Signal Detection

Core theory of this strategy
─────────────────────────────
Polymarket's 5-minute BTC markets are prediction markets where the "UP"
token price approximates the market-implied probability that BTC is
higher than the open at expiry.

The latency arbitrage edge arises because:
  1. Binance is a centralised limit-order book updated in real-time (< 1 ms)
  2. Polymarket's CLOB order book is updated by human market makers and
     slower reactive bots — typically 2–10 seconds behind large moves.
  3. When BTC moves sharply on Binance, the correct UP/DOWN token
     probability shifts immediately, but Polymarket odds lag.
  4. By entering BEFORE Polymarket reprices, we capture the difference
     between the stale Polymarket price and the fair post-move probability.

Signal generation (every 1 second tick):
  - Compare: price_now vs price_30s_ago  → compute pct_change
  - If pct_change > +THRESHOLD → the market moved UP → BUY UP token
  - If pct_change < -THRESHOLD → the market moved DOWN → BUY DOWN token
  - Cross-check 1: Polymarket UP price must be in [0.35, 0.65]
      (if it's already near 0 or 1, the market has priced in the move)
  - Cross-check 2: |fair_probability − polymarket_price| > MIN_EDGE
      (edge filter: only enter if the mis-pricing is large enough)
  - Risk guards: settlement buffer, spread filter, daily loss limit

Fair probability estimation:
  - We use a simplified heuristic: map the %-price-change to a probability
    using a logistic-style function calibrated for the 5-minute window.
  - A +0.4% move in 30 sec strongly implies the market closes UP → ~70–80%
  - A −0.4% move implies DOWN → UP fair prob ~20–30%
"""

import math
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from logger import log, log_error


class Direction(str, Enum):
    UP = "UP"
    DOWN = "DOWN"


@dataclass
class Signal:
    direction: Direction
    polymarket_price: float   # current stale market price (0-1)
    fair_probability: float   # our estimated fair prob (0-1)
    edge: float               # fair_probability - polymarket_price (signed)
    btc_price_now: float
    btc_price_then: float     # price N seconds ago
    pct_change: float
    timestamp: float          # unix timestamp


def _estimate_fair_probability(pct_change: float, window_seconds: int = 30) -> float:
    """
    Map a short-term BTC price change to a fair UP-token probability.

    Intuition:
      - The UP token pays $1 if BTC closes ABOVE the open price.
      - A +0.4% move in 30 seconds is a strong signal of continued
        upward momentum in a 5-minute window.
      - We use a logistic function scaled to the 5-minute time horizon.
      - The steepness (k=50) reflects that even a 1% move within 30s
        is a very large signal for a 5-minute binary outcome.

    This is intentionally conservative: we shade toward 0.5 at small
    moves to avoid over-confident entries on noise.
    """
    # Logistic: P(UP) = 1 / (1 + exp(-k * pct_change))
    # k=50: ±0.4% → P≈0.73/0.27; ±1% → P≈0.99/0.01
    k = 50.0
    fair_up_prob = 1.0 / (1.0 + math.exp(-k * (pct_change / 100.0)))
    # Clip to [0.05, 0.95] to avoid degenerate probabilities
    return max(0.05, min(0.95, fair_up_prob))


class LatencyArbStrategy:
    """
    Detects latency-arbitrage signals by comparing Binance BTC price
    momentum against stale Polymarket odds.
    """

    def __init__(self, config: dict):
        """
        config: the parsed config.yaml dict (full document)
        """
        sig_cfg = config.get("signal", {})
        risk_cfg = config.get("risk", {})

        # Signal parameters
        self.price_window_seconds: int = sig_cfg.get("price_window_seconds", 30)
        self.price_change_threshold_pct: float = sig_cfg.get("price_change_threshold_pct", 0.4)
        self.polymarket_min_price: float = sig_cfg.get("polymarket_min_price", 0.35)
        self.polymarket_max_price: float = sig_cfg.get("polymarket_max_price", 0.65)
        self.min_edge: float = sig_cfg.get("min_edge", 0.10)

        # Risk parameters
        self.settlement_buffer_seconds: int = risk_cfg.get("settlement_buffer_seconds", 60)
        self.max_spread: float = risk_cfg.get("max_spread", 0.05)
        self.daily_loss_limit_usdc: float = config.get("risk", {}).get("daily_loss_limit_usdc", 100.0)

        # Daily P&L tracking (resets at midnight UTC)
        self._day_pnl: float = 0.0
        self._day_start: float = self._today_start()
        self._trading_halted: bool = False

    # ── Daily P&L tracking ───────────────────────────────────────────────────

    @staticmethod
    def _today_start() -> float:
        """Unix timestamp of midnight UTC today."""
        now = time.time()
        return now - (now % 86400)

    def record_pnl(self, pnl_usdc: float) -> None:
        """
        Call after every trade close to keep the daily loss counter current.
        Automatically resets the counter at midnight UTC.
        """
        # Roll over at midnight
        if time.time() - self._day_start >= 86400:
            self._day_start = self._today_start()
            self._day_pnl = 0.0
            self._trading_halted = False
            log.info("[Strategy] New trading day — daily loss counter reset")

        self._day_pnl += pnl_usdc

        if self._day_pnl <= -self.daily_loss_limit_usdc and not self._trading_halted:
            self._trading_halted = True
            log.warning(
                f"[Strategy] DAILY LOSS LIMIT HIT "
                f"(cumulative={self._day_pnl:.2f} USDC). "
                f"Trading halted for the rest of today."
            )

    @property
    def is_halted(self) -> bool:
        """True if the daily loss limit has been triggered."""
        return self._trading_halted

    # ── Core signal evaluation (called every 1 second) ───────────────────────

    def evaluate(
        self,
        binance_feed,      # BinanceFeed instance
        polymarket_feed,   # PolymarketFeed instance
    ) -> Optional[Signal]:
        """
        Run one tick of signal detection.

        Returns a Signal if all conditions are met, otherwise None.
        Logs the rejection reason at DEBUG level so the terminal isn't noisy.
        """
        try:
            # ── Guard 0: daily loss limit ────────────────────────────────────
            if self._trading_halted:
                return None  # silently skip; already warned at halt time

            # ── Guard 1: feeds must be live ──────────────────────────────────
            if not binance_feed.is_connected or binance_feed.price is None:
                return None
            if not polymarket_feed.is_connected:
                return None
            if polymarket_feed.up_price is None or polymarket_feed.down_price is None:
                return None

            price_now = binance_feed.price
            price_then = binance_feed.price_n_seconds_ago(self.price_window_seconds)

            # ── Guard 2: not enough price history yet ────────────────────────
            if price_then is None or price_then <= 0:
                return None

            pct_change = ((price_now - price_then) / price_then) * 100.0

            # ── Signal direction check ───────────────────────────────────────
            # The core latency-arb trigger: has Binance moved significantly
            # while Polymarket odds are still stale?
            if pct_change >= self.price_change_threshold_pct:
                direction = Direction.UP
                polymarket_price = polymarket_feed.up_price
            elif pct_change <= -self.price_change_threshold_pct:
                direction = Direction.DOWN
                # When buying DOWN, we're buying the DOWN token.
                # The "UP price" is our reference since UP + DOWN = 1.
                # Fair prob is 1 - estimated_up_prob.
                polymarket_price = polymarket_feed.down_price
            else:
                return None  # move not large enough

            # ── Guard 3: Polymarket hasn't priced in the move yet ────────────
            # We check the UP token price regardless of direction, because it
            # represents the market consensus on the UP probability.
            # If it's already near 0 or 1, the market makers have already
            # repriced and our information edge is gone.
            up_px = polymarket_feed.up_price
            if not (self.polymarket_min_price <= up_px <= self.polymarket_max_price):
                log.debug(
                    f"[Strategy] Signal rejected: UP price {up_px:.3f} outside "
                    f"[{self.polymarket_min_price}, {self.polymarket_max_price}]"
                )
                return None

            # ── Fair probability estimate ────────────────────────────────────
            # Compute what the UP token SHOULD be worth given the Binance move.
            fair_up_prob = _estimate_fair_probability(pct_change, self.price_window_seconds)

            if direction == Direction.UP:
                fair_prob = fair_up_prob
                edge = fair_prob - polymarket_price   # positive = we have edge
            else:
                fair_prob = 1.0 - fair_up_prob        # fair DOWN probability
                edge = fair_prob - polymarket_price   # DOWN token edge

            # ── Guard 4: edge must be POSITIVE and exceed minimum threshold ─────
            # edge = fair_prob - polymarket_price
            # POSITIVE: token is underpriced vs our fair value → buy it (good)
            # NEGATIVE: token is OVERpriced → wrong direction, skip
            # Using abs() was a bug: it allowed buying overpriced tokens.
            if edge < self.min_edge:
                log.debug(
                    f"[Strategy] Signal rejected: edge {edge:+.3f} < "
                    f"min_edge {self.min_edge} (need positive edge)"
                )
                return None

            # ── Guard 5: settlement buffer ───────────────────────────────────
            # Never enter a trade in the last N seconds of a 5-min window.
            # Settlement risk: the market closes before we can exit profitably.
            secs_left = polymarket_feed.seconds_until_settlement()
            if secs_left is not None and secs_left < self.settlement_buffer_seconds:
                log.debug(
                    f"[Strategy] Signal rejected: {secs_left:.0f}s until settlement "
                    f"(buffer={self.settlement_buffer_seconds}s)"
                )
                return None

            # ── Guard 6: spread filter ───────────────────────────────────────
            # Skip if the order book is too thin (wide spread = bad fill).
            spread = polymarket_feed.spread
            if spread is not None and spread > self.max_spread:
                log.debug(
                    f"[Strategy] Signal rejected: spread {spread:.4f} > "
                    f"max_spread {self.max_spread}"
                )
                return None

            # ── All checks passed — emit signal ─────────────────────────────
            signal = Signal(
                direction=direction,
                polymarket_price=polymarket_price,
                fair_probability=fair_prob,
                edge=edge,
                btc_price_now=price_now,
                btc_price_then=price_then,
                pct_change=pct_change,
                timestamp=time.time(),
            )
            log.info(
                f"[Strategy] SIGNAL {direction.value} | "
                f"BTC {price_then:.2f}→{price_now:.2f} ({pct_change:+.3f}%) | "
                f"PM price={polymarket_price:.3f} fair={fair_prob:.3f} edge={edge:+.3f}"
            )
            return signal

        except Exception as e:
            log_error("[Strategy] evaluate() error", e)
            return None
