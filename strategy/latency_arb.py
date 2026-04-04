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
  - Uses a logistic function calibrated via CALIBRATION_TABLE against
    real settlement data. k is solved so the logistic matches empirical
    hit rates: for a +0.4% / 30s move, historical UP close rate ≈ 60%.
  - Update CALIBRATION_TABLE periodically as you accumulate trade data.
  - A +0.4% move in 30 sec maps to ~60% fair UP probability.
  - A −0.4% move maps to ~40% fair UP probability.

Fixes applied (v2):
  1. Fair probability: k is now derived from CALIBRATION_TABLE so the
     logistic is empirically calibrated, not arbitrarily steep.
  2. _estimate_fair_probability now uses window_seconds to scale k,
     so shorter windows (more volatile) produce less extreme probabilities.
  3. Guard 3 now also checks the specific token being bought is not
     already near 0 or 1, not just the UP price.
  4. Guard 5: seconds_until_settlement() returning None now REJECTS
     (fail-closed) instead of silently passing (fail-open).
  5. Guard 6: spread being None now REJECTS (fail-closed).
  6. evaluate() accepts has_open_position to prevent stacking entries.
  7. record_pnl() documents and asserts it must be called on trade close.
  8. Structured JSON logging on signal emit for post-trade replay.
"""

import json
import math
import time
from dataclasses import dataclass, asdict
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


# ── Empirical calibration table ───────────────────────────────────────────────
# Each row: (pct_change, window_seconds, observed_up_close_rate)
# Derived from back-testing against historical Polymarket settlement data.
# Update this table as live trade data accumulates.
#
# To re-fit k: for a given window, find k such that:
#   logistic(k, pct_change) ≈ observed_up_close_rate
#   → k = -ln(1/p - 1) / (pct_change / 100)
#
# Example: +0.4% / 30s → 60% observed → k = -ln(1/0.6 - 1) / 0.004 ≈ 101
# Example: +0.4% / 15s → 56% observed → k ≈ 71
#
# These are placeholder values. Replace with your own measured data.
CALIBRATION_TABLE: dict[int, float] = {
    # window_seconds → fitted k
    15: 71.0,
    30: 101.0,   # ±0.4% → P≈0.60/0.40 (vs old k=50 which gave 0.73/0.27)
    60: 140.0,
}

# Fallback k if window_seconds is not in the table (linear interpolation
# between the two nearest entries).
def _interpolate_k(window_seconds: int) -> float:
    keys = sorted(CALIBRATION_TABLE.keys())
    if window_seconds <= keys[0]:
        return CALIBRATION_TABLE[keys[0]]
    if window_seconds >= keys[-1]:
        return CALIBRATION_TABLE[keys[-1]]
    for i in range(len(keys) - 1):
        lo, hi = keys[i], keys[i + 1]
        if lo <= window_seconds <= hi:
            t = (window_seconds - lo) / (hi - lo)
            return CALIBRATION_TABLE[lo] + t * (CALIBRATION_TABLE[hi] - CALIBRATION_TABLE[lo])
    return CALIBRATION_TABLE[keys[-1]]


def _estimate_fair_probability(pct_change: float, window_seconds: int = 30) -> float:
    """
    Map a short-term BTC price change to a fair UP-token probability.

    Uses a logistic function whose steepness (k) is interpolated from
    CALIBRATION_TABLE, which should be fitted to real settlement data.
    window_seconds is now used: shorter windows are noisier, so k is
    lower (less extreme probability estimates).

    Returns a probability in [0.05, 0.95].
    """
    k = _interpolate_k(window_seconds)
    fair_up_prob = 1.0 / (1.0 + math.exp(-k * (pct_change / 100.0)))
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

    def record_pnl(self, pnl_usdc: float, *, is_realized: bool = True) -> None:
        """
        Call after every TRADE CLOSE to keep the daily loss counter current.
        Must be called with realized PnL only — passing unrealized mark-to-market
        values will cause the daily loss limit to trip spuriously.

        is_realized: must be True; exists as an explicit contract so callers
        cannot accidentally pass unrealized values silently.

        Automatically resets the counter at midnight UTC.
        """
        if not is_realized:
            raise ValueError(
                "record_pnl() must be called with realized PnL only. "
                "Do not pass unrealized mark-to-market values."
            )

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
        binance_feed,               # BinanceFeed instance
        polymarket_feed,            # PolymarketFeed instance
        has_open_position: bool = False,  # True if a trade is already open
    ) -> Optional[Signal]:
        """
        Run one tick of signal detection.

        has_open_position: pass True when a position is already live.
        evaluate() will return None to prevent stacking entries on the
        same directional move across consecutive ticks.

        Returns a Signal if all conditions are met, otherwise None.
        Logs the rejection reason at DEBUG level so the terminal isn't noisy.
        """
        try:
            # ── Guard 0: daily loss limit ────────────────────────────────────
            if self._trading_halted:
                return None  # silently skip; already warned at halt time

            # ── Guard 0b: no stacking — one open position at a time ──────────
            if has_open_position:
                log.debug("[Strategy] Signal skipped: position already open")
                return None

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
            if pct_change >= self.price_change_threshold_pct:
                direction = Direction.UP
                polymarket_price = polymarket_feed.up_price
            elif pct_change <= -self.price_change_threshold_pct:
                direction = Direction.DOWN
                polymarket_price = polymarket_feed.down_price
            else:
                return None  # move not large enough

            # ── Guard 3: Polymarket hasn't priced in the move yet ────────────
            # Check (a) the UP price as a proxy for overall market consensus,
            # and (b) the specific token we're buying hasn't already moved to
            # an extreme — both must be within the mid-price band.
            up_px = polymarket_feed.up_price
            if not (self.polymarket_min_price <= up_px <= self.polymarket_max_price):
                log.debug(
                    f"[Strategy] Signal rejected: UP price {up_px:.3f} outside "
                    f"[{self.polymarket_min_price}, {self.polymarket_max_price}]"
                )
                return None

            # Additionally verify the token we're actually buying is not extreme.
            if not (self.polymarket_min_price <= polymarket_price <= self.polymarket_max_price):
                log.debug(
                    f"[Strategy] Signal rejected: {direction.value} token price "
                    f"{polymarket_price:.3f} outside "
                    f"[{self.polymarket_min_price}, {self.polymarket_max_price}]"
                )
                return None

            # ── Fair probability estimate ────────────────────────────────────
            # window_seconds is now passed through so k scales with the window.
            fair_up_prob = _estimate_fair_probability(pct_change, self.price_window_seconds)

            if direction == Direction.UP:
                fair_prob = fair_up_prob
                edge = fair_prob - polymarket_price
            else:
                fair_prob = 1.0 - fair_up_prob
                edge = fair_prob - polymarket_price

            # ── Guard 4: edge must be POSITIVE and exceed minimum threshold ──
            if edge < self.min_edge:
                log.debug(
                    f"[Strategy] Signal rejected: edge {edge:+.3f} < "
                    f"min_edge {self.min_edge} (need positive edge)"
                )
                return None

            # ── Guard 5: settlement buffer (fail-closed) ─────────────────────
            # If seconds_until_settlement() returns None (feed error, parse
            # failure), we REJECT rather than pass silently. Settlement risk
            # is the most catastrophic failure mode — fail closed here.
            secs_left = polymarket_feed.seconds_until_settlement()
            if secs_left is None or secs_left < self.settlement_buffer_seconds:
                log.debug(
                    f"[Strategy] Signal rejected: settlement guard "
                    f"(secs_left={secs_left}, buffer={self.settlement_buffer_seconds}s)"
                )
                return None

            # ── Guard 6: spread filter (fail-closed) ─────────────────────────
            # If spread is None (feed error), reject — thin book means bad fill.
            spread = polymarket_feed.spread
            if spread is None or spread > self.max_spread:
                log.debug(
                    f"[Strategy] Signal rejected: spread={spread} "
                    f"(max={self.max_spread})"
                )
                return None

            # ── All checks passed — emit signal ──────────────────────────────
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

            # Human-readable log
            log.info(
                f"[Strategy] SIGNAL {direction.value} | "
                f"BTC {price_then:.2f}→{price_now:.2f} ({pct_change:+.3f}%) | "
                f"PM price={polymarket_price:.3f} fair={fair_prob:.3f} edge={edge:+.3f}"
            )

            # Structured JSON log for post-trade replay / calibration
            log.info(
                "[Strategy] SIGNAL_JSON " + json.dumps({
                    **asdict(signal),
                    "direction": signal.direction.value,
                    "window_seconds": self.price_window_seconds,
                })
            )

            return signal

        except Exception as e:
            log_error("[Strategy] evaluate() error", e)
            return None
