"""
execution/test_executor.py — Simulated trade execution (--mode test)

REALISTIC SIMULATION MODE
--------------------------
This executor simulates what live trading would actually cost:

1. ENTRY fills at the ASK price (you are a taker buying at the offer)
2. EXIT fills at the BID price (you are a taker selling at the bid)
3. TAKER FEES: Polymarket charges a taker fee on every CLOB order.
   For the 5-min BTC markets, the fee schedule is:
     fee = 0.01 * (1 - |token_price - 0.5| / 0.5)
   This peaks at ~1% at 50/50 odds and approaches 0% near 0 or 1.
   Fee is charged on the USDC notional of the order.
4. SPREAD COST is implicitly captured by filling at ask/bid rather than mid.

Why this matters vs. naive mid-price simulation:
  - At a spread of 0.01 and entry price 0.60:
      Mid fill:  $20 buys tokens worth $20 at mid
      Ask fill:  $20 buys tokens at 0.605 (ask) — you pay ~0.8% more
  - Taker fee at 50/50 odds: ~1.0% of $20 = $0.20 per entry
  - Total round-trip cost (spread + 2x fees): ~$0.50-0.70 per trade
  - Over hundreds of trades this dominates P&L

CSV columns include fee_usdc so you can see the exact cost per trade.
"""

import asyncio
import time
from dataclasses import dataclass
from typing import Optional

from logger import log, log_trade, log_error
from strategy.latency_arb import Signal, Direction


def _taker_fee_rate(token_price: float) -> float:
    """
    Polymarket CLOB taker fee rate for the 5-minute BTC Up/Down markets.

    Fee schedule (from Polymarket docs, effective Feb 2026):
      fee_rate = 0.01 * (1 - |p - 0.5| / 0.5)
    where p is the token price (0-1).

    Examples:
      p=0.50 (50/50)  → fee = 1.0%
      p=0.60          → fee = 0.8%
      p=0.70          → fee = 0.6%
      p=0.80          → fee = 0.4%
      p=0.90          → fee = 0.2%
      p=0.95          → fee = 0.1%

    Fee is charged on the USDC notional (size_usdc), not on shares.
    """
    p = max(0.01, min(0.99, token_price))
    return 0.01 * (1.0 - abs(p - 0.5) / 0.5)


@dataclass
class SimPosition:
    """Represents one open simulated position."""
    market_id:     str
    direction:     Direction
    entry_price:   float    # realistic ask price at entry
    entry_time:    float    # unix timestamp
    size_usdc:     float
    edge_at_entry: float
    entry_fee:     float    # USDC fee paid on entry
    signal:        Signal


class TestExecutor:
    """
    Simulates Polymarket order fills with REALISTIC costs:
      - Entry at ASK price (taker buying)
      - Exit at BID price (taker selling)
      - Taker fee on both legs
    """

    def __init__(self, config: dict, strategy):
        pos_cfg  = config.get("position", {})
        self.hold_seconds:       int   = pos_cfg.get("hold_seconds", 240)
        self.take_profit_pct:    float = pos_cfg.get("take_profit_pct", 0.15)
        self.max_trade_size_usdc: float = pos_cfg.get("max_trade_size_usdc", 20.0)
        self.max_concurrent:     int   = pos_cfg.get("max_concurrent", 1)

        self.strategy = strategy
        self.open_position: Optional[SimPosition] = None

    @property
    def has_open_position(self) -> bool:
        return self.open_position is not None

    async def enter(self, signal: Signal, polymarket_feed) -> bool:
        """
        Simulate realistic entry:
          - Fill at ASK price for the token we're buying
          - Deduct taker fee on entry
        """
        if self.has_open_position:
            return False

        try:
            if signal.direction == Direction.UP:
                # Buying UP token — we pay the ASK (taker)
                ask = polymarket_feed._best_ask
                # Fallback: mid + half-spread estimate
                if ask is None and polymarket_feed.up_price is not None:
                    ask = round(polymarket_feed.up_price + 0.005, 4)
                fill_price = ask
            else:
                # Buying DOWN token — use DOWN token's best ask directly
                # _down_best_ask is tracked in the persistent book
                down_ask = getattr(polymarket_feed, "_down_best_ask", None)
                if down_ask is not None:
                    fill_price = down_ask
                elif polymarket_feed.down_price is not None:
                    fill_price = round(polymarket_feed.down_price + 0.005, 4)
                else:
                    fill_price = None

            # Sanity check: fill price must be in (0.01, 0.99)
            if fill_price is None or not (0.01 < fill_price < 0.99):
                log_error(
                    "[TestExecutor] Skipping — fill price out of range: {}".format(fill_price)
                )
                return False

            # Taker fee on entry (charged on USDC notional)
            entry_fee_rate = _taker_fee_rate(fill_price)
            entry_fee_usdc = self.max_trade_size_usdc * entry_fee_rate

            pos = SimPosition(
                market_id=polymarket_feed.market_id or "unknown",
                direction=signal.direction,
                entry_price=fill_price,
                entry_time=time.time(),
                size_usdc=self.max_trade_size_usdc,
                edge_at_entry=signal.edge,
                entry_fee=entry_fee_usdc,
                signal=signal,
            )
            self.open_position = pos

            log.info(
                "[TEST] ENTER {} | fill={:.4f} (ask) size=${:.2f} "
                "fee=${:.3f} ({:.2f}%) edge={:+.3f}".format(
                    signal.direction.value,
                    fill_price,
                    self.max_trade_size_usdc,
                    entry_fee_usdc,
                    entry_fee_rate * 100,
                    signal.edge,
                )
            )

            asyncio.create_task(self._monitor_position(polymarket_feed))
            return True

        except Exception as e:
            log_error("[TestExecutor] enter() error", e)
            return False

    async def _monitor_position(self, polymarket_feed) -> None:
        """
        Polls every second. Exit conditions:
          1. Take-profit crossed (measured against BID, not mid)
          2. Hold time expired
          3. Market about to settle
        """
        pos = self.open_position
        if pos is None:
            return

        try:
            while self.open_position is not None:
                await asyncio.sleep(1)
                pos = self.open_position
                if pos is None:
                    break

                # Exit price = BID (we sell at bid as a taker)
                if pos.direction == Direction.UP:
                    exit_price = polymarket_feed._best_bid
                    if exit_price is None and polymarket_feed.up_price is not None:
                        exit_price = round(polymarket_feed.up_price - 0.005, 4)
                else:
                    # Selling DOWN token — use DOWN token's best bid directly
                    down_bid = getattr(polymarket_feed, "_down_best_bid", None)
                    if down_bid is not None:
                        exit_price = down_bid
                    elif polymarket_feed.down_price is not None:
                        exit_price = round(polymarket_feed.down_price - 0.005, 4)
                    else:
                        exit_price = None

                if exit_price is None:
                    continue

                hold_elapsed = time.time() - pos.entry_time

                # P&L based on bid vs ask fill (realistic)
                pct_profit = (exit_price - pos.entry_price) / pos.entry_price

                # Exit 1: take-profit
                if pct_profit >= self.take_profit_pct:
                    await self._close_position(exit_price, hold_elapsed, reason="take-profit")
                    return

                # Exit 2: hold time expired
                if hold_elapsed >= self.hold_seconds:
                    await self._close_position(exit_price, hold_elapsed, reason="hold-timeout")
                    return

                # Exit 3: settlement imminent
                secs_left = polymarket_feed.seconds_until_settlement()
                if secs_left is not None and secs_left <= 5:
                    await self._close_position(exit_price, hold_elapsed, reason="settlement")
                    return

        except Exception as e:
            log_error("[TestExecutor] _monitor_position error", e)
            self.open_position = None

    async def _close_position(self, exit_price: float, hold_seconds: float, reason: str) -> None:
        """Compute realistic P&L including both legs of fees, log trade."""
        pos = self.open_position
        if pos is None:
            return

        try:
            # Taker fee on exit
            exit_fee_rate = _taker_fee_rate(exit_price)
            exit_fee_usdc = pos.size_usdc * exit_fee_rate
            total_fee     = pos.entry_fee + exit_fee_usdc

            # Gross P&L: price move on the token position
            # We bought $size_usdc worth of tokens at entry_price
            # Number of tokens = size_usdc / entry_price
            # Token value at exit = tokens * exit_price
            # Gross P&L = tokens * (exit_price - entry_price)
            tokens    = pos.size_usdc / pos.entry_price
            gross_pnl = tokens * (exit_price - pos.entry_price)

            # Net P&L after fees
            net_pnl = gross_pnl - total_fee

            log.info(
                "[TEST] EXIT {} | reason={} entry={:.4f} exit={:.4f} "
                "hold={:.0f}s gross={:+.4f} fees=-{:.3f} net={:+.4f} USDC".format(
                    pos.direction.value, reason,
                    pos.entry_price, exit_price, hold_seconds,
                    gross_pnl, total_fee, net_pnl,
                )
            )

            log_trade(
                market_id=pos.market_id,
                direction=pos.direction.value,
                entry_price=pos.entry_price,
                exit_price=exit_price,
                hold_seconds=hold_seconds,
                pnl_usdc=net_pnl,
                edge_at_entry=pos.edge_at_entry,
                mode="test",
            )

            self.strategy.record_pnl(net_pnl)

        except Exception as e:
            log_error("[TestExecutor] _close_position error", e)
        finally:
            self.open_position = None
