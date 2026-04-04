"""
execution/live_executor.py — Real trade execution via Polymarket CLOB (--mode live)

Uses py_clob_client (Polymarket's official Python SDK) to place orders.

Correct order flow per official docs and py-clob-client README:
  BUY  → MarketOrderArgs + create_market_order() + post_order(FOK)
  SELL → OrderArgs (limit at current price) + create_order() + post_order(GTC)
         Note: MarketOrderArgs does NOT support side=SELL (known SDK bug #145)

Authentication requires three env vars:
  POLYMARKET_PRIVATE_KEY    — wallet private key
  POLYMARKET_API_KEY        — CLOB API key
  POLYMARKET_API_SECRET     — CLOB API secret
  POLYMARKET_API_PASSPHRASE — CLOB API passphrase
  POLYMARKET_FUNDER         — proxy wallet address (the address that holds USDC)
"""

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Optional

from logger import log, log_trade, log_error
from strategy.latency_arb import Signal, Direction
from execution.utils import taker_fee_rate


def _import_clob_client():
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import MarketOrderArgs, OrderArgs, OrderType, ApiCreds
        from py_clob_client.order_builder.constants import BUY, SELL
        return ClobClient, MarketOrderArgs, OrderArgs, OrderType, ApiCreds, BUY, SELL
    except ImportError as e:
        raise ImportError(
            "py_clob_client is required for live mode. "
            "Install it with: pip install py_clob_client"
        ) from e


@dataclass
class LivePosition:
    market_id: str
    direction: Direction
    entry_price: float
    entry_time: float
    size_usdc: float
    edge_at_entry: float
    order_id: str
    token_id: str
    shares: float          # number of shares held (needed for sell sizing)
    fee_rate: float        # live fee rate at time of entry (fraction)


class LiveExecutor:

    def __init__(self, config: dict, strategy):
        pos_cfg  = config.get("position", {})
        exec_cfg = config.get("execution", {})

        self.hold_seconds:        int   = pos_cfg.get("hold_seconds", 240)
        self.take_profit_pct:     float = pos_cfg.get("take_profit_pct", 0.15)
        self.max_trade_size_usdc: float = pos_cfg.get("max_trade_size_usdc", 20.0)
        self.max_concurrent:      int   = pos_cfg.get("max_concurrent", 1)
        self.poll_interval:       int   = exec_cfg.get("poll_interval_seconds", 10)
        self.fill_timeout:        int   = exec_cfg.get("fill_timeout_seconds", 30)
        self.chain_id:            int   = config.get("polymarket", {}).get("chain_id", 137)

        self.strategy = strategy
        self.open_position: Optional[LivePosition] = None
        self._client = None

    @property
    def has_open_position(self) -> bool:
        return self.open_position is not None

    def _get_client(self):
        """
        Lazily initialise ClobClient from environment variables.
        Requires: POLYMARKET_PRIVATE_KEY, POLYMARKET_API_KEY,
                  POLYMARKET_API_SECRET, POLYMARKET_API_PASSPHRASE, POLYMARKET_FUNDER
        """
        if self._client is not None:
            return self._client

        ClobClient, _, _, _, ApiCreds, _, _ = _import_clob_client()

        private_key  = os.environ.get("POLYMARKET_PRIVATE_KEY")
        api_key      = os.environ.get("POLYMARKET_API_KEY")
        api_secret   = os.environ.get("POLYMARKET_API_SECRET")
        api_pass     = os.environ.get("POLYMARKET_API_PASSPHRASE")
        funder       = os.environ.get("POLYMARKET_FUNDER")  # proxy wallet address

        missing = [k for k, v in {
            "POLYMARKET_PRIVATE_KEY":    private_key,
            "POLYMARKET_API_KEY":        api_key,
            "POLYMARKET_API_SECRET":     api_secret,
            "POLYMARKET_API_PASSPHRASE": api_pass,
            "POLYMARKET_FUNDER":         funder,
        }.items() if not v]

        if missing:
            raise EnvironmentError(
                "Missing required environment variables: {}".format(", ".join(missing))
            )

        # signature_type=1 for Magic/email wallet proxy (most common for Polymarket)
        # signature_type=2 for MetaMask/browser wallet proxy
        # signature_type=0 for raw EOA (no proxy)
        sig_type = int(os.environ.get("POLYMARKET_SIGNATURE_TYPE", "1"))

        creds = ApiCreds(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_pass,
        )

        self._client = ClobClient(
            host="https://clob.polymarket.com",
            key=private_key,
            chain_id=self.chain_id,
            creds=creds,
            signature_type=sig_type,
            funder=funder,
        )
        log.info("[LiveExecutor] ClobClient initialised (chain_id={} sig_type={})".format(
            self.chain_id, sig_type))
        return self._client

    # ── Entry ─────────────────────────────────────────────────────────────────

    async def enter(self, signal: Signal, polymarket_feed) -> bool:
        if self.has_open_position:
            return False

        try:
            ClobClient, MarketOrderArgs, OrderArgs, OrderType, ApiCreds, BUY, SELL = _import_clob_client()
            client = self._get_client()

            if signal.direction == Direction.UP:
                token_id  = polymarket_feed.up_token_id
                ref_price = polymarket_feed.up_price
            else:
                token_id  = polymarket_feed.down_token_id
                ref_price = polymarket_feed.down_price

            if token_id is None or ref_price is None or ref_price <= 0:
                log_error("[LiveExecutor] Invalid token_id or ref_price")
                return False

            # BUY: MarketOrderArgs with amount in USDC, order_type=FOK
            # Per official README: create_market_order() then post_order(FOK)
            mo = MarketOrderArgs(
                token_id=token_id,
                amount=self.max_trade_size_usdc,   # dollar amount, not shares
                order_type=OrderType.FOK,
            )

            log.info(
                "[LIVE] Placing MARKET BUY {} | token={}... amount=${:.2f} ref_price={:.4f}".format(
                    signal.direction.value, token_id[:20], self.max_trade_size_usdc, ref_price
                )
            )

            loop = asyncio.get_event_loop()
            signed = await loop.run_in_executor(
                None, lambda: client.create_market_order(mo)
            )
            response = await loop.run_in_executor(
                None, lambda: client.post_order(signed, OrderType.FOK)
            )

            order_id = self._extract_order_id(response)
            if not order_id:
                log_error("[LiveExecutor] Order placement failed: {}".format(response))
                return False

            log.info("[LIVE] Order placed: {}".format(order_id))

            # Estimate shares from USDC amount and ref_price
            # Actual fill may differ slightly due to FOK matching
            fill_price, shares = await self._extract_fill(client, response, ref_price)

            pos = LivePosition(
                market_id=polymarket_feed.market_id or "unknown",
                direction=signal.direction,
                entry_price=fill_price,
                entry_time=time.time(),
                size_usdc=self.max_trade_size_usdc,
                edge_at_entry=signal.edge,
                order_id=order_id,
                token_id=token_id,
                shares=shares,
                fee_rate=getattr(polymarket_feed, "fee_rate", None),
            )
            self.open_position = pos

            log.info(
                "[LIVE] Position OPEN | {} fill={:.4f} shares={:.2f} market={}".format(
                    signal.direction.value, fill_price, shares, pos.market_id
                )
            )

            asyncio.create_task(self._monitor_position(client, polymarket_feed))
            return True

        except Exception as e:
            log_error("[LiveExecutor] enter() failed", e)
            return False

    async def _extract_fill(self, client, response: dict, ref_price: float):
        """
        Extract fill price and shares from the FOK order response.
        FOK orders are either fully filled or cancelled immediately —
        no need to poll. The response contains takingAmount / makingAmount.
        """
        try:
            # response fields from Polymarket: takingAmount (USDC), makingAmount (shares)
            taking = float(response.get("takingAmount") or 0)  # USDC spent
            making = float(response.get("makingAmount") or 0)  # shares received

            if making > 0 and taking > 0:
                actual_price = taking / making
                log.info("[LIVE] Fill: {:.2f} USDC → {:.4f} shares @ {:.4f}".format(
                    taking, making, actual_price))
                return actual_price, making

            # Fallback: estimate from ref_price
            shares = self.max_trade_size_usdc / ref_price
            return ref_price, shares

        except Exception:
            shares = self.max_trade_size_usdc / ref_price
            return ref_price, shares

    # ── Position monitor ──────────────────────────────────────────────────────

    async def _monitor_position(self, client, polymarket_feed) -> None:
        pos = self.open_position
        if pos is None:
            return

        try:
            while self.open_position is not None:
                await asyncio.sleep(1)
                pos = self.open_position
                if pos is None:
                    break

                # Use the BID as our exit reference — that's what we receive
                # as a taker selling. Using mid triggers take-profit too early
                # since the real fill comes in ~0.01 below mid.
                if pos.direction == Direction.UP:
                    bid_price = polymarket_feed._best_bid
                    mid_price = polymarket_feed.up_price
                    exit_ref  = bid_price if bid_price is not None else (
                        round(mid_price - 0.005, 4) if mid_price is not None else None
                    )
                else:
                    bid_price = getattr(polymarket_feed, "_down_best_bid", None)
                    mid_price = polymarket_feed.down_price
                    exit_ref  = bid_price if bid_price is not None else (
                        round(mid_price - 0.005, 4) if mid_price is not None else None
                    )

                if exit_ref is None:
                    continue

                hold_elapsed = time.time() - pos.entry_time
                pct_profit   = (exit_ref - pos.entry_price) / pos.entry_price

                # 30s minimum hold before checking take-profit — prevents
                # an immediate exit triggered by a stale WS tick or own-order
                # book impact in the seconds right after entry.
                if hold_elapsed >= 30 and pct_profit >= self.take_profit_pct:
                    await self._close_position(client, exit_ref, hold_elapsed, "take-profit")
                    return

                if hold_elapsed >= self.hold_seconds:
                    await self._close_position(client, exit_ref, hold_elapsed, "hold-timeout")
                    return

                secs_left = polymarket_feed.seconds_until_settlement()
                if secs_left is not None and secs_left <= 5:
                    await self._close_position(client, exit_ref, hold_elapsed, "settlement")
                    return

        except Exception as e:
            log_error("[LiveExecutor] _monitor_position error", e)
            self.open_position = None

    async def _close_position(self, client, exit_price: float, hold_seconds: float, reason: str) -> None:
        """
        Place a limit SELL order at current price to exit.
        Uses OrderArgs + GTC because MarketOrderArgs doesn't support side=SELL (SDK bug #145).
        Placing a limit sell at the current best bid ensures a fast fill.
        """
        pos = self.open_position
        if pos is None:
            return

        try:
            _, MarketOrderArgs, OrderArgs, OrderType, ApiCreds, BUY, SELL = _import_clob_client()

            # Sell at slightly below mid to ensure a fill (taker order)
            # Clamp to valid range [0.01, 0.99]
            sell_price = round(max(0.01, min(0.99, exit_price - 0.01)), 2)

            log.info(
                "[LIVE] Placing LIMIT SELL {} | reason={} token={}... shares={:.4f} price={:.4f}".format(
                    pos.direction.value, reason, pos.token_id[:20], pos.shares, sell_price
                )
            )

            sell_args = OrderArgs(
                token_id=pos.token_id,
                price=sell_price,
                size=round(pos.shares, 4),
                side=SELL,
            )

            loop = asyncio.get_event_loop()
            try:
                signed_sell = await loop.run_in_executor(
                    None, lambda: client.create_order(sell_args)
                )
                sell_resp = await loop.run_in_executor(
                    None, lambda: client.post_order(signed_sell, OrderType.GTC)
                )
                log.info("[LIVE] Sell order response: {}".format(sell_resp))
            except Exception as e:
                log_error("[LiveExecutor] Sell order failed — position may need manual close", e)

            # P&L uses sell_price (actual fill) not exit_price (the bid that
            # triggered the decision). sell_price = exit_price - 0.01, so using
            # exit_price would overstate net P&L by ~0.01 * shares per trade.
            # Fees are charged on USDC notional for both entry and exit legs.
            entry_fee_usdc = pos.size_usdc * taker_fee_rate(pos.entry_price, pos.fee_rate)
            exit_fee_usdc  = pos.size_usdc * taker_fee_rate(sell_price, pos.fee_rate)
            total_fees     = entry_fee_usdc + exit_fee_usdc

            gross_pnl = (sell_price - pos.entry_price) * pos.shares
            pnl_usdc  = gross_pnl - total_fees

            log.info(
                "[LIVE] EXIT {} | reason={} entry={:.4f} fill={:.4f} hold={:.0f}s "
                "gross={:+.4f} fees=-{:.3f} net={:+.4f} USDC".format(
                    pos.direction.value, reason, pos.entry_price,
                    sell_price, hold_seconds, gross_pnl, total_fees, pnl_usdc
                )
            )

            log_trade(
                market_id=pos.market_id,
                direction=pos.direction.value,
                entry_price=pos.entry_price,
                exit_price=sell_price,
                hold_seconds=hold_seconds,
                pnl_usdc=pnl_usdc,
                edge_at_entry=pos.edge_at_entry,
                mode="live",
                reason=reason,
            )

            self.strategy.record_pnl(pnl_usdc)

        except Exception as e:
            log_error("[LiveExecutor] _close_position error", e)
        finally:
            self.open_position = None

    @staticmethod
    def _extract_order_id(response) -> Optional[str]:
        if response is None:
            return None
        if isinstance(response, dict):
            return (response.get("orderID")
                    or response.get("order_id")
                    or response.get("id"))
        if hasattr(response, "orderID"):
            return response.orderID
        if hasattr(response, "order_id"):
            return response.order_id
        return str(response) if response else None
