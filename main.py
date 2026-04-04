"""
main.py — Entry point for the Polymarket Latency Arbitrage Bot

Usage:
    python main.py --mode test    # simulation with real live data, no orders
    python main.py --mode live    # real orders via Polymarket CLOB

The main loop:
    1. Load config.yaml
    2. Start Binance WebSocket feed (asyncio task)
    3. Fetch active Polymarket 5-min BTC market (REST on startup)
    4. Start Polymarket CLOB WebSocket feed (asyncio task)
    5. Every 1 second: evaluate signal → execute if conditions met
    6. Every 60 seconds: print P&L summary to terminal
    7. All exceptions are caught and logged; the loop never exits

Fixes applied (v2):
  1. executor.has_open_position is now passed into strategy.evaluate() so
     the strategy's deduplication guard is actually wired up.
  2. Market expiry resets via polymarket_feed.reset_book() instead of
     poking private attributes directly.
  3. _reconnect_triggered replaced with a proper asyncio.Event so the
     reconnect guard is state-based, not time-based.
  4. asyncio.gather return values are inspected — dead feed tasks are
     logged as errors rather than swallowed silently.
  5. Diagnostic ticker None-checks price before formatting, and logs
     a warning instead of silently passing on failure.
  6. Market refresh is debounced: once a refresh is in-flight, subsequent
     ticks skip the refresh block until it completes.
"""

import argparse
import asyncio
import sys
import time
from pathlib import Path

import yaml
from dotenv import load_dotenv

# ── Local imports ────────────────────────────────────────────────────────────
from feeds.binance import BinanceFeed
from feeds.polymarket import PolymarketFeed
from strategy.latency_arb import LatencyArbStrategy
from execution.test_executor import TestExecutor
from execution.live_executor import LiveExecutor
from logger import log, log_error, print_pnl_summary
from telegram_notify import notify_startup, notify_signal


def parse_args():
    parser = argparse.ArgumentParser(
        description="Polymarket Latency Arbitrage Bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --mode test    # Simulate trades using real live data
  python main.py --mode live    # Place real orders (requires API keys in env)
        """,
    )
    parser.add_argument(
        "--mode",
        choices=["test", "live"],
        required=True,
        help="'test' = simulation mode (no real orders); 'live' = real CLOB orders",
    )
    return parser.parse_args()


def load_config() -> dict:
    config_path = Path(__file__).parent / "config.yaml"
    if not config_path.exists():
        log.error(f"config.yaml not found at {config_path}")
        sys.exit(1)
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


async def signal_loop(
    mode: str,
    config: dict,
    binance_feed: BinanceFeed,
    polymarket_feed: PolymarketFeed,
    strategy: LatencyArbStrategy,
    executor,
) -> None:
    """
    Core 1-second tick loop.
    Evaluates the latency-arb signal and triggers execution if conditions are met.
    Also drives the periodic P&L summary printer.
    """
    summary_interval = config.get("logging", {}).get("summary_interval_seconds", 60)
    last_summary_ts = time.time()
    last_diag_ts = time.time()

    # Guards against launching concurrent refreshes if fetch_active_market()
    # takes longer than one tick (or the market stays expired for several ticks).
    _refresh_in_flight = False

    log.info(f"[Main] Signal loop started in {mode.upper()} mode")
    notify_startup(mode)

    while True:
        try:
            await asyncio.sleep(1)

            # ── Check if Polymarket market has expired (new 5-min window) ────
            # Debounced: skip if a refresh is already in progress.
            if polymarket_feed.refresh_if_expired() and not _refresh_in_flight:
                _refresh_in_flight = True
                log.info("[Main] Market window expired — re-fetching active market")

                # Delegate book reset to the feed (no private-attribute poking).
                polymarket_feed.reset_book()

                ok = await polymarket_feed.fetch_active_market()

                if ok:
                    # Signal the WS task to reconnect to the new token IDs.
                    # Use an asyncio.Event so state is explicit, not time-based.
                    polymarket_feed.request_reconnect()
                else:
                    log.warning("[Main] No active market found — waiting 15s")
                    await asyncio.sleep(15)

                _refresh_in_flight = False
                if not ok:
                    continue

            # ── Skip tick if a refresh is in flight ──────────────────────────
            if _refresh_in_flight:
                continue

            # ── Evaluate signal (pass open-position state into strategy) ──────
            # has_open_position is checked both here (fast path) and inside
            # strategy.evaluate() (redundant safety net). Both must agree.
            has_open = executor.has_open_position

            if not has_open:
                signal = strategy.evaluate(
                    binance_feed,
                    polymarket_feed,
                    has_open_position=has_open,
                )
                if signal is not None:
                    notify_signal(
                        mode=mode,
                        direction=signal.direction.value,
                        btc_pct_change=signal.pct_change,
                        pm_price=signal.polymarket_price,
                        fair_prob=signal.fair_probability,
                        edge=signal.edge,
                    )
                    await executor.enter(signal, polymarket_feed)

            # ── Diagnostic ticker every 30s ───────────────────────────────────
            now = time.time()
            if now - last_diag_ts >= 30:
                last_diag_ts = now
                try:
                    p_now = binance_feed.price
                    if p_now is None:
                        log.warning("[Diag] Binance price unavailable")
                    else:
                        window = config["signal"]["price_window_seconds"]
                        p_then = binance_feed.price_n_seconds_ago(window)
                        threshold = config["signal"]["price_change_threshold_pct"]
                        up_px = polymarket_feed.up_price
                        spread = polymarket_feed.spread

                        if p_then and p_then > 0:
                            pct = (p_now - p_then) / p_then * 100.0
                            log.info(
                                f"[Diag] BTC={p_now:.2f} | "
                                f"{window}s_chg={pct:+.4f}% (need ±{threshold}%) | "
                                f"PM_UP={up_px} | spread={spread}"
                            )
                        else:
                            log.info(
                                f"[Diag] BTC={p_now:.2f} | "
                                f"{window}s_chg=N/A (building history) | "
                                f"PM_UP={up_px} | spread={spread}"
                            )
                except Exception as e:
                    log.warning(f"[Diag] Diagnostic ticker error: {e}")

            # ── Periodic P&L summary ──────────────────────────────────────────
            if now - last_summary_ts >= summary_interval:
                print_pnl_summary(mode)
                last_summary_ts = now

        except asyncio.CancelledError:
            log.info("[Main] Signal loop cancelled")
            break
        except Exception as e:
            log_error("[Main] Unexpected error in signal loop", e)
            await asyncio.sleep(1)


async def main(mode: str) -> None:
    # ── Load environment variables (.env file if present) ───────────────────
    load_dotenv()

    # ── Load configuration ────────────────────────────────────────────────────
    config = load_config()
    log.info(f"[Main] Config loaded. Starting in {mode.upper()} mode.")

    # ── Validate live mode credentials early ─────────────────────────────────
    if mode == "live":
        import os
        if not os.environ.get("POLYMARKET_PRIVATE_KEY"):
            log.error("POLYMARKET_PRIVATE_KEY not set. Export it before live mode.")
            sys.exit(1)
        if not os.environ.get("POLYMARKET_API_KEY"):
            log.error("POLYMARKET_API_KEY not set. Export it before live mode.")
            sys.exit(1)
        log.info("[Main] Live mode credentials found.")

    # ── Initialise feeds ─────────────────────────────────────────────────────
    binance_feed = BinanceFeed(
        price_window_seconds=config["signal"]["price_window_seconds"]
    )
    polymarket_feed = PolymarketFeed(
        clob_rest_url=config["polymarket"]["clob_rest_url"],
        clob_ws_url=config["polymarket"]["clob_ws_url"],
    )

    # ── Fetch the active 5-minute BTC market before starting ─────────────────
    log.info("[Main] Fetching active Polymarket 5-minute BTC market...")
    ok = await polymarket_feed.fetch_active_market()
    if not ok:
        log.warning("[Main] No active market found on startup — bot will retry automatically")

    # ── Initialise strategy ───────────────────────────────────────────────────
    strategy = LatencyArbStrategy(config)

    # ── Initialise executor (test or live) ───────────────────────────────────
    if mode == "test":
        executor = TestExecutor(config, strategy)
        log.info("[Main] Test executor initialised — simulated fills only")
    else:
        executor = LiveExecutor(config, strategy)
        log.info("[Main] Live executor initialised — REAL orders will be placed")

    # ── Launch all tasks concurrently ─────────────────────────────────────────
    # return_exceptions=True prevents one crashed task from cancelling the
    # others mid-run. We inspect results after gather() so crashes don't
    # vanish silently.
    log.info("[Main] Launching concurrent tasks: Binance WS, Polymarket WS, signal loop")
    results = await asyncio.gather(
        binance_feed.run(),
        polymarket_feed.run(),
        signal_loop(mode, config, binance_feed, polymarket_feed, strategy, executor),
        return_exceptions=True,
    )

    # ── Report any task that exited with an exception ─────────────────────────
    task_names = ["BinanceFeed.run", "PolymarketFeed.run", "signal_loop"]
    for name, result in zip(task_names, results):
        if isinstance(result, Exception):
            log_error(f"[Main] Task '{name}' exited with exception", result)
        elif isinstance(result, BaseException):
            log.error(f"[Main] Task '{name}' exited with: {result!r}")


if __name__ == "__main__":
    args = parse_args()
    try:
        asyncio.run(main(args.mode))
    except KeyboardInterrupt:
        log.info("[Main] Bot stopped by user (KeyboardInterrupt)")
