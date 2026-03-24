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

    log.info(f"[Main] Signal loop started in {mode.upper()} mode")

    while True:
        try:
            await asyncio.sleep(1)

            # ── Check if Polymarket market has expired (new 5-min window) ────
            if polymarket_feed.refresh_if_expired():
                log.info("[Main] Market window expired — re-fetching active market")
                ok = await polymarket_feed.fetch_active_market()
                if not ok:
                    log.warning("[Main] No active market found — waiting 15s")
                    await asyncio.sleep(15)
                    continue

            # ── Skip if there's already an open position ─────────────────────
            if executor.has_open_position:
                continue

            # ── Evaluate signal ───────────────────────────────────────────────
            signal = strategy.evaluate(binance_feed, polymarket_feed)

            if signal is not None:
                await executor.enter(signal, polymarket_feed)

            # ── Diagnostic ticker every 30s ───────────────────────────────
            now_diag = time.time()
            if now_diag - last_diag_ts >= 30:
                last_diag_ts = now_diag
                try:
                    p_now = binance_feed.price
                    p_then = binance_feed.price_n_seconds_ago(config["signal"]["price_window_seconds"])
                    pct = ((p_now - p_then) / p_then * 100.0) if (p_then and p_then > 0) else None
                    up_px = polymarket_feed.up_price
                    spread = polymarket_feed.spread
                    threshold = config["signal"]["price_change_threshold_pct"]
                    log.info(
                        f"[Diag] BTC={p_now:.2f} | 30s_chg={pct:+.4f}% (need ±{threshold}%) | "
                        f"PM_UP={up_px:.3f} | spread={spread:.4f}"
                        if pct is not None else
                        f"[Diag] BTC={p_now:.2f} | 30s_chg=N/A (building history) | "
                        f"PM_UP={up_px} | spread={spread}"
                    )
                except Exception:
                    pass

            # ── Periodic P&L summary ─────────────────────────────────────────
            now = time.time()
            if now - last_summary_ts >= summary_interval:
                print_pnl_summary(mode)
                last_summary_ts = now

        except asyncio.CancelledError:
            log.info("[Main] Signal loop cancelled")
            break
        except Exception as e:
            log_error("[Main] Unexpected error in signal loop", e)
            await asyncio.sleep(1)  # brief pause before retrying


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

    # ── Initialise executor (test or live) ────────────────────────────────────
    if mode == "test":
        executor = TestExecutor(config, strategy)
        log.info("[Main] Test executor initialised — simulated fills only")
    else:
        executor = LiveExecutor(config, strategy)
        log.info("[Main] Live executor initialised — REAL orders will be placed")

    # ── Launch all tasks concurrently ─────────────────────────────────────────
    # Both WebSocket feeds and the signal loop run as concurrent asyncio tasks.
    # They are never REST-polled — all data is pushed via WebSocket.
    log.info("[Main] Launching concurrent tasks: Binance WS, Polymarket WS, signal loop")
    await asyncio.gather(
        binance_feed.run(),
        polymarket_feed.run(),
        signal_loop(mode, config, binance_feed, polymarket_feed, strategy, executor),
        return_exceptions=True,  # don't let one task crash the others
    )


if __name__ == "__main__":
    args = parse_args()
    try:
        asyncio.run(main(args.mode))
    except KeyboardInterrupt:
        log.info("[Main] Bot stopped by user (KeyboardInterrupt)")
