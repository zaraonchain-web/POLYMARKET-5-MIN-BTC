"""
logger.py — Handles all file-based output:
  - results.csv: every trade (simulated or real) with P&L schema
  - error.log: exceptions caught anywhere in the main loop
  - Terminal P&L summary printer
"""

import csv
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


# ── File paths (resolved relative to project root) ──────────────────────────
_BASE_DIR = Path(__file__).parent
RESULTS_CSV = _BASE_DIR / "results.csv"
ERROR_LOG = _BASE_DIR / "error.log"

# ── CSV Schema ───────────────────────────────────────────────────────────────
RESULTS_FIELDNAMES = [
    "timestamp",
    "market_id",
    "direction",       # "UP" or "DOWN"
    "entry_price",
    "exit_price",
    "hold_seconds",
    "pnl_usdc",
    "edge_at_entry",
    "mode",            # "test" or "live"
]

# ── Error logger (file handler) ──────────────────────────────────────────────
_error_logger = logging.getLogger("error")
_error_logger.setLevel(logging.ERROR)
if not _error_logger.handlers:
    _fh = logging.FileHandler(ERROR_LOG)
    _fh.setFormatter(logging.Formatter("%(asctime)s [ERROR] %(message)s"))
    _error_logger.addHandler(_fh)
    # Also emit errors to stderr so they're visible
    _sh = logging.StreamHandler()
    _sh.setFormatter(logging.Formatter("%(asctime)s [ERROR] %(message)s"))
    _error_logger.addHandler(_sh)

# ── Terminal logger (INFO) ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("bot")


def _ensure_csv_header():
    """Create results.csv with header row if it doesn't exist yet."""
    if not RESULTS_CSV.exists() or RESULTS_CSV.stat().st_size == 0:
        with open(RESULTS_CSV, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=RESULTS_FIELDNAMES)
            writer.writeheader()


def log_trade(
    *,
    market_id: str,
    direction: str,
    entry_price: float,
    exit_price: float,
    hold_seconds: float,
    pnl_usdc: float,
    edge_at_entry: float,
    mode: str,
    reason: str = "unknown",
    timestamp: Optional[datetime] = None,
) -> None:
    """Append one completed trade row to results.csv and notify Telegram."""
    _ensure_csv_header()
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)
    row = {
        "timestamp": timestamp.isoformat(),
        "market_id": market_id,
        "direction": direction,
        "entry_price": round(entry_price, 6),
        "exit_price": round(exit_price, 6),
        "hold_seconds": round(hold_seconds, 1),
        "pnl_usdc": round(pnl_usdc, 4),
        "edge_at_entry": round(edge_at_entry, 4),
        "mode": mode,
    }
    with open(RESULTS_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RESULTS_FIELDNAMES)
        writer.writerow(row)
    log.info(
        f"Trade logged | {direction} {market_id} | "
        f"entry={entry_price:.4f} exit={exit_price:.4f} "
        f"hold={hold_seconds:.0f}s pnl={pnl_usdc:+.4f} USDC edge={edge_at_entry:.3f}"
    )

    # Telegram notification (silently skipped if env vars not set)
    from telegram_notify import notify_trade
    notify_trade(
        mode=mode,
        direction=direction,
        entry_price=entry_price,
        exit_price=exit_price,
        hold_seconds=hold_seconds,
        pnl_usdc=pnl_usdc,
        edge_at_entry=edge_at_entry,
        reason=reason,
    )


def log_error(message: str, exc: Optional[Exception] = None) -> None:
    """Write an error to error.log (never raises)."""
    try:
        if exc:
            _error_logger.error(f"{message} | {type(exc).__name__}: {exc}")
        else:
            _error_logger.error(message)
    except Exception:
        pass  # Last-resort safety: error logging must never crash the bot


def print_pnl_summary(mode: str) -> None:
    """
    Read results.csv and print a running P&L summary to stdout.
    Called every config.logging.summary_interval_seconds.
    """
    try:
        _ensure_csv_header()
        trades = []
        with open(RESULTS_CSV, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["mode"] == mode:
                    trades.append(row)

        total = len(trades)
        if total == 0:
            log.info(f"[P&L Summary] No trades yet in {mode} mode.")
            return

        wins = sum(1 for t in trades if float(t["pnl_usdc"]) > 0)
        total_pnl = sum(float(t["pnl_usdc"]) for t in trades)
        win_rate = (wins / total * 100) if total > 0 else 0.0

        log.info(
            f"[P&L Summary | {mode.upper()}] "
            f"Trades: {total} | Win rate: {win_rate:.1f}% | "
            f"Total P&L: {total_pnl:+.4f} USDC"
        )
    except Exception as e:
        log_error("Failed to print P&L summary", e)
