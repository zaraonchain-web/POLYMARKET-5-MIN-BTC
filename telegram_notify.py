"""
telegram_notify.py — Send trade notifications to a Telegram bot.

Setup (one-time, takes 2 minutes):
  1. Open Telegram and message @BotFather
  2. Send /newbot, follow prompts, copy the API token
  3. Message your new bot once (so it can send to you)
  4. Visit: https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates
     Copy the "id" value inside "chat" — that's your CHAT_ID
  5. Add to Railway environment variables:
       TELEGRAM_BOT_TOKEN = 123456:ABC-your-token-here
       TELEGRAM_CHAT_ID   = 123456789

If these env vars are not set, notifications are silently skipped —
the bot keeps running normally without Telegram.
"""

import asyncio
import os
from typing import Optional

import aiohttp


TELEGRAM_BOT_TOKEN: Optional[str] = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID:   Optional[str] = os.environ.get("TELEGRAM_CHAT_ID")

_ENABLED = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


async def _send(text: str) -> None:
    """Fire-and-forget HTTP POST to the Telegram Bot API."""
    if not _ENABLED:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(
                url,
                json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": text,
                    "parse_mode": "HTML",
                },
                timeout=aiohttp.ClientTimeout(total=5),
            )
    except Exception:
        pass  # Never crash the bot over a notification failure


def notify(text: str) -> None:
    """
    Schedule a Telegram message from sync or async context.
    Safe to call from anywhere — creates its own task if a loop is running.
    """
    if not _ENABLED:
        return
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(_send(text))
        else:
            loop.run_until_complete(_send(text))
    except Exception:
        pass


def notify_trade(
    *,
    mode: str,
    direction: str,
    entry_price: float,
    exit_price: float,
    hold_seconds: float,
    pnl_usdc: float,
    edge_at_entry: float,
    reason: str,
) -> None:
    """
    Send a formatted trade-close notification.

    Example message:
      📊 [TEST] TRADE CLOSED
      Direction : UP
      Entry     : 0.5200
      Exit      : 0.5650
      Hold      : 47s
      Edge      : +0.120
      Reason    : take-profit
      P&L       : +$0.87 ✅
    """
    emoji = "✅" if pnl_usdc >= 0 else "❌"
    mode_tag = mode.upper()
    pnl_sign = "+" if pnl_usdc >= 0 else ""

    text = (
        f"📊 <b>[{mode_tag}] TRADE CLOSED</b>\n"
        f"Direction : {direction}\n"
        f"Entry     : {entry_price:.4f}\n"
        f"Exit      : {exit_price:.4f}\n"
        f"Hold      : {hold_seconds:.0f}s\n"
        f"Edge      : {edge_at_entry:+.3f}\n"
        f"Reason    : {reason}\n"
        f"P&amp;L       : {pnl_sign}${pnl_usdc:.4f} {emoji}"
    )
    notify(text)


def notify_signal(
    *,
    mode: str,
    direction: str,
    btc_pct_change: float,
    pm_price: float,
    fair_prob: float,
    edge: float,
) -> None:
    """
    Send a notification when a signal is detected and a trade is entered.

    Example message:
      🚀 [TEST] SIGNAL ENTERED
      Direction : UP
      BTC move  : +0.412%
      PM price  : 0.510
      Fair prob : 0.608
      Edge      : +0.098
    """
    mode_tag = mode.upper()
    text = (
        f"🚀 <b>[{mode_tag}] SIGNAL ENTERED</b>\n"
        f"Direction : {direction}\n"
        f"BTC move  : {btc_pct_change:+.3f}%\n"
        f"PM price  : {pm_price:.3f}\n"
        f"Fair prob : {fair_prob:.3f}\n"
        f"Edge      : {edge:+.3f}"
    )
    notify(text)


def notify_halt(*, daily_pnl: float, limit: float) -> None:
    """Notify when the daily loss limit is hit and trading halts."""
    text = (
        f"🛑 <b>DAILY LOSS LIMIT HIT</b>\n"
        f"Cumulative P&amp;L : ${daily_pnl:+.2f}\n"
        f"Limit         : -${limit:.2f}\n"
        f"Trading halted for today."
    )
    notify(text)


def notify_startup(mode: str) -> None:
    """Notify when the bot starts up."""
    text = f"🟢 <b>Bot started in {mode.upper()} mode</b>"
    notify(text)


def notify_error(message: str) -> None:
    """Notify on critical errors."""
    text = f"⚠️ <b>Bot error</b>\n{message}"
    notify(text)
