"""
execution/utils.py — Shared utilities for live and test executors.
"""


def taker_fee_rate(token_price: float) -> float:
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
