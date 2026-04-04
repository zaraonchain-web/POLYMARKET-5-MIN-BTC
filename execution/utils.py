"""
execution/utils.py — Shared utilities for live and test executors.
"""

# Fallback peak fee rate used when the live API rate is unavailable.
# Based on Polymarket's crypto market fee schedule (March 2026).
# Always prefer the live rate from polymarket_feed.fee_rate over this.
_FALLBACK_PEAK_FEE_RATE = 0.018  # 1.8% peak at 50/50 odds


def taker_fee_rate(token_price: float, live_fee_rate: float = None) -> float:
    """
    Compute the Polymarket taker fee rate for a given token price.

    Polymarket's fee formula for binary markets:
      fee_rate = peak * (1 - |p - 0.5| / 0.5)
    where:
      peak = live_fee_rate if available, else _FALLBACK_PEAK_FEE_RATE
      p    = token price clamped to [0.01, 0.99]

    The fee peaks at 50/50 odds and approaches zero near 0 or 1.

    Args:
      token_price:   Current token price (0-1)
      live_fee_rate: Peak fee rate fetched live from CLOB API (fraction, e.g.
                     0.018 = 1.8%). Pass polymarket_feed.fee_rate here.
                     If None, falls back to _FALLBACK_PEAK_FEE_RATE.

    Returns:
      Fee rate as a fraction (e.g. 0.014 = 1.4% on a $20 trade = $0.28 fee)

    Examples (at 1.8% peak):
      p=0.50 -> 1.80%
      p=0.60 -> 1.44%
      p=0.70 -> 1.08%
      p=0.80 -> 0.72%
      p=0.90 -> 0.36%
    """
    peak = live_fee_rate if live_fee_rate is not None else _FALLBACK_PEAK_FEE_RATE
    p = max(0.01, min(0.99, token_price))
    return peak * (1.0 - abs(p - 0.5) / 0.5)
