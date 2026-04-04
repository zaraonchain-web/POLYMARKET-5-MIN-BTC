"""
feeds/polymarket.py — Polymarket CLOB WebSocket + REST handler

Persistent order book implementation:
- Full depth snapshot on WS connect, then delta updates
- Each bid/ask level stored as price->size dict
- Levels with size=0 are removed (market maker cancel)
- best_bid = max(bids), best_ask = min(asks), spread = best_ask - best_bid
- Gamma API outcomePrices used to pre-seed prices before WS connects
- REST /midpoint fallback polls every 5s when WS book is stale (known Polymarket freeze bug)

Official WS message format (from docs.polymarket.com/developers/CLOB/websocket/market-channel):
  Subscription : {"assets_ids": ["<token_id>", ...], "type": "market"}
  Book event   : {"event_type": "book", "asset_id": "...", "bids": [{"price": ".48", "size": "30"},...], "asks": [...]}
  Bids = lower prices (buyers), Asks = higher prices (sellers) — standard convention, NO inversion needed.

Public interface:
    PolymarketFeed.market_id         -- active 5-min market condition ID
    PolymarketFeed.up_price          -- current UP token mid (0-1)
    PolymarketFeed.down_price        -- current DOWN token mid (0-1)
    PolymarketFeed.spread            -- best ask - best bid for UP token
    PolymarketFeed.market_end_ts     -- unix timestamp of market close
    PolymarketFeed.up_token_id       -- token ID for UP (needed for orders)
    PolymarketFeed.down_token_id     -- token ID for DOWN
    PolymarketFeed.run()             -- coroutine; keeps WS alive forever
    PolymarketFeed.fetch_active_market() -- one-shot REST call at startup
"""

import asyncio
import json
import time
from typing import Optional, Dict

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed

from logger import log, log_error

CLOB_REST  = "https://clob.polymarket.com"
CLOB_WS    = "wss://ws-subscriptions-clob.polymarket.com/ws/"
GAMMA_API  = "https://gamma-api.polymarket.com"

# How many seconds of no WS book updates before we fall back to REST polling
WS_STALE_THRESHOLD = 10.0
# How often to poll REST midpoint when WS is stale (seconds)
REST_POLL_INTERVAL = 5.0


class PolymarketFeed:

    def __init__(self, clob_rest_url: str = CLOB_REST, clob_ws_url: str = CLOB_WS):
        self.clob_rest_url  = clob_rest_url
        self.clob_ws_url    = clob_ws_url

        self.market_id:     Optional[str]   = None
        self.up_token_id:   Optional[str]   = None
        self.down_token_id: Optional[str]   = None
        self.market_end_ts: Optional[float] = None

        # Persistent order books: price_float -> size_float
        self._up_bids:   Dict[float, float] = {}
        self._up_asks:   Dict[float, float] = {}
        self._down_bids: Dict[float, float] = {}
        self._down_asks: Dict[float, float] = {}

        self.up_price:   Optional[float] = None
        self.down_price: Optional[float] = None
        self.spread:     Optional[float] = None
        self._best_bid:  Optional[float] = None
        self._best_ask:  Optional[float] = None
        self._down_best_bid: Optional[float] = None
        self._down_best_ask: Optional[float] = None
        self._up_book_last_updated:   float = 0.0
        self._down_book_last_updated: float = 0.0
        self._connected = False
        self._force_reconnect = False

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def fetch_active_market(self) -> bool:
        import math
        try:
            async with aiohttp.ClientSession() as session:
                now = time.time()
                for offset in range(4):
                    window_ts = int(math.floor(now / 300) * 300) + offset * 300
                    slug = "btc-updown-5m-{}".format(window_ts)
                    url  = "{}/events".format(GAMMA_API)
                    async with session.get(url, params={"slug": slug},
                                           timeout=aiohttp.ClientTimeout(total=8)) as resp:
                        if resp.status != 200:
                            continue
                        data   = await resp.json()
                        events = data if isinstance(data, list) else []
                        if not events:
                            continue
                        ok = self._populate_from_gamma_event(events[0], window_ts)
                        if ok:
                            log.info(
                                "[Polymarket] Active market: {} | UP_token={}... DOWN_token={}... | Ends: {} | slug={}".format(
                                    self.market_id,
                                    str(self.up_token_id)[:20],
                                    str(self.down_token_id)[:20],
                                    self.market_end_ts,
                                    slug
                                )
                            )
                            return True
            log_error("[Polymarket] No active 5-minute BTC market found")
            return False
        except Exception as e:
            log_error("[Polymarket] fetch_active_market failed", e)
            return False

    def _populate_from_gamma_event(self, event: dict, window_ts: int) -> bool:
        try:
            markets = event.get("markets", [])
            if not markets:
                return False
            market = markets[0]

            self.market_id = market.get("conditionId") or market.get("condition_id")
            if not self.market_id:
                return False

            clob_token_ids = market.get("clobTokenIds", [])
            if isinstance(clob_token_ids, str):
                clob_token_ids = json.loads(clob_token_ids)
            if len(clob_token_ids) >= 2:
                self.up_token_id   = str(clob_token_ids[0])
                self.down_token_id = str(clob_token_ids[1])
            else:
                return False

            outcome_prices = market.get("outcomePrices")
            if outcome_prices:
                if isinstance(outcome_prices, str):
                    outcome_prices = json.loads(outcome_prices)
                if len(outcome_prices) >= 2:
                    try:
                        up_p   = float(outcome_prices[0])
                        down_p = float(outcome_prices[1])
                        if 0 < up_p < 1 and 0 < down_p < 1:
                            self.up_price   = up_p
                            self.down_price = down_p
                            log.info("[Polymarket] Gamma seed prices: UP={:.3f} DOWN={:.3f}".format(up_p, down_p))
                    except (ValueError, TypeError):
                        pass

            self.market_end_ts = float(window_ts + 300)
            return True
        except Exception as e:
            log_error("[Polymarket] _populate_from_gamma_event error", e)
            return False

    # ── REST midpoint fallback (handles Polymarket's known WS silent-freeze bug) ──

    async def _rest_price_poller(self) -> None:
        """
        Polls CLOB REST /midpoint every REST_POLL_INTERVAL seconds when the WS
        book has gone stale. This is the documented workaround for the Polymarket
        CLOB WSS silent-freeze bug (github.com/Polymarket/py-clob-client/issues/292).
        """
        while True:
            await asyncio.sleep(REST_POLL_INTERVAL)
            try:
                if self.up_token_id is None or self.down_token_id is None:
                    continue

                ws_age = time.time() - self._up_book_last_updated
                if ws_age < WS_STALE_THRESHOLD:
                    continue  # WS is fresh, no need to poll

                async with aiohttp.ClientSession() as session:
                    # Fetch UP midpoint
                    async with session.get(
                        "{}/midpoint".format(self.clob_rest_url),
                        params={"token_id": self.up_token_id},
                        timeout=aiohttp.ClientTimeout(total=5),
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            mid = float(data.get("mid", 0))
                            if 0 < mid < 1:
                                self.up_price = mid
                                self.down_price = round(1.0 - mid, 6)
                                # REST doesn't give us spread; set None so strategy uses seed
                                self.spread = None
                                log.debug("[Polymarket] REST fallback: UP={:.4f} (WS stale {:.0f}s)".format(mid, ws_age))
            except Exception as e:
                log_error("[Polymarket] REST price poller error", e)

    # ── WebSocket channel ─────────────────────────────────────────────────────

    # Official WS endpoint per docs.polymarket.com/developers/CLOB/websocket/wss-overview
    WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    async def run(self) -> None:
        # Start the REST fallback poller as a background task
        asyncio.get_event_loop().create_task(self._rest_price_poller())

        backoff = 1
        while True:
            if self.market_id is None:
                ok = await self.fetch_active_market()
                if not ok:
                    log_error("[Polymarket] Cannot start WS: no active market. Retrying in 15s")
                    await asyncio.sleep(15)
                    continue

            try:
                log.info("[Polymarket] Connecting WS for market {}".format(self.market_id))
                async with websockets.connect(
                    self.WS_MARKET_URL,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    self._connected = True
                    backoff = 1

                    # Clear stale book on reconnect
                    self._up_bids.clear();   self._up_asks.clear()
                    self._down_bids.clear(); self._down_asks.clear()
                    self._up_book_last_updated = 0.0

                    # Official subscription format per Polymarket docs:
                    # {"assets_ids": ["<token_id>", ...], "type": "market"}
                    # type must be lowercase "market" (not "Market" or "MARKET")
                    sub_msg = {
                        "assets_ids": [self.up_token_id, self.down_token_id],
                        "type": "market",
                    }
                    await ws.send(json.dumps(sub_msg))
                    log.info("[Polymarket] Subscribed to order book for {}".format(self.market_id))

                    async for raw in ws:
                        if self._force_reconnect:
                            self._force_reconnect = False
                            break
                        await self._handle_message(raw)

            except ConnectionClosed as e:
                self._connected = False
                log_error("[Polymarket] WS closed: {}. Reconnecting in {}s...".format(e, backoff))
            except Exception as e:
                self._connected = False
                log_error("[Polymarket] WS error: {}. Reconnecting in {}s...".format(e, backoff), e)
            finally:
                self._connected = False

            self.market_id     = None
            self.up_token_id   = None
            self.down_token_id = None
            self.market_end_ts = None

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    async def _handle_message(self, raw: str) -> None:
        try:
            msg    = json.loads(raw)
            events = msg if isinstance(msg, list) else [msg]
            for event in events:
                if not isinstance(event, dict):
                    continue
                # Official field name per market channel docs is "asset_id"
                asset_id = event.get("asset_id") or event.get("token_id")
                if asset_id == self.up_token_id:
                    self._apply_book_update(event, self._up_bids, self._up_asks)
                    self._recompute_up_price()
                    self._up_book_last_updated = time.time()
                elif asset_id == self.down_token_id:
                    self._apply_book_update(event, self._down_bids, self._down_asks)
                    self._recompute_down_price()
                    self._down_book_last_updated = time.time()
                # else: could be empty [], heartbeat, or other event — silently ignore
        except (json.JSONDecodeError, TypeError) as e:
            log_error("[Polymarket] Failed to parse WS message: {}".format(raw[:200]), e)

    @staticmethod
    def _apply_book_update(
        event: dict,
        bids: Dict[float, float],
        asks: Dict[float, float],
    ) -> None:
        """
        Apply a book snapshot or delta update to the in-memory order book.

        Per official Polymarket docs, the book event format is standard:
          bids = resting buy orders (lower prices, e.g. [".48", ".49"])
          asks = resting sell orders (higher prices, e.g. [".52", ".53"])
        No inversion needed.
        """
        for o in event.get("bids", []):
            try:
                p = float(o.get("price") or o.get("p") or 0)
                s = float(o.get("size")  or o.get("s") or 0)
                if p <= 0:
                    continue
                if s == 0:
                    bids.pop(p, None)
                else:
                    bids[p] = s
            except (TypeError, ValueError):
                continue

        for o in event.get("asks", []):
            try:
                p = float(o.get("price") or o.get("p") or 0)
                s = float(o.get("size")  or o.get("s") or 0)
                if p <= 0:
                    continue
                if s == 0:
                    asks.pop(p, None)
                else:
                    asks[p] = s
            except (TypeError, ValueError):
                continue

    def _recompute_up_price(self) -> None:
        best_bid = max(self._up_bids.keys(), default=None)
        best_ask = min(self._up_asks.keys(), default=None)

        if best_bid is not None and best_ask is not None:
            self.up_price  = (best_bid + best_ask) / 2
            self._best_bid = best_bid
            self._best_ask = best_ask
            self.spread    = round(best_ask - best_bid, 6)
        elif best_bid is not None:
            self.up_price  = best_bid
            self._best_bid = best_bid
            self._best_ask = None
            self.spread    = None
        elif best_ask is not None:
            self.up_price  = best_ask
            self._best_bid = None
            self._best_ask = best_ask
            self.spread    = None
        else:
            self._best_bid = None
            self._best_ask = None
            self.spread    = None

    def _recompute_down_price(self) -> None:
        best_bid = max(self._down_bids.keys(), default=None)
        best_ask = min(self._down_asks.keys(), default=None)

        self._down_best_bid = best_bid
        self._down_best_ask = best_ask

        if best_bid is not None and best_ask is not None:
            self.down_price = (best_bid + best_ask) / 2
        elif best_bid is not None:
            self.down_price = best_bid
        elif best_ask is not None:
            self.down_price = best_ask

    def seconds_until_settlement(self) -> Optional[float]:
        if self.market_end_ts is None:
            return None
        return self.market_end_ts - time.time()

    def refresh_if_expired(self) -> bool:
        if self.market_end_ts is None:
            return True
        return time.time() >= self.market_end_ts
