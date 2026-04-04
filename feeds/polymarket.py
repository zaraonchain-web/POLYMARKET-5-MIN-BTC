"""
feeds/polymarket.py — Polymarket CLOB WebSocket + REST handler

Official WS message types (docs.polymarket.com/market-data/websocket/market-channel):

  book         — Full snapshot on subscribe + after trades.
                 Top-level asset_id. bids/asks are standard: bids < asks.
                 {"event_type":"book","asset_id":"...","bids":[{"price":".48","size":"30"},...],
                  "asks":[{"price":".52","size":"25"},...]}

  price_change — New order placed or cancelled. Nested structure:
                 {"event_type":"price_change","market":"...","price_changes":[
                   {"asset_id":"...","price":"0.5","size":"200","side":"BUY",
                    "best_bid":"0.5","best_ask":"1","hash":"..."},
                   ...
                 ]}
                 size="0" means the level was removed.
                 best_bid/best_ask are the new top-of-book after the change.

  best_bid_ask — (custom_feature_enabled only) Emitted when best bid/ask changes.
                 {"event_type":"best_bid_ask","asset_id":"...","best_bid":"0.73","best_ask":"0.77","spread":"0.04"}

Strategy: parse all three event types. Use best_bid/best_ask from price_change
and best_bid_ask directly — these give us the authoritative top-of-book without
having to maintain a full depth ladder from deltas.
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

WS_STALE_THRESHOLD = 10.0   # seconds before REST fallback kicks in
REST_POLL_INTERVAL = 5.0    # REST polling interval when WS is stale


class PolymarketFeed:

    def __init__(self, clob_rest_url: str = CLOB_REST, clob_ws_url: str = CLOB_WS):
        self.clob_rest_url  = clob_rest_url
        self.clob_ws_url    = clob_ws_url

        self.market_id:     Optional[str]   = None
        self.up_token_id:   Optional[str]   = None
        self.down_token_id: Optional[str]   = None
        self.market_end_ts: Optional[float] = None

        # Live fee rate fetched from CLOB API once per market window.
        # Stored as a fraction (e.g. 0.018 = 1.8%). None = not yet fetched.
        self.fee_rate: Optional[float] = None

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
        # Reconnect signalling: use an asyncio.Event (state-based) so
        # main.py doesn't need a time-based flag to coordinate reconnects.
        self._reconnect_event: Optional[asyncio.Event] = None

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ── Book reset and reconnect control ─────────────────────────────────────────

    def reset_book(self) -> None:
        """
        Clear all order-book and price state for a new market window.
        Called by main.py after refresh_if_expired() returns True.
        """
        self._up_bids.clear()
        self._up_asks.clear()
        self._down_bids.clear()
        self._down_asks.clear()
        self._best_bid      = None
        self._best_ask      = None
        self._down_best_bid = None
        self._down_best_ask = None
        self.spread         = None
        self.up_price       = None
        self.down_price     = None
        self._up_book_last_updated   = 0.0
        self._down_book_last_updated = 0.0
        self.fee_rate = None

    def request_reconnect(self) -> None:
        """
        Signal the WebSocket run() loop to reconnect to the new token IDs.
        Uses an asyncio.Event (state-based, not timer-based).
        """
        if self._reconnect_event is not None:
            self._reconnect_event.set()

    # ── Market discovery ─────────────────────────────────────────────────────

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
                            # Fetch live fee rate for this market window
                            await self._fetch_fee_rate(session)
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

    async def _fetch_fee_rate(self, session: aiohttp.ClientSession) -> None:
        """
        Fetch the live taker fee rate for the UP token from the CLOB API.
        Called once per market window inside fetch_active_market().
        Stores result as a fraction in self.fee_rate (e.g. 0.018 = 1.8% peak).
        Falls back to None on any error — executors will use the formula fallback.

        API: GET https://clob.polymarket.com/fee-rate?token_id={token_id}
        Response: {"fee_rate_bps": "180"} where bps = basis points (100 bps = 1%)
        """
        if self.up_token_id is None:
            return
        try:
            url = "{}/fee-rate".format(self.clob_rest_url)
            async with session.get(
                url,
                params={"token_id": self.up_token_id},
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    bps = (
                        data.get("fee_rate_bps")
                        or data.get("feeRateBps")
                        or data.get("base_fee")
                    )
                    if bps is not None:
                        # Convert basis points to fraction: 180 bps → 0.0180
                        self.fee_rate = float(bps) / 10000.0
                        log.info(
                            "[Polymarket] Live fee rate: {:.4f} ({:.2f}% peak) for token {}...".format(
                                self.fee_rate, self.fee_rate * 100, self.up_token_id[:20]
                            )
                        )
                    else:
                        log.warning("[Polymarket] fee-rate response missing bps field: {}".format(data))
                else:
                    log.warning("[Polymarket] fee-rate API returned status {}".format(resp.status))
        except Exception as e:
            log_error("[Polymarket] _fetch_fee_rate failed — will use formula fallback", e)

    # ── REST midpoint fallback ────────────────────────────────────────────────

    async def _rest_price_poller(self) -> None:
        """
        Polls CLOB REST /midpoint when WS book is stale.
        Workaround for Polymarket's known WS silent-freeze bug.
        (github.com/Polymarket/py-clob-client/issues/292)
        """
        while True:
            await asyncio.sleep(REST_POLL_INTERVAL)
            try:
                if self.up_token_id is None:
                    continue
                ws_age = time.time() - self._up_book_last_updated
                if ws_age < WS_STALE_THRESHOLD:
                    continue

                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        "{}/midpoint".format(self.clob_rest_url),
                        params={"token_id": self.up_token_id},
                        timeout=aiohttp.ClientTimeout(total=5),
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            mid = float(data.get("mid", 0))
                            if 0 < mid < 1:
                                self.up_price   = mid
                                self.down_price = round(1.0 - mid, 6)
                                self.spread     = None  # REST has no spread
                                log.debug("[Polymarket] REST fallback: UP={:.4f} (WS stale {:.0f}s)".format(mid, ws_age))
            except Exception as e:
                log_error("[Polymarket] REST price poller error", e)

    # ── WebSocket ─────────────────────────────────────────────────────────────

    WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    async def run(self) -> None:
        # Initialise the reconnect event here so it's bound to the running loop.
        self._reconnect_event = asyncio.Event()
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
                    # Clear event from any previous cycle before entering receive loop.
                    self._reconnect_event.clear()

                    self._up_bids.clear();   self._up_asks.clear()
                    self._down_bids.clear(); self._down_asks.clear()
                    self._up_book_last_updated = 0.0

                    # Official subscription per docs:
                    # {"assets_ids": [...], "type": "market", "custom_feature_enabled": true}
                    # custom_feature_enabled gives us best_bid_ask events — the cleanest price source.
                    sub_msg = {
                        "assets_ids": [self.up_token_id, self.down_token_id],
                        "type": "market",
                        "custom_feature_enabled": True,
                    }
                    await ws.send(json.dumps(sub_msg))
                    log.info("[Polymarket] Subscribed to order book for {}".format(self.market_id))

                    async for raw in ws:
                        # State-based reconnect check (replaces old time-based _force_reconnect flag)
                        if self._reconnect_event.is_set():
                            self._reconnect_event.clear()
                            log.info("[Polymarket] Reconnect requested — restarting WS for new market")
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

    # ── Message parsing ───────────────────────────────────────────────────────

    async def _handle_message(self, raw: str) -> None:
        try:
            msg    = json.loads(raw)
            events = msg if isinstance(msg, list) else [msg]
            for event in events:
                if not isinstance(event, dict):
                    continue
                event_type = event.get("event_type", "")

                if event_type == "book":
                    # Full depth snapshot — standard bids/asks at top level
                    self._handle_book_event(event)

                elif event_type == "price_change":
                    # Nested structure: price_changes is a list, each entry has asset_id
                    self._handle_price_change_event(event)

                elif event_type == "best_bid_ask":
                    # Authoritative best bid/ask — use directly, no book math needed
                    self._handle_best_bid_ask_event(event)

                # other event types (tick_size_change, last_trade_price, etc.) — ignore

        except (json.JSONDecodeError, TypeError) as e:
            log_error("[Polymarket] Failed to parse WS message: {}".format(raw[:200]), e)

    def _handle_book_event(self, event: dict) -> None:
        """Full depth snapshot — emitted on subscribe and after trades."""
        asset_id = event.get("asset_id") or event.get("token_id")
        if asset_id == self.up_token_id:
            self._up_bids.clear()
            self._up_asks.clear()
            self._apply_book_levels(event.get("bids", []), self._up_bids)
            self._apply_book_levels(event.get("asks", []), self._up_asks)
            self._recompute_up_price()
            self._up_book_last_updated = time.time()
        elif asset_id == self.down_token_id:
            self._down_bids.clear()
            self._down_asks.clear()
            self._apply_book_levels(event.get("bids", []), self._down_bids)
            self._apply_book_levels(event.get("asks", []), self._down_asks)
            self._recompute_down_price()
            self._down_book_last_updated = time.time()

    def _handle_price_change_event(self, event: dict) -> None:
        """
        price_change has a nested structure with a price_changes list.
        Each entry contains asset_id, price, size, side, best_bid, best_ask.
        We use best_bid/best_ask directly — these are the authoritative new
        top-of-book values, no need to track all depth levels.
        """
        for change in event.get("price_changes", []):
            if not isinstance(change, dict):
                continue
            asset_id = change.get("asset_id")
            best_bid_str = change.get("best_bid")
            best_ask_str = change.get("best_ask")

            if best_bid_str is None or best_ask_str is None:
                continue

            try:
                best_bid = float(best_bid_str)
                best_ask = float(best_ask_str)
            except (ValueError, TypeError):
                continue

            if asset_id == self.up_token_id:
                self._update_up_from_best(best_bid, best_ask)
                self._up_book_last_updated = time.time()
            elif asset_id == self.down_token_id:
                self._update_down_from_best(best_bid, best_ask)
                self._down_book_last_updated = time.time()

    def _handle_best_bid_ask_event(self, event: dict) -> None:
        """
        best_bid_ask — requires custom_feature_enabled:true.
        Most direct source: best_bid, best_ask, spread are all given.
        """
        asset_id = event.get("asset_id")
        try:
            best_bid = float(event.get("best_bid", 0))
            best_ask = float(event.get("best_ask", 0))
        except (ValueError, TypeError):
            return

        if asset_id == self.up_token_id:
            self._update_up_from_best(best_bid, best_ask)
            self._up_book_last_updated = time.time()
        elif asset_id == self.down_token_id:
            self._update_down_from_best(best_bid, best_ask)
            self._down_book_last_updated = time.time()

    def _update_up_from_best(self, best_bid: float, best_ask: float) -> None:
        """Update UP token price using authoritative best bid/ask values."""
        if best_bid > 0 and best_ask > 0 and best_ask > best_bid:
            self._best_bid = best_bid
            self._best_ask = best_ask
            self.up_price  = (best_bid + best_ask) / 2
            self.spread    = round(best_ask - best_bid, 6)
        elif best_bid > 0:
            self._best_bid = best_bid
            self._best_ask = None
            self.up_price  = best_bid
            self.spread    = None
        elif best_ask > 0:
            self._best_bid = None
            self._best_ask = best_ask
            self.up_price  = best_ask
            self.spread    = None

    def _update_down_from_best(self, best_bid: float, best_ask: float) -> None:
        """Update DOWN token price using authoritative best bid/ask values.
        Also tracks _down_best_bid and _down_best_ask so the executor can
        fill at the real ask (entry) and bid (exit) rather than the mid.
        """
        if best_bid > 0 and best_ask > 0 and best_ask > best_bid:
            self._down_best_bid = best_bid
            self._down_best_ask = best_ask
            self.down_price = (best_bid + best_ask) / 2
        elif best_bid > 0:
            self._down_best_bid = best_bid
            self._down_best_ask = None
            self.down_price = best_bid
        elif best_ask > 0:
            self._down_best_bid = None
            self._down_best_ask = best_ask
            self.down_price = best_ask

    @staticmethod
    def _apply_book_levels(levels: list, book: Dict[float, float]) -> None:
        """Apply a list of {price, size} levels to a book dict."""
        for o in levels:
            try:
                p = float(o.get("price") or o.get("p") or 0)
                s = float(o.get("size")  or o.get("s") or 0)
                if p <= 0:
                    continue
                if s == 0:
                    book.pop(p, None)
                else:
                    book[p] = s
            except (TypeError, ValueError):
                continue

    def _recompute_up_price(self) -> None:
        best_bid = max(self._up_bids.keys(), default=None)
        best_ask = min(self._up_asks.keys(), default=None)

        if best_bid is not None and best_ask is not None and best_ask > best_bid:
            self._best_bid = best_bid
            self._best_ask = best_ask
            self.up_price  = (best_bid + best_ask) / 2
            self.spread    = round(best_ask - best_bid, 6)
        elif best_bid is not None:
            self._best_bid = best_bid
            self._best_ask = None
            self.up_price  = best_bid
            self.spread    = None
        elif best_ask is not None:
            self._best_bid = None
            self._best_ask = best_ask
            self.up_price  = best_ask
            self.spread    = None
        else:
            self._best_bid = None
            self._best_ask = None
            self.spread    = None

    def _recompute_down_price(self) -> None:
        best_bid = max(self._down_bids.keys(), default=None)
        best_ask = min(self._down_asks.keys(), default=None)

        if best_bid is not None and best_ask is not None and best_ask > best_bid:
            self._down_best_bid = best_bid
            self._down_best_ask = best_ask
            self.down_price = (best_bid + best_ask) / 2
        elif best_bid is not None:
            self._down_best_bid = best_bid
            self._down_best_ask = None
            self.down_price = best_bid
        elif best_ask is not None:
            self._down_best_bid = None
            self._down_best_ask = best_ask
            self.down_price = best_ask

    def seconds_until_settlement(self) -> Optional[float]:
        if self.market_end_ts is None:
            return None
        return self.market_end_ts - time.time()

    def refresh_if_expired(self) -> bool:
        if self.market_end_ts is None:
            return True
        return time.time() >= self.market_end_ts
