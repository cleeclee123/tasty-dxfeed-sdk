import asyncio
import json
import logging
import re
from asyncio import Lock, Queue
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import AsyncIterator, Dict, List, Optional

import websockets
from pydantic import BaseModel

from dxfeed_clee.candle import Candle
from dxfeed_clee.event import Event, EventType
from dxfeed_clee.quote import Quote
from dxfeed_clee.summary import Summary
from dxfeed_clee.timeandsales import TimeAndSale
from dxfeed_clee.trade import Trade
from session import ProductionSession, TastytradeError


class Channel(str, Enum):
    DATA = "/service/data"
    HANDSHAKE = "/meta/handshake"
    HEARTBEAT = "/meta/connect"
    SUBSCRIPTION = "/service/sub"
    TIME_SERIES = "/service/timeSeriesData"


def _dasherize(s: str) -> str:
    return s.replace("_", "-")


class TastytradeJsonDataclass(BaseModel):
    class Config:
        alias_generator = _dasherize
        populate_by_name = True


CERT_STREAMER_URL = "wss://streamer.cert.tastyworks.com"
STREAMER_URL = "wss://streamer.tastyworks.com"


class QuoteAlert(TastytradeJsonDataclass):
    user_external_id: str
    symbol: str
    alert_external_id: str
    expires_at: int
    completed_at: datetime
    created_at: datetime
    triggered_at: datetime
    field: str
    operator: str
    threshold: str
    threshold_numeric: Decimal
    dx_symbol: str


class SubscriptionType(str, Enum):
    ACCOUNT = "account-subscribe"  # may be 'connect' in the future
    HEARTBEAT = "heartbeat"
    PUBLIC_WATCHLISTS = "public-watchlists-subscribe"
    QUOTE_ALERTS = "quote-alerts-subscribe"
    USER_MESSAGE = "user-message-subscribe"


class DXLinkStreamer:
    def __init__(self, session: ProductionSession):
        self._counter = 0
        self._lock: Lock = Lock()
        self._queues: Dict[EventType, Queue] = defaultdict(Queue)
        self._channels: Dict[EventType, int] = {
            EventType.CANDLE: 1,
            EventType.QUOTE: 7,
            EventType.SUMMARY: 9,
            EventType.TIME_AND_SALE: 13,
            EventType.TRADE: 15,
        }
        self._subscription_state: Dict[EventType, str] = defaultdict(
            lambda: "CHANNEL_CLOSED"
        )

        self._session = session
        self._authenticated = False
        self._wss_url = session.dxlink_url
        self._auth_token = session.streamer_token

        self._connect_task = asyncio.create_task(self._connect())

    async def __aenter__(self):
        time_out = 100
        while not self._authenticated:
            await asyncio.sleep(0.1)
            time_out -= 1
            if time_out < 0:
                raise TastytradeError("Connection timed out")

        return self

    @classmethod
    async def create(cls, session: ProductionSession) -> "DXLinkStreamer":
        self = cls(session)
        return await self.__aenter__()

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        self._connect_task.cancel()
        self._heartbeat_task.cancel()

    async def _connect(self) -> None:
        async with websockets.connect(self._wss_url) as websocket:  # type: ignore
            self._websocket = websocket
            await self._setup_connection()

            while True:
                raw_message = await self._websocket.recv()
                message = json.loads(raw_message)

                logging.debug("received: %s", message)
                if message["type"] == "SETUP":
                    await self._authenticate_connection()
                elif message["type"] == "AUTH_STATE":
                    if message["state"] == "AUTHORIZED":
                        self._authenticated = True
                        self._heartbeat_task = asyncio.create_task(self._heartbeat())
                elif message["type"] == "CHANNEL_OPENED":
                    channel = next(
                        k for k, v in self._channels.items() if v == message["channel"]
                    )
                    self._subscription_state[channel] = message["type"]
                elif message["type"] == "CHANNEL_CLOSED":
                    pass
                elif message["type"] == "FEED_CONFIG":
                    pass
                elif message["type"] == "FEED_DATA":
                    await self._map_message(message["data"])
                elif message["type"] == "KEEPALIVE":
                    pass
                else:
                    raise TastytradeError("Unknown message type:", message)

    async def _setup_connection(self):
        message = {
            "type": "SETUP",
            "channel": 0,
            "keepaliveTimeout": 60,
            "acceptKeepaliveTimeout": 60,
            "version": "0.1-js/1.0.0",
        }
        await self._websocket.send(json.dumps(message))

    async def _authenticate_connection(self):
        message = {
            "type": "AUTH",
            "channel": 0,
            "token": self._auth_token,
        }
        await self._websocket.send(json.dumps(message))

    async def listen(self, event_type: EventType) -> AsyncIterator[Event]:
        while True:
            yield await self._queues[event_type].get()

    async def get_event(self, event_type: EventType) -> Event:
        return await self._queues[event_type].get()

    async def _heartbeat(self) -> None:
        message = {"type": "KEEPALIVE", "channel": 0}

        while True:
            logging.debug("sending keepalive message: %s", message)
            await self._websocket.send(json.dumps(message))
            await asyncio.sleep(30)

    async def subscribe(self, event_type: EventType, symbols: List[str]) -> None:
        if self._subscription_state[event_type] != "CHANNEL_OPENED":
            await self._channel_request(event_type)
        message = {
            "type": "FEED_SUBSCRIPTION",
            "channel": self._channels[event_type],
            "add": [{"symbol": symbol, "type": event_type} for symbol in symbols],
        }
        logging.debug("sending subscription: %s", message)
        await self._websocket.send(json.dumps(message))

    async def cancel_channel(self, event_type: EventType) -> None:
        message = {
            "type": "CHANNEL_CANCEL",
            "channel": self._channels[event_type],
        }
        logging.debug("sending channel cancel: %s", message)
        await self._websocket.send(json.dumps(message))

    async def _channel_request(self, event_type: EventType) -> None:
        message = {
            "type": "CHANNEL_REQUEST",
            "channel": self._channels[event_type],
            "service": "FEED",
            "parameters": {
                "contract": "AUTO",
            },
        }
        logging.debug("sending subscription: %s", message)
        await self._websocket.send(json.dumps(message))
        time_out = 100
        while not self._subscription_state[event_type] == "CHANNEL_OPENED":
            await asyncio.sleep(0.1)
            time_out -= 1
            if time_out <= 0:
                raise TastytradeError("Subscription channel not opened")

    async def unsubscribe(self, event_type: EventType, symbols: List[str]) -> None:
        if not self._authenticated:
            raise TastytradeError("Stream not authenticated")
        event_type_str = str(event_type).split(".")[1].capitalize()
        message = {
            "type": "FEED_SUBSCRIPTION",
            "channel": self._channels[event_type],
            "remove": [
                {"symbol": symbol, "type": event_type_str} for symbol in symbols
            ],
        }
        logging.debug("sending subscription: %s", message)
        await self._websocket.send(json.dumps(message))

    async def subscribe_candle(
        self,
        symbols: List[str],
        interval: str,
        start_time: datetime,
        extended_trading_hours: bool = False,
    ) -> None:
        await self._channel_request(EventType.CANDLE)

        message = {
            "type": "FEED_SUBSCRIPTION",
            "channel": self._channels[EventType.CANDLE],
            "add": [
                {
                    "symbol": (
                        f"{ticker}{{={interval},tho=true}}"
                        if extended_trading_hours
                        else f"{ticker}{{={interval}}}"
                    ),
                    "type": "Candle",
                    "fromTime": int(start_time.timestamp() * 1000),
                }
                for ticker in symbols
            ],
        }

        await self._websocket.send(json.dumps(message))

    async def unsubscribe_candle(
        self,
        symbols: List[str],
        interval: Optional[str] = None,
        extended_trading_hours: bool = False,
    ) -> None:
        await self._channel_request(EventType.CANDLE)
        message = {
            "type": "FEED_SUBSCRIPTION",
            "channel": self._channels[EventType.CANDLE],
            "remove": [
                {
                    "symbol": (
                        f"{ticker}{{={interval},tho=true}}"
                        if extended_trading_hours
                        else f"{ticker}{{={interval}}}"
                    ),
                    "type": "Candle",
                }
                for ticker in symbols
            ],
        }
        await self._websocket.send(json.dumps(message))

    async def subscribe_quote(
        self,
        symbols: List[str],
    ) -> None:
        await self._channel_request(EventType.QUOTE)
        message = {
            "type": "FEED_SUBSCRIPTION",
            "channel": 1,
            "add": [
                {
                    "symbol": ticker,
                    "type": "Quote",
                }
                for ticker in symbols
            ],
        }
        await self._websocket.send(json.dumps(message))

    async def unsubscribe_quote(
        self,
        symbols: List[str],
    ) -> None:
        await self._channel_request(EventType.QUOTE)
        message = {
            "type": "FEED_SUBSCRIPTION",
            "channel": 1,
            "remove": [
                {
                    "symbol": ticker,
                    "type": "Quote",
                }
                for ticker in symbols
            ],
        }
        await self._websocket.send(json.dumps(message))

    async def _map_message(self, message) -> None:
        for item in message:
            msg_type = item.pop("eventType")
            if msg_type == EventType.CANDLE:
                await self._queues[EventType.CANDLE].put(Candle(**item))
            elif msg_type == EventType.QUOTE:
                await self._queues[EventType.QUOTE].put(Quote(**item))
            elif msg_type == EventType.SUMMARY:
                await self._queues[EventType.SUMMARY].put(Summary(**item))
            elif msg_type == EventType.TIME_AND_SALE:
                tas = TimeAndSale(**item)
                await self._queues[EventType.TIME_AND_SALE].put(tas)
            elif msg_type == EventType.TRADE:
                await self._queues[EventType.TRADE].put(Trade(**item))
            else:
                raise TastytradeError(f"Unknown message type: {message}")
