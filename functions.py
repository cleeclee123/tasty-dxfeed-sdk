import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Set

import pandas as pd
from dateutil import tz

from dxfeed_clee.candle import candle_to_dict
from dxfeed_clee.event import EventType
from session import Session
from streamer import DXLinkStreamer
from utils import generate_timestamps


def convert_to_chicago(ts: float):
    from_zone = tz.gettz("UTC")
    to_zone = tz.gettz("America/Chicago")

    dt = datetime.fromtimestamp(ts / 1000)
    utc = dt.replace(tzinfo=from_zone)
    central = utc.astimezone(to_zone)
    return central.timestamp() * 1000


async def fetch_futures_historical_data(
    session: Session,
    symbols: List[str],
    start_date: datetime,
    end_date: datetime,
    interval: str,
    timeout=1,
    xlsx_path: Optional[str] = None,
    run_converison=False,
):
    async with DXLinkStreamer(session) as streamer:
        await streamer.subscribe_candle(
            symbols=symbols,
            interval=interval,
            start_time=start_date,
            extended_trading_hours=True,
        )

        async def get_next_candle():
            async for candle in streamer.listen(EventType.CANDLE):
                return candle
            return None

        try:
            ts = set(
                generate_timestamps(
                    start_date=start_date, end_date=end_date, interval=interval
                )
            )

            candles_dict: Dict[str, List[Dict[str, float | str]]] = {}
            ts_dict: Dict[str, Set[float]] = {}
            for ticker in symbols:
                ticker = ticker.split(":")[0]
                candles_dict[ticker] = []
                ts_dict[ticker] = ts.copy()

            while True:
                try:
                    candle = await asyncio.wait_for(get_next_candle(), timeout=timeout)
                    if candle is None:
                        print("Stream closed, exiting.")
                        break

                    curr_candle = candle_to_dict(candle=candle)
                    curr_ticker = curr_candle["eventSymbol"].split(":")[0]

                    if not run_converison:
                        curr_candles_list = candles_dict[curr_ticker]
                        curr_candles_list.append(curr_candle)
                    else:
                        curr_ts = ts_dict[curr_ticker]
                        curr_candle["time"] = convert_to_chicago(curr_candle["time"])
                        if curr_candle["time"] not in curr_ts:
                            continue

                        curr_candles_list = candles_dict[curr_ticker]
                        curr_candles_list.append(curr_candle)
                        curr_ts.discard(curr_candle["time"])

                        if len(curr_ts) == 0:
                            print("Timestamp set is empty")
                            continue

                except asyncio.TimeoutError:
                    print(f"No data received for {timeout} seconds, exiting.")
                    break
                except Exception as e:
                    print(f"error: {e}")
                    break
        finally:
            print(f"{len(ts)} timestamps not found") if run_converison else None

            df_dict = {}
            if xlsx_path:
                with pd.ExcelWriter(xlsx_path) as writer:
                    for symbol, candles in candles_dict.items():
                        if len(candles) != 0:
                            df = pd.DataFrame(candles)
                            df["time"] = pd.to_datetime(df["time"], unit="ms")
                            df_dict[symbol] = df
                            df.to_excel(writer, sheet_name=symbol[1:], index=False)
            else:
                for symbol, candles in candles_dict.items():
                    if len(candles) != 0:
                        df = pd.DataFrame(candles)
                        df["time"] = pd.to_datetime(df["time"], unit="ms")
                        df_dict[symbol] = df

            await streamer.unsubscribe_candle(
                symbols=symbols,
                interval=interval,
                extended_trading_hours=True,
            )

    return df_dict


async def quote_streamer():
    pass