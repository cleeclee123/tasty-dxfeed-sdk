import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Set

import pandas as pd
from dateutil import tz

from dxfeed_clee.candle import Candle, candle_to_dict
from dxfeed_clee.event import EventType
from dxfeed_clee.futures import (
    gen_futures_streamer_symbols,
    get_all_streamer_symbols,
    sort_cme_contracts,
    cme_contract_code_to_datetime
)
from dxfeed_clee.quote import Quote, quote_to_dict
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


async def main(session):
    subs_list = ["SPY", "SPX"]
    async with DXLinkStreamer(session) as streamer:
        await streamer.subscribe(EventType.QUOTE, subs_list)
        quote = await streamer.get_event(EventType.QUOTE)
        print(quote)


async def get_quotes(
    session: Session,
    symbols: Optional[List[str]] = None,
    contract_code: str = None,
    timeout=1,
    just_midpoint=False,
    just_ask=False,
    just_bid=False,
    return_df=False,
) -> Dict[str, Quote]:
    async with DXLinkStreamer(session) as streamer:
        if symbols and contract_code:
            streamer_codes: List[str] = get_all_streamer_symbols(
                session=session, future_contract_code=contract_code, flip_keys=True
            ).keys()
            symbols.extend(x for x in streamer_codes if x not in symbols)

        elif symbols:
            await streamer.subscribe(
                event_type=EventType.QUOTE,
                symbols=symbols,
            )
        elif contract_code:
            streamer_codes: Dict[str, str] = get_all_streamer_symbols(
                session=session, future_contract_code=contract_code, flip_keys=True
            )
            # print(streamer_codes)
            if not streamer_codes:
                return

            symbols = streamer_codes.keys()
            await streamer.subscribe(
                event_type=EventType.QUOTE,
                symbols=symbols,
            )
        else:
            return

        async def get_next_quote():
            async for quote in streamer.listen(EventType.QUOTE):
                return quote
            return None

        await streamer.subscribe(EventType.QUOTE, symbols)

        quote_dict: Dict[str, Quote | Decimal] = {}
        while True:
            try:
                quote: Quote = await asyncio.wait_for(get_next_quote(), timeout=timeout)
                if quote is None:
                    break

                # quote = quote_to_dict(quote=quote)
                if just_midpoint:
                    quote_dict[quote.eventSymbol] = (
                        (quote.askPrice + quote.bidPrice) / 2
                        if quote.askPrice and quote.bidPrice
                        else None
                    )
                elif just_ask:
                    quote_dict[quote.eventSymbol] = quote.askPrice
                elif just_bid:
                    quote_dict[quote.eventSymbol] = quote.bidPrice
                else:
                    quote_dict[quote.eventSymbol] = quote

                if len(quote_dict) >= len(symbols):
                    break

            except asyncio.TimeoutError as to:
                print(to)
                break

        await streamer.unsubscribe_quote(
            symbols=symbols,
        )

    if return_df:
        rows = []
        for contract_code, quote in quote_dict.items():
            date = cme_contract_code_to_datetime(contract_code)
            rows.append({
                "Date": date,
                "Contract": contract_code,
                "Price": quote
            })

        df = pd.DataFrame(rows)
        df = df[df["Price"].notna()]
        df = df.sort_values(by=['Date'], ascending=True)
        df = df.drop("Date", axis=1)
        return df
        
    return quote_dict


async def get_futures_historical_data(
    session: Session,
    start_date: datetime,
    end_date: datetime,
    interval: str,
    timeout=1,
    symbols: Optional[List[str]] = None,
    contract_code: str = None,
    xlsx_path: Optional[str] = None,
    run_converison=False,
):
    async with DXLinkStreamer(session) as streamer:
        if symbols:
            await streamer.subscribe_candle(
                symbols=symbols,
                interval=interval,
                start_time=start_date,
                extended_trading_hours=True,
            )
        elif contract_code:
            streamer_codes: Dict[str, str] = get_all_streamer_symbols(
                session=session, future_contract_code=contract_code
            )
            symbols = streamer_codes.values()
            await streamer.subscribe_candle(
                symbols=streamer_codes.values(),
                interval=interval,
                start_time=start_date,
                extended_trading_hours=True,
            )
        else:
            return

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


async def get_historical_forward_curves(
    session: Session,
    contract_code: str,
    start_date: datetime,
    end_date: datetime,
    buffer: int = 5,
    interval: str = "1d",
    timeout: int = 1,
    xlsx_path: Optional[str] = None,
    df_value_key: str = "close",
    return_df: bool = False,
) -> Dict[datetime, Dict[str, Candle | Dict[str, str | int | float]]] | pd.DataFrame:
    year_start = int(str(start_date.year)[-2:])
    year_end = int(str(end_date.year + buffer)[-2:])
    all_contracts_streamer_symbols = gen_futures_streamer_symbols(
        session=session,
        contract_root=contract_code,
        year_start=year_start,
        year_end=year_end,
    )

    async with DXLinkStreamer(session) as streamer:
        await streamer.subscribe_candle(
            symbols=all_contracts_streamer_symbols,
            interval=interval,
            start_time=start_date,
            extended_trading_hours=True,
        )

        async def get_next_candle():
            async for candle in streamer.listen(EventType.CANDLE):
                return candle
            return None

        try:
            candles_dict: Dict[
                datetime, Dict[str, Candle | Dict[str, str | int | float]]
            ] = {}

            while True:
                try:
                    candle: Candle = await asyncio.wait_for(
                        get_next_candle(), timeout=timeout
                    )
                    if candle is None:
                        print("Stream closed, exiting.")
                        break

                    curr_candle = candle_to_dict(candle=candle)
                    curr_candle["time"] = convert_to_chicago(curr_candle["time"])
                    curr_date = datetime.fromtimestamp(curr_candle["time"] // 1000)
                    curr_ticker = curr_candle["eventSymbol"].split(":")[0]

                    if curr_date not in candles_dict:
                        candles_dict[curr_date] = {}

                    if xlsx_path or return_df:
                        return_df = True
                        candles_dict[curr_date][curr_ticker] = curr_candle[df_value_key]
                    else:
                        candles_dict[curr_date][curr_ticker] = curr_candle

                except asyncio.TimeoutError:
                    print(f"No data received for {timeout} seconds, exiting.")
                    break
                except Exception as e:
                    print("Big Error ", e)
                    break

        finally:
            await streamer.unsubscribe_candle(
                symbols=all_contracts_streamer_symbols,
                interval=interval,
                extended_trading_hours=True,
            )

        if return_df:
            df = pd.DataFrame(candles_dict)
            df = df.transpose()
            df = df.sort_index()
            sorted_contracts = sort_cme_contracts(list(df.columns.values))
            df = df[sorted_contracts]
            df = df.iloc[1:]
            df = (
                df.drop(df.columns[0], axis=1) if df.iloc[:, 0].isnull().all() else None
            )
            df = df.ffill(axis=1)
            df.index = pd.to_datetime(df.index)
            df.index = df.index.tz_localize(None)
            df.to_excel(xlsx_path) if xlsx_path else None
            return df

        return candles_dict
