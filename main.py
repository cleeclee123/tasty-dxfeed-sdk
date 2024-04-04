import asyncio
import time
from datetime import datetime

import pandas as pd
import requests
from dateutil import tz

from dxfeed_clee.candle import candle_to_dict
from dxfeed_clee.event import EventType
from dxfeed_clee.futures import (
    get_all_streamer_symbols,
    gen_futures_streamer_symbols,
    cme_contract_code_to_datetime,
)
from dxfeed_clee.quote import Quote
from functions import get_futures_historical_data, get_quotes
from session import ProductionSession, Session
from streamer import DXLinkStreamer


def get_future(session: Session, symbol: str):
    symbol = symbol.replace("/", "")
    response = requests.get(
        f"{session.base_url}/instruments/futures/{symbol}", headers=session.headers
    )
    if response.ok:
        data = response.json()["data"]
        return data

    print(response.status_code)
    return None


def get_cryto(session: Session, symbol: str):
    params = {"symbol[]": [symbol]}
    response = requests.get(
        f"{session.base_url}/instruments/cryptocurrencies",
        headers=session.headers,
        params=params,
    )
    if response.ok:
        data = response.json()["data"]["items"]
        return data

    print(response.status_code)
    return None


if __name__ == "__main__":
    df_secret = pd.read_csv(r"C:\Users\chris\trade\curr_pos\secret.txt")
    my_email = df_secret.iloc[0]["email"]
    my_username = df_secret.iloc[0]["username"]
    my_password = df_secret.iloc[0]["password"]
    session = ProductionSession(my_username, my_password, remember_token=True)

    # print(session.base_url)
    # print(session.dxfeed_url)
    # print(session.dxlink_url)
    # print(session.rest_url)

    # fu = get_future(session, "CLK4")
    # print(fu)

    # from_zone = tz.gettz("UTC")
    # to_zone = tz.gettz("America/Chicago")

    t1 = time.time()
    # start_date = datetime(2024, 3, 1)
    # end_date = datetime(2024, 3, 28)
    # interval = "1d"
    # df = asyncio.run(
    #     get_futures_historical_data(
    #         session=session,
    #         # symbols=["BTC/USD:CXTALP"],
    #         contract_code="CL",
    #         start_date=start_date,
    #         end_date=end_date,
    #         interval=interval,
    #         timeout=0.1,
    #         # run_converison=True,
    #         xlsx_path="temp.xlsx",
    #     )
    # )
    # print(df)

    # symbols = gen_futures_streamer_symbols(
    #     session=session, contract_root="NG", year_start=25, year_end=30
    # )
    # # quotes = asyncio.run(get_quotes(session=session, contract_code="NG", just_ask=True))
    # quotes = asyncio.run(get_quotes(session=session, contract_code="NG", symbols=symbols, just_ask=True))
    # print(quotes)

    date = cme_contract_code_to_datetime("/NGH30:XNYM")
    print(date)

    t2 = time.time()
    print(f"{t2 - t1} seconds")

    # ts = generate_timestamps(
    #     start_date=datetime(2024, 3, 27), end_date=datetime(2024, 3, 28), interval="1h"
    # )
    # date_strings = [
    #     datetime.fromtimestamp(ts / 1000).strftime("%d-%m, %H:%M:%S") for ts in ts
    # ]
    # print(date_strings)
