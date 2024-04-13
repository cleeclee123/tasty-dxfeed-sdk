import asyncio
import time
from datetime import datetime

import pandas as pd
import requests
import ujson as json
from dateutil import tz

from dxfeed_clee.candle import candle_to_dict
from dxfeed_clee.event import EventType
from dxfeed_clee.futures import (cme_contract_code_to_datetime,
                                 gen_futures_streamer_symbols,
                                 get_all_streamer_symbols)
from dxfeed_clee.quote import Quote
from functions import (get_futures_historical_data,
                       get_historical_forward_curves, get_quotes)
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

    # symbols = gen_futures_streamer_symbols(
    #     session=session, contract_root="SR3", year_start=23, year_end=28, only_active=False,
    # )

    start_date = datetime(2020, 1, 1)
    end_date = datetime(2024, 4, 6)
    # interval = "1d"
    # df = asyncio.run(
    #     get_futures_historical_data(
    #         session=session,
    #         contract_code="CL",
    #         # symbols=["/CLZ23:XNYM"],
    #         start_date=start_date,
    #         end_date=end_date,
    #         interval=interval,
    #         timeout=0.1,
    #         # run_converison=True,
    #         xlsx_path="temp.xlsx",
    #     )
    # )
    # print(df)
    
    fwd_curves_dict = asyncio.run(
        get_historical_forward_curves(
            session=session,
            contract_code="NG",
            start_date=start_date,
            end_date=end_date,
            xlsx_path="historical_natgas_futures_curve.xlsx",
            return_df=True
        )
    )
    
    print(fwd_curves_dict) 
    print(json.dumps(fwd_curves_dict, indent=4))  
    

    # quotes = asyncio.run(get_quotes(session=session, contract_code="NG", just_ask=True))
    # quotes = asyncio.run(get_quotes(session=session, contract_code="SR3", symbols=symbols, just_midpoint=True, return_df=True))
    # print(quotes)
    # print(json.dumps(quotes, indent=4))

    # date = cme_contract_code_to_datetime("/NGH30:XNYM")
    # print(date)

    t2 = time.time()
    print(f"{t2 - t1} seconds")

    # ts = generate_timestamps(
    #     start_date=datetime(2024, 3, 27), end_date=datetime(2024, 3, 28), interval="1h"
    # )
    # date_strings = [
    #     datetime.fromtimestamp(ts / 1000).strftime("%d-%m, %H:%M:%S") for ts in ts
    # ]
    # print(date_strings)
