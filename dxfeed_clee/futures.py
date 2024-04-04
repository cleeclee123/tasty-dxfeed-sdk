from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

import requests

from session import Session


@dataclass
class Future:
    product_code: str
    tick_size: Decimal
    notional_multiplier: Decimal
    display_factor: Decimal
    last_trade_date: datetime
    expiration_date: datetime
    closing_only_date: datetime
    active: bool
    active_month: bool
    next_active_month: bool
    is_closing_only: bool
    stops_trading_at: datetime
    expires_at: datetime
    product_group: str
    exchange: str
    streamer_exchange_code: str
    back_month_first_calendar_symbol: bool
    instrument_type = "Future"
    streamer_symbol: Optional[str] = None
    is_tradeable: Optional[bool] = None
    future_product: Optional[str] = None
    contract_size: Optional[Decimal] = None
    main_fraction: Optional[Decimal] = None
    sub_fraction: Optional[Decimal] = None
    first_notice_date: Optional[datetime] = None
    roll_target_symbol: Optional[str] = None
    true_underlying_symbol: Optional[str] = None
    future_etf_equivalent: Optional[str] = None
    tick_sizes: Optional[List[int]] = None
    option_tick_sizes: Optional[List[int]] = None
    spread_tick_sizes: Optional[List[int]] = None


def get_future(session: Session, contract_code: str) -> Future:
    params: Dict[str, Any] = {"product-code": contract_code}
    response = requests.get(
        f"{session.base_url}/instruments/futures/",
        headers=session.headers,
        params={k: v for k, v in params.items() if v is not None},
    )
    if response.ok:
        return response.json()["data"]
    return None


def get_futures(
    session: Session,
    symbols: Optional[List[str]] = None,
    product_codes: Optional[List[str]] = None,
) -> List[Future]:
    params: Dict[str, Any] = {"symbol[]": symbols, "product-code[]": product_codes}
    response = requests.get(
        f"{session.base_url}/instruments/futures",
        headers=session.headers,
        params={k: v for k, v in params.items() if v is not None},
    )
    return response.json()["data"]["items"] if response.ok else None


def get_all_streamer_symbols(
    session: Session,
    future_contract_code: str,
    flip_keys=False,
) -> Dict[str, str]:
    futures_obj_list: List[Future] = get_futures(
        session=session, product_codes=[future_contract_code]
    )
    if future_contract_code:
        if flip_keys:
            return {
                contract["streamer-symbol"]: contract["symbol"]
                for contract in futures_obj_list
            }
        return {
            contract["symbol"]: contract["streamer-symbol"]
            for contract in futures_obj_list
        }

    return None


def gen_futures_streamer_symbols(
    session: Session,
    contract_root: str,
    year_start: int,
    year_end: int,
    active_months: List[str] = None,
    exchange_code: str = None,
    only_active=True,
):
    month_codes = ["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"]

    if not active_months or not exchange_code:
        future_info = get_future(session=session, contract_code=contract_root)
        if future_info:
            active_months = future_info["items"][0]["future-product"]["listed-months"]
            exchange_code = future_info["items"][0]["future-product"][
                "streamer-exchange-code"
            ]
        else:
            return []

    now = datetime.now()
    current_month = now.month
    current_yr = now.year
    symbols = []
    for year in range(year_start, year_end + 1):
        for month_code in active_months:
            if only_active:
                year_case = current_yr < year
                month_case = year_case and current_month < month_codes.index(month_code)
                if year or month_case:
                    symbol = f"/{contract_root}{month_code}{year}:{exchange_code}"
                    symbols.append(symbol)
            else:
                symbol = f"/{contract_root}{month_code}{year}:{exchange_code}"
                symbols.append(symbol)

    return symbols


def cme_contract_code_to_datetime(contract_code: str) -> datetime:
    month_codes = ["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"]
    contract_code = contract_code.replace("/", "")
    stripped = contract_code.split(":")[0][2:]
    month_number = month_codes.index(stripped[0]) + 1
    year = 2000 + int(stripped[1:])
    return datetime(year, month_number, 1)
