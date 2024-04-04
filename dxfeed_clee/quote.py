from decimal import Decimal
from typing import Optional, Dict

from .event import Event


class Quote(Event):
    eventSymbol: str
    eventTime: int
    sequence: int
    timeNanoPart: int
    bidTime: int
    bidExchangeCode: str
    askTime: int
    askExchangeCode: str
    bidPrice: Optional[Decimal] = None
    askPrice: Optional[Decimal] = None
    bidSize: Optional[int] = None
    askSize: Optional[int] = None


def quote_to_dict(quote: Quote) -> Dict[str, str | int | float]:
    return {
        "eventSymbol": quote.eventSymbol,
        "eventTime": quote.eventTime,
        "sequence: int": quote.sequence,
        "timeNanoPart": quote.timeNanoPart,
        "bidTime": quote.bidTime,
        "bidExchangeCode": quote.bidExchangeCode,
        "askTime": quote.askTime,
        "askExchangeCode": quote.askExchangeCode,
        "bidPrice": quote.bidPrice,
        "askPrice": quote.askPrice,
        "bidSize": quote.bidSize,
        "askSize": quote.askSize,
    }
