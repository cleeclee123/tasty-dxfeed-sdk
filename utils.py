import re
from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd
from pytz import timezone as tz


def generate_timestamps(
    start_date: datetime,
    end_date: datetime,
    interval: str,
    from_zone: Optional[tz] = None,
    to_zone: Optional[tz] = None,
) -> List[float]:
    if from_zone and to_zone:
        start_date = start_date.astimezone(to_zone)
        end_date = end_date.astimezone(to_zone)

    all_days = pd.date_range(start=start_date, end=end_date, freq="D")
    no_saturdays = all_days[all_days.dayofweek != 5]
    timestamps = []
    for day in no_saturdays:
        start_datetime = pd.Timestamp(day).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        end_datetime = pd.Timestamp(day).replace(
            hour=23, minute=59, second=59, microsecond=999999
        )
        times = pd.date_range(start=start_datetime, end=end_datetime, freq=interval)
        timestamps.extend([time.value // 10**6 for time in times])

    return timestamps


def interval_to_timedelta(interval_str: str) -> timedelta:
    match = re.match(r"(\d+)([mhd])", interval_str)
    if not match:
        raise ValueError("Invalid interval format")

    amount, unit = match.groups()
    amount = int(amount)

    if unit == "m":
        return timedelta(minutes=amount)
    elif unit == "h":
        return timedelta(hours=amount)
    elif unit == "d":
        return timedelta(days=amount)
    elif unit == "s":
        return timedelta(seconds=amount)
    elif unit == "w":
        return timedelta(weeks=amount)
    else:
        raise ValueError("Unknown time unit")
