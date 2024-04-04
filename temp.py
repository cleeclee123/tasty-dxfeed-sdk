import asyncio
import pandas as pd
from tastytrade.dxfeed import EventType
from tastytrade import DXLinkStreamer, ProductionSession


async def main(session):
    subs_list = ["SPY", "SPX"]
    async with DXLinkStreamer(session) as streamer:
        await streamer.subscribe(EventType.QUOTE, subs_list)
        async for quote in streamer.listen(EventType.QUOTE):
            print(quote)


if __name__ == "__main__":
    df_secret = pd.read_csv(r"C:\Users\chris\trade\curr_pos\secret.txt")
    my_email = df_secret.iloc[0]["email"]
    my_username = df_secret.iloc[0]["username"]
    my_password = df_secret.iloc[0]["password"]
    session = ProductionSession(my_username, my_password, remember_token=True)
    asyncio.run(main(session))
