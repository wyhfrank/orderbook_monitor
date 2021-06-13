import time
import asyncio
import aiohttp
import dotenv

from exchanges import *
from version import print_version
from utils import get_db_manager


def construct_exchanges():

    symbols = [
        "btc_jpy", 
        "eth_jpy", 
        "xrp_jpy", 
        "ltc_jpy",
        "eth_btc",
        "mona_jpy",
        "xlm_jpy",
        ]

    # These symbols are only supported by bitbank:
    # symbols = [
        # "xrp_btc",
        # "ltc_btc",
        # "mona_btc",
        # "bcc_jpy",
        # "bcc_btc",
        # "xlm_btc",
        # "qtum_jpy",
        # "qtum_btc",
        # "bat_jpy",
        # "bat_btc",
    # ]
    # symbols = ["btc_jpy"]    

    exchange_list = [
        Bitbank, 
        Decurrent, 
        GMOCoin, 
        Bitflyer, 
        Coincheck, 
        BITPoint, 
        Quoine,
        Zaif,
        Huobi,
        ]

    exchanges = []
    for s in symbols:
        for e_class in exchange_list:
            e = e_class(s)
            exchanges.append(e)
    return exchanges    


async def runner():

    exchanges = construct_exchanges()

    with get_db_manager() as db:
        db.create_tables_safe()

        async with aiohttp.ClientSession() as session:
            try:
                while True:
                    tasks = []
                    timestamp = int(time.time())
                    for e in exchanges:
                        tasks.append(asyncio.ensure_future(e.get_latest_orderbook(session=session, timestamp=timestamp)))

                    records = await asyncio.gather(*tasks)
                    records = list(filter(lambda x: x is not None, records))
                    for record in records:
                        log_record(record)
                    db.insert_records(records=records)
                    
                    time.sleep(2)
            except KeyboardInterrupt:
                print("Interruped...")


def log_record(record):
    print("Syb[{}]\tEx[{}]\tAsk1[{:.1f}]\tBid1[{:.1f}]\tTime[{}]".format(
        record['symbol'], record['exchange'], record['best_ask'], 
        record['best_bid'], record['timestamp'], 
    ))


def main():
    dotenv.load_dotenv()
    print_version()
    asyncio.run(runner())
    

if __name__ == "__main__":
    main()
