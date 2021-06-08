import time
import asyncio
import aiohttp

from exchanges import *
from dbmanager import DBManager
from version import print_version


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
        ]

    exchanges = []
    for s in symbols:
        for e_class in exchange_list:
            e = e_class(s)
            exchanges.append(e)
    return exchanges    


async def runner():

    db_path = 'db'
    exchanges = construct_exchanges()

    with DBManager(db_path=db_path) as db:
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
                        print(record)
                    db.insert_records(records=records)
                    
                    time.sleep(1)
            except KeyboardInterrupt:
                print("Interruped...")


def main():
    print_version()
    asyncio.run(runner())
    

if __name__ == "__main__":
    main()
