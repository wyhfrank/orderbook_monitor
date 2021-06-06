import time
import asyncio
import aiohttp

from exchanges import *
from dbmanager import DBManager


def construct_exchanges():
    symbols = ["ltc_jpy"]
    # symbols = ["btc_jpy", "eth_jpy", "xrp_jpy", "ltc"]
    exchange_list = [Bitbank, Decurrent, GMOCoin, Bitflyer, Coincheck]

    exchanges = []
    for s in symbols:
        for e_class in exchange_list:
            e = e_class(s)
            exchanges.append(e)
    return exchanges    


async def runner():

    db_path = 'db'
    exchanges = construct_exchanges()
    db = DBManager(db_path=db_path)
    db.connect()
    db.create_db()

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
    db.close()

def main():
    asyncio.run(runner())
    

main()
