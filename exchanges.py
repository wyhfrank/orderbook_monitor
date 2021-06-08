import asyncio
import json
import aiohttp
import aiohttp.client_exceptions


class ExchangeBase:
    name = ''
    url = ''
    top_n = 10
    symbol_map = None
    retry_count_down = 10  # Retry after N times

    def __init__(self, symbol='btc_jpy') -> None:
        self.initialize()
        self.symbol = symbol
        self.formatted_symbol = self.format_symbol(symbol)
        self.retry_counter = 0
    
    def get_url(self):
        return self.url.format(self.formatted_symbol)
    
    @classmethod
    def format_symbol(cls, symbol):
        if cls.symbol_map:
            if callable(cls.symbol_map):
                return cls.symbol_map(symbol)
            if cls.symbol_map.get(symbol):
                return cls.symbol_map.get(symbol)
        return symbol

    async def _send_request(self, session):
        async with session.get(self.get_url()) as resp:
            data = await resp.json()
            return data
    
    @classmethod
    def parse_orderbook(cls, data):
        raise NotImplementedError()

    async def get_latest_orderbook(self, session, timestamp=None):
        if not self.retry_status_ok():
            print("Skip: [{}] [{}]. Retry after {} times.".format(self.name, self.symbol, self.retry_counter))
            return None

        try:
            data = await self._send_request(session=session)
        except (aiohttp.ClientConnectionError, asyncio.exceptions.TimeoutError):
            print("Cannot connect to {}".format(self.get_url()))
            self.setup_retry()
            return None
        except (aiohttp.ContentTypeError, aiohttp.client_exceptions.ClientPayloadError):
            print("Cannot parse content from {}".format(self.get_url()))
            self.setup_retry()
            return None

        try:
            res = self.parse_orderbook(data)
        except (TypeError, KeyError):
            print("Cannot parse data: [{}] [{}], url: {}".format(self.name, self.symbol, self.get_url()))
            self.setup_retry()
            return None
        if isinstance(res, dict):
            if timestamp:
                res['timestamp'] = timestamp
            res['exchange'] = self.name
            res['symbol'] = self.symbol
        return res
    
    @classmethod
    def initialize(cls):
        return
    
    def retry_status_ok(self):
        if self.retry_counter > 0:
            self.retry_counter -= 1
            return False
        return True
    
    def setup_retry(self):
        self.retry_counter = self.retry_count_down


class ParseLayerBase(ExchangeBase):
    """An example of parsing the data using list position or dictionary key
    """
    ask_key = "asks"
    bid_key = "bids"
    price_pos = "price"   # or a number for list type
    amount_pos = "amount" # or a number for list type
    
    @classmethod
    def preprocess_data(cls, data):
        return data

    @classmethod
    def parse_orderbook(cls, data):
        data = cls.preprocess_data(data)

        asks = cls.parse_depth(data[cls.ask_key][:cls.top_n])
        bids = cls.parse_depth(data[cls.bid_key][:cls.top_n])
        if len(asks) < 1 or len(bids) < 1:
            raise TypeError("Depth data length is 0.")
        best_ask = asks[0]['price']
        best_bid = bids[0]['price']
        best_ask = float(best_ask)
        best_bid = float(best_bid)

        res = {
            "best_ask": best_ask,
            "best_bid": best_bid,
            "depth": {
                "ask": asks,
                "bid": bids,
            }
        }            
        return res

    @classmethod
    def parse_depth(cls, depth_records):
        return [
            {'price': record[cls.price_pos], 
            'amount': record[cls.amount_pos]} 
            for record in depth_records
        ]


class Bitbank(ParseLayerBase):
    name = 'bitbank'
    url = 'https://public.bitbank.cc/{0}/depth'
    price_pos = 0
    amount_pos = 1
    
    @classmethod
    def preprocess_data(cls, data):
        return data["data"]


class Decurrent(ParseLayerBase):
    name = 'decurrent'
    url = 'https://api-trade.decurret.com/api/v1/orderbook?symbolId={0}'

    # These four are the only supported symbols
    # Check here: https://api-trade.decurret.com/api/v1/symbol
    symbol_map = {
        "btc_jpy": 1,
        "eth_btc": 2,
        "eth_jpy": 3,
        "xrp_jpy": 8,
    }


class GMOCoin(ParseLayerBase):
    name = 'gmocoin'
    url = 'https://api.coin.z.com/public/v1/orderbooks?symbol={0}'
    symbol_map = str.upper

    amount_pos = "size"

    @classmethod
    def preprocess_data(cls, data):
        return data["data"]


class Bitflyer(ParseLayerBase):
    name = 'bitflyer'
    url = 'https://api.bitflyer.com/v1/board?product_code={0}'

    amount_pos = "size"


class Coincheck(ParseLayerBase):
    name = 'coincheck'
    url = 'https://coincheck.com/api/order_books?pair={0}'

    price_pos = 0
    amount_pos = 1


class BITPoint(ParseLayerBase):
    name = 'bitpoint'
    url = 'https://smartapi.bitpoint.co.jp/bpj-smart-api/api/depth?symbol={0}'

    price_pos = 'price'
    amount_pos = 'qty'

    @classmethod
    def symbol_map(cls, symbol: str):
        return str.upper(symbol.replace("_",""))


class Quoine(ParseLayerBase):
    name = 'quoine'
    url = 'https://api.liquid.com/products/{0}/price_levels'
    
    ask_key = 'sell_price_levels'
    bid_key = 'buy_price_levels'
    price_pos = 0
    amount_pos = 1
    
    @classmethod
    def load_symbol_map(cls):
        print("loading symbol map for " + cls.__name__)
        import requests
        url_products = 'https://api.liquid.com/products'

        symbol_map = {}
        try:
            res = requests.get(url=url_products).json()
            for record in res:
                id = record["id"]
                currency = record["currency"]
                base_currency = record["base_currency"]

                symbol = str.lower("{}_{}".format(base_currency, currency))
                symbol_map[symbol] = id
        except json.JSONDecodeError:
            pass

        # print(symbol_map)
        cls.symbol_map = symbol_map

    @classmethod
    def initialize(cls):
        if not cls.symbol_map:
            cls.load_symbol_map()


def get_quoine_products():
    Quoine.load_symbol_map()


def single_test():

    async def runner():

        # e = Bitbank()
        e = Quoine()
        async with aiohttp.ClientSession() as session:
            res = await e.get_latest_orderbook(session=session)
            print(res)

    asyncio.run(runner())

if __name__ == "__main__":
    # get_quoine_products()
    single_test()
    pass
