

class ExchangeBase:
    name = ''
    url = ''
    top_n = 3
    symbol_map = None

    def __init__(self, symbol='btc_jpy') -> None:
        self.symbol = symbol
        self.formatted_symbol = self.format_symbol(symbol)
    
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
        data = await self._send_request(session=session)
        try:
            res = self.parse_orderbook(data)
        except (TypeError, KeyError):
            res = None
        if isinstance(res, dict):
            if timestamp:
                res['timestamp'] = timestamp
            res['exchange'] = self.name
            res['symbol'] = self.symbol
        return res


class Decurrent(ExchangeBase):
    name = 'decurrent'
    url = 'https://api-trade.decurret.com/api/v1/orderbook?symbolId={0}'
    symbol_map = {
        "btc_jpy": 1,
        "eth_btc": 2,
        "eth_jpy": 3,
        "xrp_jpy": 8,
    }

    @classmethod
    def parse_orderbook(cls, data):
        best_ask = data["bestAsk"]
        best_bid = data["bestBid"]
        
        res = {
            "best_ask": best_ask,
            "best_bid": best_bid,
        }
        return res


class Bitbank(ExchangeBase):
    name = 'bitbank'
    url = 'https://public.bitbank.cc/{0}/depth'

    @classmethod
    def parse_orderbook(cls, data):
        best_ask = best_bid = None
        if data["success"] == 1:
            asks = data["data"]["asks"][:cls.top_n]
            bids = data["data"]["bids"][:cls.top_n]

            # asks = list(map(int, asks))
            # bids = list(map(int, bids))

            best_ask = asks[0][0] if len(asks) > 0 else None
            best_bid = bids[0][0] if len(bids) > 0 else None
            best_ask = float(best_ask)
            best_bid = float(best_bid)

        res = {
            "best_ask": best_ask,
            "best_bid": best_bid,
        }            
        return res


class GMOCoin(ExchangeBase):
    name = 'gmocoin'
    url = 'https://api.coin.z.com/public/v1/orderbooks?symbol={0}'
    symbol_map = str.upper

    @classmethod
    def parse_orderbook(cls, data):
        best_ask = best_bid = None
        if data["status"] == 0:
            asks = data["data"]["asks"][:cls.top_n]
            bids = data["data"]["bids"][:cls.top_n]

            # asks = list(map(int, asks))
            # bids = list(map(int, bids))

            best_ask = asks[0]["price"] if len(asks) > 0 else None
            best_bid = bids[0]["price"] if len(bids) > 0 else None
            best_ask = float(best_ask)
            best_bid = float(best_bid)

        res = {
            "best_ask": best_ask,
            "best_bid": best_bid,
        }            
        return res

class Bitflyer(ExchangeBase):
    name = 'bitflyer'
    url = 'https://api.bitflyer.com/v1/board?product_code={0}'

    @classmethod
    def parse_orderbook(cls, data):
        asks = data["asks"][:cls.top_n]
        bids = data["bids"][:cls.top_n]

        best_ask = asks[0]["price"] if len(asks) > 0 else None
        best_bid = bids[0]["price"] if len(bids) > 0 else None
        best_ask = float(best_ask)
        best_bid = float(best_bid)

        res = {
            "best_ask": best_ask,
            "best_bid": best_bid,
        }            
        return res


class Coincheck(ExchangeBase):
    name = 'coincheck'
    url = 'https://coincheck.com/api/order_books?pair={0}'

    @classmethod
    def parse_orderbook(cls, data):
        best_ask = best_bid = None
        try:
            asks = data["asks"][:cls.top_n]
            bids = data["bids"][:cls.top_n]

            best_ask = asks[0][0] if len(asks) > 0 else None
            best_bid = bids[0][0] if len(bids) > 0 else None
            best_ask = float(best_ask)
            best_bid = float(best_bid)
        except KeyError:
            pass

        res = {
            "best_ask": best_ask,
            "best_bid": best_bid,
        }            
        return res

