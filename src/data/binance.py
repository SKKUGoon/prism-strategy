from typing import Literal, List, Tuple, Callable
from datetime import datetime, timedelta
import requests
import pandas as pd
import time


class RateLimitFactory:
    def __init__(self, interval: int, limit: int):
        self.interval = interval  # in seconds
        self.limit = limit

        self.last_requested_time = time.time()
        self.used_weight = 0

    def __repr__(self):
        return f"RateLimit interval: {self.interval}s, limit {self.used_weight}/{self.limit} (updated {self.last_requested_time})"

    def update(self, weight_func: Callable | None = None):
        """
        Decorator to update the used weight. 
        If it's over the limit, 
          it will sleep until the next start of theinterval
        """
        if not callable(weight_func):
            static_weight = weight_func
            weight_func = lambda *args, **kwargs: static_weight
        
        def decorator(func):
            def wrapper(*args, **kwargs):
                # Get the weight dynamically
                weight = weight_func(*args, **kwargs)
                print(f"Decorator: setting weight to {weight}")

                current_time = time.time()
                if current_time - self.last_requested_time >= self.interval:
                    self.last_requested_time = current_time
                    self.used_weight = 0

                if self.used_weight + weight >= self.limit:
                    sleep_time = self.interval - (current_time - self.last_requested_time)
                    if sleep_time > 0:
                        print(f"RATE LIMIT - Sleeping for {sleep_time} seconds")
                        time.sleep(sleep_time)

                    self.last_requested_time = time.time()
                    self.used_weight = 0

                self.used_weight += weight
                return func(*args, **kwargs)
            return wrapper
        return decorator    


class BinanceRateLimitsFactory:
    def __init__(self, data: List[dict]):
        """Return the weight limit translated into seconds"""
        self.data = data

    def _interval_to_seconds(self, interval: str) -> int:
        """Helper method to convert interval string to seconds"""
        intervals = {
            'SECOND': 1,
            'MINUTE': 60,
            'HOUR': 3600,
            'DAY': 86400
        }
        return intervals.get(interval, 1)

    def get_request_weight_limit(self) -> int:
        wl = [d for d in self.data if d['rateLimitType'] == 'REQUEST_WEIGHT']
        return RateLimitFactory(
            interval=self._interval_to_seconds(wl[0]['interval']),
            limit=wl[0]['limit']
        )
    
    def get_orders_limit(self):
        raise NotImplementedError('Not implemented')
    

class BinanceSymbol:
    def __init__(self, total_symbols: pd.DataFrame):
        self.symbols = total_symbols
        self.target_symbol = None
        self.target_symbol_filters = None

    def get_total_symbols(self):
        return self.symbols['symbol'].unique()

    def set_target_symbol(self, symbol: str):
        self.target_symbol = symbol.upper()
        self.target_symbol_filters = self.symbols.loc[self.symbols['symbol'] == symbol].iloc[0]['filters']

    def get_price_filters(self) -> dict:
        return [f for f in self.target_symbol_filters if f['filterType'] == 'PRICE_FILTER'][0]
    
    def get_lot_size_filters(self) -> dict:
        return [f for f in self.target_symbol_filters if f['filterType'] == 'LOT_SIZE'][0]


class BinanceInformation:
    def __init__(self):
        self.base_url = 'https://api.binance.com'
        self.exchange_info = self.get_info()

    def get_info(self):
        endpoint = '/api/v3/exchangeInfo'
        r = requests.get(url=f'{self.base_url}{endpoint}')
        if r.status_code == 200:
            return r.json()
        else:
            raise Exception(f'Error: {r.status_code} {r.text}')
        
    def get_rate_limits(self):
        return BinanceRateLimitsFactory(self.exchange_info['rateLimits'])

    def get_trading_symbols(self) -> BinanceSymbol:
        symbols = pd.DataFrame(self.exchange_info['symbols'])
        symbols = symbols.loc[
            (symbols['status'] == 'TRADING') &
            (symbols['quoteAsset'] == 'USDT') &
            (symbols['isSpotTradingAllowed'] == True) &
            (symbols['isMarginTradingAllowed'] == True)
        ]

        symbols = symbols[['symbol', 'baseAsset', 'quoteAsset', 'quotePrecision', 'filters']]

        return BinanceSymbol(symbols)


# Create Exchange info and rate limits
bi = BinanceInformation()

# Trading symbols
symbol_searcher = bi.get_trading_symbols()

# Rate limits
_rate_limits = bi.get_rate_limits()  # Hidden to the user
weight_limiter = _rate_limits.get_request_weight_limit()


class BinanceHistory:
    def __init__(self):
        self.base_url = 'https://api.binance.com'
        self.symbol = None

    def set_symbol(self, symbol: str):
        self.symbol = symbol
        print(f"Setting symbol to {symbol}")

    def _calculate_klines_weight(self,
                                 interval: Literal['1s', '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M'] = '1s',
                                 time: Tuple[datetime, datetime] | None = None,  # startTime - Long (Timestamp) 1499040000000
                                 time_zone: str = '9',
                                 number_of_minutes: int | None = 500,
                                 *args, 
                                 **kwargs):
        if time is None:
            curr_time = datetime.now().replace(microsecond=0)
            end_time = int(curr_time.timestamp() * 1000)
            start_time = int((curr_time - timedelta(minutes=number_of_minutes)).timestamp() * 1000)

        else:
            start_time = int(time[0].timestamp() * 1000)
            end_time = int(time[1].timestamp() * 1000)
            number_of_minutes = (end_time - start_time) / (1000 * 60)  # Convert milliseconds to minutes, Override the `limit` parameter        

        if number_of_minutes > 1000:
            # Split them into multiple requests: Each request can have at most 1000 klines
            # Calculate number of 1000-minute intervals needed
            interval_ms = 1000 * 60 * 1000  # 1000 minutes in milliseconds
            num_intervals = (end_time - start_time) // interval_ms + 1
            
            # Generate all interval start/end times at once using list comprehension
            requests_times = [
                (
                    start_time + (i * interval_ms), 
                    min(start_time + ((i + 1) * interval_ms), end_time) - 1  # -1 to avoid the INCLUSIVE last minute
                )  
                for i in range(int(num_intervals))
            ]
            return 2 * len(requests_times)
        else:
            return 2    

    @weight_limiter.update(weight_func=_calculate_klines_weight)  # weight is 2 for each request
    def klines(self, 
               interval: Literal['1s', '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M'] = '1s',
               time: Tuple[datetime, datetime] | None = None,  # startTime - Long (Timestamp) 1499040000000
               time_zone: str = '9',
               number_of_minutes: int | None = 500) -> list[list[float]]:
        # API Endpoint
        endpoint = '/api/v3/klines'

        if time is None:
            curr_time = datetime.now().replace(microsecond=0)
            end_time = int(curr_time.timestamp() * 1000)
            start_time = int((curr_time - timedelta(minutes=number_of_minutes)).timestamp() * 1000)
            print(start_time, end_time)
        else:
            start_time = int(time[0].timestamp() * 1000)
            end_time = int(time[1].timestamp() * 1000)
            number_of_minutes = (end_time - start_time) / (1000 * 60)  # Convert milliseconds to minutes, Override the `limit` parameter

        requests_times = []
        if number_of_minutes > 1000:
            # Split them into multiple requests: Each request can have at most 1000 klines
            # Calculate number of 1000-minute intervals needed
            interval_ms = 1000 * 60 * 1000  # 1000 minutes in milliseconds
            num_intervals = (end_time - start_time) // interval_ms + 1
            
            # Generate all interval start/end times at once using list comprehension
            requests_times = [
                (
                    start_time + (i * interval_ms), 
                    min(start_time + ((i + 1) * interval_ms), end_time) - 1  # -1 to avoid the INCLUSIVE last minute
                )  
                for i in range(int(num_intervals))
            ]
            print(f"Splitting into {len(requests_times)} requests")
            number_of_minutes = 1000
        else:
            requests_times = [(start_time, end_time)]

        # Make requests
        columns = [
            'timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 
            'quote_volume', 'count', 'taker_buy_volume', 'taker_buy_quote_volume', 'ignore'
        ]
        data = []
        for i, (start_time, end_time) in enumerate(requests_times):
            print(f"Requesting {self.symbol} batch {i+1}/{len(requests_times)} \
                  - Containing {datetime.fromtimestamp(start_time/1000).strftime('%Y-%m-%d %H:%M:%S')} \
                  - {datetime.fromtimestamp(end_time/1000).strftime('%Y-%m-%d %H:%M:%S')}")
            params = {
                'symbol': self.symbol,
                'interval': interval,
                'startTime': start_time,
                'endTime': end_time,
                'timeZone': time_zone,
                'limit': number_of_minutes,
            }
            r = requests.get(url=f'{self.base_url}{endpoint}', params=params)

            if r.status_code == 200:
                data.append(pd.DataFrame(r.json(), columns=columns))
            else:
                raise Exception(f'Error: {r.status_code} {r.text}')

        return pd.concat(data)

