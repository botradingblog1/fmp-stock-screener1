import pandas as pd
from config import *
from utils.fmp_client import FmpClient
from utils.log_utils import *
import time
from datetime import datetime, timedelta


class FmpPriceLoader:
    def __init__(self, fmp_api_key):
        self.fmp_client = FmpClient(fmp_api_key)

    def fetch(self, symbol_list):
        prices_dict = {}
        lookback_days = 365 * 3
        for symbol in symbol_list:
            logd(f"Fetching prices for {symbol}...")

            # Fetch price history
            start_date = datetime.today() - timedelta(days=lookback_days)

            prices_df = self.fmp_client.fetch_daily_prices(symbol)
            if prices_df is None and len(prices_df) > 252:
                logw(f"Not enough price data for {symbol}")
                continue

            # Check for min price requirement
            last_price = prices_df['close'].iloc[-1]
            if last_price < MIN_PRICE:
                logi(f"{symbol} price doesn't meet minimum of {MIN_PRICE}")
                continue

            prices_df = prices_df[prices_df.index >= start_date]          

            prices_dict[symbol] = prices_df

        return prices_dict

