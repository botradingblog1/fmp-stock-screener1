import pandas as pd
from config import *
from utils.fmp_client import FmpClient
from utils.log_utils import *
import time
from datetime import datetime, timedelta


class FmpPriceLoader:
    def __init__(self, fmp_api_key):
        self.fmp_client = FmpClient(fmp_api_key)

    def fetch_all(self):
        all_prices_df = self.fmp_client.fetch_all_prices()
        return all_prices_df

    def fetch(self, symbol_list):
        prices_dict = {}
        lookback_days = 365 * 3
        for symbol in symbol_list:
            logd(f"Fetching prices for {symbol}...")

            # Fetch price history
            start_date = datetime.today() - timedelta(days=lookback_days)
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date = datetime.today()
            end_date_str = end_date.strftime("%Y-%m-%d")

            prices_df = self.fmp_client.fetch_daily_prices(symbol, start_date_str, end_date_str)
            if prices_df is None or len(prices_df) < 252:
                logw(f"Not enough price data for {symbol}")
                continue

            prices_dict[symbol] = prices_df

        return prices_dict

