import os.path

import pandas as pd
from config import *
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
from utils.log_utils import *
import time
from utils.df_utils import cap_outliers
from utils.file_utils import *
from datetime import datetime


GROWTH_DATA_DIR = "cache/growth_data"


class FmpGrowthLoader1:
    def __init__(self, fmp_api_key):
        self.fmp_client = FmpDataLoader(fmp_api_key)

    def fetch(self, symbol_list):
        logi(f"Fetching income growth data....")
        i = 1
        income_growth_dict = {}
        for symbol in symbol_list:
            logd(f"Fetching growth for {symbol}... ({i}/{len(symbol_list)})")

            today_str = datetime.today().strftime("%Y-%m-%d")
            file_name = f"{symbol}_{today_str}_growth.csv"
            path = os.path.join(GROWTH_DATA_DIR, file_name)
            if os.path.exists(path):
                # Load from cache
                growth_df = pd.read_csv(path)
            else:
                # fetch remotely
                growth_df = self.fmp_client.get_income_growth(symbol, period="quarter")
                # Sort data by most recent last
                growth_df['date'] = pd.to_datetime(growth_df['date'], errors='coerce')
                growth_df = growth_df.sort_values(by='date', ascending=True)
                store_csv(CACHE_DIR, file_name, growth_df)

            if growth_df is None or len(growth_df) == 0:
                logd(f"No income growth data from {symbol}")
                continue

            income_growth_dict[symbol] = growth_df

            i += 1

            # Throttle for API limit
            time.sleep(API_REQUEST_DELAY)

        return income_growth_dict