import os.path

import pandas as pd
from config import *
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
from utils.log_utils import *
import time
from utils.df_utils import cap_outliers
from utils.file_utils import *
from datetime import datetime


class FmpGrowthLoader1:
    def __init__(self, fmp_api_key):
        self.fmp_client = FmpDataLoader(fmp_api_key)

    def fetch(self, symbol_list):
        logi(f"Fetching income growth data....")
        i = 1
        income_growth_dict = {}
        for symbol in symbol_list:
            #logd(f"Fetching growth for {symbol}... ({i}/{len(symbol_list)})")

            # Fetch quarterly growth
            today = datetime.today()
            today_str = today.strftime("%Y-%m-%d")
            file_name = f"{symbol}_income_growth_{today_str}.csv"
            path = os.path.join(CACHE_DIR, file_name)
            if os.path.exists(path):
                income_growth_df = pd.read_csv(path)
            else:
                income_growth_df = self.fmp_client.get_income_growth(symbol, period="quarter")
            if income_growth_df is None or len(income_growth_df) == 0:
                logd(f"No income growth data from {symbol}")
                continue

            # Sort data by most recent last
            income_growth_df['date'] = pd.to_datetime(income_growth_df['date'], errors='coerce')
            income_growth_df = income_growth_df.sort_values(by='date', ascending=True)
            store_csv(CACHE_DIR, file_name, income_growth_df)

            income_growth_dict[symbol] = income_growth_df

            i += 1

            # Throttle for API limit
            time.sleep(API_REQUEST_DELAY)

        return income_growth_dict