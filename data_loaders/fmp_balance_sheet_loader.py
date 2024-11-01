import os.path

import pandas as pd
from config import *
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
from utils.log_utils import *
import time
from utils.df_utils import cap_outliers
from utils.file_utils import *
from datetime import datetime
from utils.indicator_utils import *


class FmpBalanceSheetDataLoader:
    def __init__(self, fmp_api_key):
        self.fmp_client = FmpDataLoader(fmp_api_key)

    def fetch(self, symbol, period='quarterly', lookback_periods=4):
        logi(f"Fetching balance sheet data....")

        results = {
                'last_cash_cash_equivalents': 0,
            }

        # fetch remotely
        balance_sheet_df = self.fmp_client.get_balance_sheet(symbol, period=period)
        if balance_sheet_df is None or len(balance_sheet_df) == 0:
            return results

        # Sort data by most recent last
        balance_sheet_df = balance_sheet_df.sort_values(by='date', ascending=True)

        results['last_cash_cash_equivalents'] = balance_sheet_df['cashAndCashEquivalents'].iloc[-1]

        return results