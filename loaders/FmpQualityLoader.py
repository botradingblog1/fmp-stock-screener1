import pandas as pd
from config import *
from utils.fmp_client import FmpClient
from utils.log_utils import *
from utils.file_utils import *
from utils.df_utils import cap_outliers
import time


class FmpQualityLoader:
    def __init__(self, fmp_api_key):
        self.fmp_client = FmpClient(fmp_api_key)

    def calculate_quality_factor(self, symbol, ratios_df):
        # Check minimum length
        if ratios_df is None or len(ratios_df) == 0:
            logw(f"Not enough income quality data for {symbol}")
            return 0

        return_on_equity = ratios_df['returnOnEquity'].iloc[0] or 0
        debt_equity_ratio = ratios_df['debtEquityRatio'].iloc[0] or 0

        # Calculate quality factor
        quality_factor = 0.5 * return_on_equity - 0.5 * debt_equity_ratio
        return quality_factor

    def fetch(self, symbol_list):
        quality_results_df = pd.DataFrame()
        i = 1
        for symbol in symbol_list:
            logd(f"Fetching quality info for {symbol}... ({i}/{len(symbol_list)})")

            # Fetch quarterly ratios
            quarterly_ratios_df = self.fmp_client.get_financial_ratios(symbol, period="quarterly")
            store_csv(CACHE_DIR, f"{symbol}_quarterly_ratios.csv", quarterly_ratios_df)

            # Calculate Quality factor
            quarterly_quality_factor = self.calculate_quality_factor(symbol, quarterly_ratios_df)

            # Fetch annual ratios
            annual_ratios_df = self.fmp_client.get_financial_ratios(symbol, period="annual")
            store_csv(CACHE_DIR, f"{symbol}_annual_ratios.csv", quarterly_ratios_df)

            # Calculate Quality factor
            annual_quality_factor = self.calculate_quality_factor(symbol, annual_ratios_df)

            # Combine quarterly and annual factors
            quality_factor = 0.6 * quarterly_quality_factor + 0.4 * annual_quality_factor

            row = pd.DataFrame({'symbol': [symbol], 'quality_factor': [quality_factor]})
            quality_results_df = pd.concat([quality_results_df, row], axis=0, ignore_index=True)

            i += 1

            # Throttle for API limit
            time.sleep(API_REQUEST_DELAY)

        # Cap outliers in the growth factor results
        quality_results_df = cap_outliers(quality_results_df, "quality_factor")

        return quality_results_df
