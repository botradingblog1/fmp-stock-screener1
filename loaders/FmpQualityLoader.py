import pandas as pd
from config import *
from utils.fmp_client import FmpClient
from utils.log_utils import *
from utils.file_utils import *
import time
from datetime import datetime, timedelta
import numpy as np


class FmpQualityLoader:
    def __init__(self, fmp_api_key):
        self.fmp_client = FmpClient(fmp_api_key)

    def calculate_quality_factor(self, symbol, ratios_df):
        # Check minimum length
        if ratios_df is None or len(ratios_df) == 0:
            logw(f"Not enough income quality data for {symbol}")
            return 0

        return_on_equity = 0 if ratios_df['returnOnEquity'].iloc[0] is None else ratios_df['returnOnEquity'].iloc[0]
        debt_equity_ratio = 0 if ratios_df['debtEquityRatio'].iloc[0] is None else ratios_df['debtEquityRatio'].iloc[0]

        # Weighted factor calculation
        quality_factor = 0.5 * return_on_equity + 0.5 * debt_equity_ratio
        return quality_factor

    def cap_outliers(self, quality_results_df):
        # Calculate mean and standard deviation
        mean_factor = quality_results_df['quality_factor'].mean()
        std_factor = quality_results_df['quality_factor'].std()

        # Define cutoffs for outliers
        upper_limit = mean_factor + OUTLIER_STD_MULTIPLIER * std_factor
        lower_limit = mean_factor - OUTLIER_STD_MULTIPLIER * std_factor

        # Cap values
        quality_results_df['quality_factor'] = np.where(quality_results_df['quality_factor'] > upper_limit, upper_limit,
                                                      quality_results_df['quality_factor'])
        quality_results_df['quality_factor'] = np.where(quality_results_df['quality_factor'] < lower_limit, lower_limit,
                                                      quality_results_df['quality_factor'])

        return quality_results_df

    def fetch(self, symbol_list):
        quality_results_df = pd.DataFrame()
        i = 1
        for symbol in symbol_list:
            logd(f"Fetching quality factor for {symbol}... ({i}/{len(symbol_list)})")

            # Fetch ratios
            ratios_df = self.fmp_client.get_financial_ratios(symbol, period="quarterly")
            store_csv(CACHE_DIR, f"{symbol}_ratios.csv", ratios_df)

            # Calculate Quality factor
            quality_factor = self.calculate_quality_factor(symbol, ratios_df)

            row = pd.DataFrame({'symbol': [symbol], 'quality_factor': [quality_factor]})
            quality_results_df = pd.concat([quality_results_df, row], axis=0, ignore_index=True)

            i += 1

            # Throttle for API limit
            time.sleep(API_REQUEST_DELAY)

        # Cap outliers in the growth factor results
        quality_results_df = self.cap_outliers(quality_results_df)

        return quality_results_df
