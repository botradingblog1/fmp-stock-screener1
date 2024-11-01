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


class FmpIncomeDataLoader:
    def __init__(self, fmp_api_key):
        self.fmp_client = FmpDataLoader(fmp_api_key)

    def fetch(self, symbol, period='quarterly', lookback_periods=4):
        logi(f"Fetching income sheet data....")

        results = {
                'last_revenue': 0,
                'last_net_income': 0,
                'last_operating_expenses': 0,
                'avg_revenue': 0,
                'avg_net_income': 0,
                'avg_operating_expenses': 0,
                'revenue_slope': 0,
                'net_income_slope': 0,
                'operating_expenses_slope': 0
            }

        # fetch remotely
        income_df = self.fmp_client.get_income_statement(symbol, period=period)
        if income_df is None or len(income_df) == 0:
            return results

        # Sort data by most recent last
        income_df = income_df.sort_values(by='date', ascending=True)

        # Calculate percentage changes
        income_df['revenue_change'] = income_df['revenue'].pct_change(1, fill_method='bfill')
        income_df['net_income_change'] = income_df['netIncome'].pct_change(1, fill_method='bfill')
        income_df['operating_expenses_change'] = income_df['operatingExpenses'].pct_change(1, fill_method='bfill')
        income_df.dropna(inplace=True)

        # Calculate slopes
        add_kernel_reg_smoothed_line(income_df, column_list=['revenue_change', 'net_income_change', 'operating_expenses_change'],
                                     output_cols=['revenue_change_smoothed', 'net_income_change_smoothed', 'operating_expenses_change_smoothed'], bandwidth=2, var_type='c')
        income_df = compute_slope(income_df, "revenue_change_smoothed", "revenue_change_slope", 3)
        income_df = compute_slope(income_df, "net_income_change_smoothed", "net_income_change_slope", 3)
        income_df = compute_slope(income_df, "operating_expenses_change_smoothed", "operating_expenses_change_slope", 3)

        results['revenue_change_slope'] = income_df['revenue_change_slope'].iloc[-1]
        results['net_income_change_slope'] = income_df['net_income_change_slope'].iloc[-1]
        results['operating_expenses_change_slope'] = income_df['operating_expenses_change_slope'].iloc[-1]

        # Get last periods
        income_df = income_df.tail(lookback_periods)

        results['last_revenue'] = income_df['revenue'].iloc[-1]
        results['last_net_income'] = income_df['netIncome'].iloc[-1]
        results['last_operating_expenses'] = income_df['operatingExpenses'].iloc[-1]

        results['avg_revenue'] = income_df['revenue'].mean()
        results['avg_net_income'] = income_df['netIncome'].mean()
        results['avg_operating_expenses'] = income_df['operatingExpenses'].mean()

        return results



