import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
from utils.log_utils import *


class FmpAnalystEstimatesLoader:
    """
    Filters earnings estimates based on minimum average future estimate changes for a given number of periods.
    """

    def __init__(self, fmp_api_key: str = ''):
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)

    def load(self, symbol: str, period: str ="quarterly"):
        # Fetch price targets
        estimates_df = self.fmp_data_loader.fetch_analyst_earnings_estimates(symbol, period=period)

        if estimates_df is None or estimates_df.empty:
            logi(f"No estimates for symbol {symbol}")
            return pd.DataFrame(), {
                'avg_revenue_change_percent': 0.0,
                'revenue_change_coefficient_variation': 0.0,
                'avg_num_analysts': 0
            }

        # Format date
        estimates_df['date'] = pd.to_datetime(estimates_df['date']).dt.tz_localize(None)

        # We are only interested in future estimates
        estimates_df = estimates_df[estimates_df['date'] > datetime.today()]
        if len(estimates_df) == 0:
            return pd.DataFrame(), {
                'avg_revenue_change_percent': 0.0,
                'revenue_change_coefficient_variation': 0.0,
                'avg_num_analysts': 0
            }

        # Sort by date to ensure dates are in order
        estimates_df = estimates_df.sort_values(by='date')

        # Calculate percentage change for revenue
        estimates_df['revenue_change_pct'] = estimates_df['estimatedRevenueAvg'].pct_change().dropna()

        # Replace infinite values
        estimates_df.replace([np.inf, -np.inf], 0, inplace=True)

        # Calculate average revenue percentage change
        avg_revenue_change_percent = estimates_df['revenue_change_pct'].mean()

        # Calculate the coefficient of variation (standard deviation / mean) of revenue changes
        revenue_change_coefficient_variation = (
            estimates_df['revenue_change_pct'].std() / avg_revenue_change_percent
        ) if avg_revenue_change_percent != 0 else 0

        # Number of analysts (use mean to get the average number of analysts across the period)
        avg_num_analysts = estimates_df['numberAnalystEstimatedRevenue'].mean()

        # Results dictionary
        results = {
            'avg_revenue_change_percent': round(avg_revenue_change_percent, 2),
            'revenue_change_coefficient_variation': round(revenue_change_coefficient_variation, 2),
            'avg_num_analysts': int(avg_num_analysts)  # Converting to int as it's more intuitive for counting analysts
        }

        return estimates_df, results

