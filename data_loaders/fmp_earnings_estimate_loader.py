import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from botrading.data_loaders.fmp_data_loader import FmpDataLoader


class FmpEarningsEstimateLoader:
    """
    Filters earnings estimates based on min. average future estimate changes for x periods.
    """
    def __init__(self, fmp_api_key: str):
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)

    def load(self, symbol_list, period="annual", num_future_periods=4, min_avg_estimate_percent=None, min_num_analysts=0):
        results = []

        # Load data
        earnings_estimates_dict = self.fmp_data_loader.fetch_multiple_analyst_earnings_estimates(symbol_list,
                                                                                                 period=period)

        # Get the current date to filter out past estimates
        current_date = pd.to_datetime(datetime.now().date())

        for symbol, earnings_estimate_df in earnings_estimates_dict.items():
            earnings_estimate_df['date'] = pd.to_datetime(earnings_estimate_df['date'])
            # Ensure necessary columns are available and format date columns
            earnings_estimate_df = earnings_estimate_df[
                ['date', 'estimatedEpsAvg', 'estimatedEpsHigh', 'estimatedEpsLow', 'numberAnalystsEstimatedEps']]

            # Filter out dates that are in the past
            earnings_estimate_df = earnings_estimate_df[earnings_estimate_df['date'] >= current_date]

            # Sort by date to ensure future dates are in order
            earnings_estimate_df = earnings_estimate_df.sort_values(by='date')

            # Ensure that we have enough periods to consider
            if len(earnings_estimate_df) < num_future_periods:
                continue

            # Select the first num_future_periods rows for future periods
            future_periods_df = earnings_estimate_df.head(num_future_periods)

            # Calculate the average estimated EPS for the selected periods
            avg_eps_estimate = future_periods_df['estimatedEpsAvg'].mean()

            # Calculate the growth percentage based on the first available estimate
            initial_eps_estimate = earnings_estimate_df.iloc[0]['estimatedEpsAvg']
            avg_eps_growth_percent = (avg_eps_estimate - initial_eps_estimate) / initial_eps_estimate

            # Calculate avg number of analysis
            avg_num_analysts = future_periods_df['numberAnalystsEstimatedEps'].mean()

            results.append({
                'symbol': symbol,
                'avg_eps_growth_percent': avg_eps_growth_percent,
                'avg_num_analysts': avg_num_analysts
            })

        eps_estimates_df = pd.DataFrame(results)

        # Filter based on minimum required average estimate percent
        if min_avg_estimate_percent:
            eps_estimates_df = eps_estimates_df[eps_estimates_df['avg_eps_growth_percent'] >= min_avg_estimate_percent]

        # Filter based on minimum number of analysis
        if min_num_analysts:
            eps_estimates_df = eps_estimates_df[eps_estimates_df['avg_num_analysts'] >= min_num_analysts]

        return eps_estimates_df
