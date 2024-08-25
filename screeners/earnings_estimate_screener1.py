import pandas as pd
from datetime import datetime, timedelta


class EarningsEstimateScreener1:
    """
    Filters earnings estimates based on min. average future estimate changes for x periods.
    """
    def __init__(self):
        pass

    def run(self, earnings_estimates_dict, min_avg_estimate_percent=0.1, min_num_analysts=2, num_period=4):
        results = []

        # Get the current date to filter out past estimates
        current_date = pd.to_datetime(datetime.now().date())

        for symbol, earnings_estimate_df in earnings_estimates_dict.items():
            # Ensure necessary columns are available and format date columns
            earnings_estimate_df = earnings_estimate_df[
                ['target_date', 'estimatedEpsAvg', 'estimatedEpsHigh', 'estimatedEpsLow', 'numberAnalystsEstimatedEps']]
            earnings_estimate_df['target_date'] = pd.to_datetime(earnings_estimate_df['target_date'])

            # Filter out dates that are in the past
            earnings_estimate_df = earnings_estimate_df[earnings_estimate_df['target_date'] >= current_date]

            # Sort by target_date to ensure future dates are in order
            earnings_estimate_df = earnings_estimate_df.sort_values(by='target_date')

            # Ensure that we have enough periods to consider
            if len(earnings_estimate_df) < num_period:
                continue

            # Select the first num_period rows for future periods
            future_periods_df = earnings_estimate_df.head(num_period)

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
        eps_estimates_df = eps_estimates_df[eps_estimates_df['avg_eps_growth_percent'] >= min_avg_estimate_percent]

        # Filter based on minimum number of analysis
        eps_estimates_df = eps_estimates_df[eps_estimates_df['avg_num_analysts'] >= min_num_analysts]

        return eps_estimates_df
