import pandas as pd
from sklearn.linear_model import LinearRegression
import numpy as np


class GrowthScreener1:
    """
    Filters growth data by minimum earnings and revenue growth
    Also checks if the growth accelerated in the last two quarters
    """
    def __init__(self):
        pass

    def calculate_slope(self, series):
        X = np.arange(len(series)).reshape(-1, 1)
        y = series.values
        model = LinearRegression().fit(X, y)
        slope = model.coef_[0]
        return slope

    def run(self, growth_data_dict, min_quarterly_earnings_growth=0.1, min_quarterly_revenue_growth=0.1):
        results = []

        for symbol, growth_df in growth_data_dict.items():
            # Ensure the data is sorted with the most recent data last
            growth_df = growth_df.sort_values(by='date', ascending=True).reset_index(drop=True)
            growth_df = growth_df[['growthRevenue', 'growthNetIncome']]

            # Only keep the last 4 quarters for analysis
            growth_df = growth_df.tail(4)

            # Get last earnings and revenue
            last_quarter_earnings_growth = growth_df['growthNetIncome'].iloc[-1]
            prev_earnings_growth = growth_df['growthNetIncome'].iloc[-2]
            earnings_acceleration = last_quarter_earnings_growth > prev_earnings_growth

            last_quarter_revenue_growth = growth_df['growthRevenue'].iloc[-1]
            prev_revenue_growth = growth_df['growthRevenue'].iloc[-2]
            revenue_acceleration = last_quarter_revenue_growth > prev_revenue_growth

            results.append({
                'symbol': symbol,
                'last_quarter_earnings_growth': last_quarter_earnings_growth,
                'last_quarter_revenue_growth': last_quarter_revenue_growth,
                'earnings_acceleration': earnings_acceleration,
                'revenue_acceleration': revenue_acceleration
            })

        growth_df = pd.DataFrame(results)

        # Filter out low growth records
        growth_df = growth_df[growth_df['last_quarter_revenue_growth'] >= min_quarterly_revenue_growth]
        growth_df = growth_df[growth_df['last_quarter_earnings_growth'] >= min_quarterly_earnings_growth]
        growth_df = growth_df[growth_df['earnings_acceleration'] == True]
        growth_df = growth_df[growth_df['revenue_acceleration'] == True]

        return growth_df