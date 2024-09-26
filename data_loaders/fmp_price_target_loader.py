import pandas as pd
from datetime import datetime, timedelta
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
from config import *


class FmpPriceTargetLoader:
    """
    Filters earnings estimates based on minimum average future estimate changes for a given number of periods.
    """

    def __init__(self, fmp_api_key: str = ''):
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)

    def load(self, symbol_list: list, prices_dict: dict, lookback_days=60):
        results = []

        # Fetch price targets for the given symbols
        price_target_dict = self.fmp_data_loader.fetch_multiple_price_targets(symbol_list, cache_data=False)

        # Get the current date to filter out past estimates
        current_date = pd.to_datetime(datetime.now().date()).tz_localize(None)
        start_date = current_date - timedelta(days=lookback_days)

        for symbol, price_target_df in price_target_dict.items():
            # Look up current price
            if symbol not in prices_dict:
                continue

            prices_df = prices_dict[symbol]
            current_price = prices_df['close'].iloc[-1]

            # Ensure necessary columns are available and format date columns
            price_target_df['publishedDate'] = pd.to_datetime(price_target_df['publishedDate']).dt.tz_localize(None)
            price_target_df = price_target_df[
                ['symbol', 'publishedDate', 'priceTarget', 'adjPriceTarget', 'priceWhenPosted']]

            # Filter the DataFrame to only include records within the lookback period
            price_target_df = price_target_df[price_target_df['publishedDate'] >= start_date]

            # Sort by date to ensure dates are in order
            price_target_df = price_target_df.sort_values(by='publishedDate')

            if price_target_df.empty:
                continue

            # Calculate percentage change in adjusted price targets relative to the current price
            price_target_df['priceTargetChangePercent'] = (
                price_target_df['adjPriceTarget'] - current_price) / current_price * 100

            # Calculate average price target percentage change
            avg_price_target_change_percent = price_target_df['priceTargetChangePercent'].mean()

            # Calculate the coefficient of variation (standard deviation / mean) of price target changes
            price_target_coefficient_variation = (
                price_target_df['priceTargetChangePercent'].std() / avg_price_target_change_percent
            ) if avg_price_target_change_percent != 0 else 0

            # Calculate agreement: ratio of positive to negative price target changes
            positive_changes = price_target_df['priceTargetChangePercent'] > 0
            negative_changes = price_target_df['priceTargetChangePercent'] < 0
            if negative_changes.sum() > 0:
                price_target_agreement_ratio = positive_changes.sum() / negative_changes.sum()
            else:
                price_target_agreement_ratio = 0  # Handle cases where there are no negative changes

            # Number of analysts
            num_price_target_analysts = len(price_target_df)

            results.append({
                'symbol': symbol,
                'avg_price_target_change_percent': round(avg_price_target_change_percent, 2),
                'price_target_coefficient_variation': round(price_target_coefficient_variation, 2),
                'price_target_agreement_ratio': round(price_target_agreement_ratio, 2),
                'num_price_target_analysts': num_price_target_analysts
            })

        price_targets_df = pd.DataFrame(results)

        return price_targets_df
