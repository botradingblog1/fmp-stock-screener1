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

    def load(self, symbol: str, lookback_days: int = 60):
        result = {
                'symbol': symbol,
                'avg_target_price': 0,
                'avg_price_target_change_percent': 0,
                'price_target_coefficient_variation': 0,
                'price_target_agreement_ratio': 0,
                'num_price_target_analysts': 0
            }

        # Fetch price targets for the given symbols
        price_target_df = self.fmp_data_loader.fetch_price_targets(symbol, cache_data=False)
        if price_target_df is None or len(price_target_df) == 0:
            return result

        # Get the current date to filter out past estimates
        current_date = pd.to_datetime(datetime.now().date()).tz_localize(None)
        start_date = current_date - timedelta(days=lookback_days)

        # Ensure necessary columns are available and format date columns
        price_target_df['publishedDate'] = pd.to_datetime(price_target_df['publishedDate']).dt.tz_localize(None)
        price_target_df = price_target_df[['symbol', 'publishedDate', 'priceTarget', 'adjPriceTarget', 'priceWhenPosted']]

        # Filter the DataFrame to only include records within the lookback period
        price_target_df = price_target_df[price_target_df['publishedDate'] >= start_date]
        if price_target_df.empty:
            return result

        # Sort by date to ensure dates are in order
        price_target_df = price_target_df.sort_values(by='publishedDate')

        # Calculate percentage change in adjusted price targets relative to the current price
        avg_price_target = price_target_df['adjPriceTarget'].mean()
        price_target_df['priceTargetChangePercent'] = (price_target_df['adjPriceTarget'] - price_target_df['priceWhenPosted']) / price_target_df['priceWhenPosted'] * 100

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

        # Update results
        result['avg_price_target'] = round(avg_price_target, 2)
        result['avg_price_target_change_percent'] = round(avg_price_target_change_percent, 2)
        result['price_target_coefficient_variation'] = round(price_target_coefficient_variation, 2)
        result['price_target_agreement_ratio'] = round(price_target_agreement_ratio, 2)
        result['num_price_target_analysts'] = round(num_price_target_analysts, 2)
        return result

    def load_list(self, symbol_list: list, lookback_days=60):
        results = []

        for symbol in symbol_list:
            result = self.load(symbol, lookback_days)
            results.append(result)

        return results
