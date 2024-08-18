import pandas as pd
from utils.log_utils import *
from datetime import datetime, timedelta


"""
 Calculated 52-week high and filters by minimum drop
"""

# Configuration
MIN_PRICE_DROP_PERCENT = 0.2  # percent


class UndervaluedScreener2:
    def run(self, symbol_list, prices_dict):
        logi(f"Finding undervalued stocks....")
        undervalued_data = []

        for symbol in symbol_list:
            # Get prices
            if symbol not in prices_dict:
                logw(f"No prices for {symbol}")
                continue

            prices_df = prices_dict[symbol]
            if prices_df is None or len(prices_df) < 252:
                logw(f"Not enough data for {symbol}")
                continue

            # Get the last year's data
            one_year_ago = datetime.now() - timedelta(days=365)
            last_year_data = prices_df[prices_df.index >= one_year_ago]

            if last_year_data.empty:
                logw(f"No data for {symbol} in the last year")
                continue

            # Ensure the DataFrame has a 'Close' column
            if 'close' not in prices_df.columns:
                logw(f"No 'close' price data for {symbol}")
                continue

            # Calculate 52-week high
            fifty_two_week_high = prices_df['close'].max()

            # Get the most recent closing price
            most_recent_close = prices_df['close'].iloc[-1]

            # Calculate the price drop from 52-week high
            price_drop_percent = ((fifty_two_week_high - most_recent_close) / fifty_two_week_high)

            # Filter by minimum price drop percentage
            if price_drop_percent >= MIN_PRICE_DROP_PERCENT:
                undervalued_data.append({
                    'symbol': symbol,
                    'price_drop_percent': price_drop_percent,
                    'fifty_two_week_high': fifty_two_week_high
                })

        # Convert the list of dictionaries to a DataFrame
        undervalued_df = pd.DataFrame(undervalued_data)

        return undervalued_df
