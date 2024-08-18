import pandas as pd
from utils.log_utils import *
from utils.file_utils import *
import time
from datetime import datetime, timedelta


# Considers 6-month returns, see https://www.bauer.uh.edu/rsusmel/phd/jegadeesh-titman93.pdf
# ‘Returns to Buying Winners and Selling Losers’ (Jegadeesh/Titman)


class MomentumScreener1:
    def __init__(self):
        pass

    def calculate_momentum_factor(self, symbol, prices_df):
        # Check minimum length
        if len(prices_df) < 252:
            logw(f"Not enough price data for {symbol}")
            return 0
        # Current price (latest)
        current_price = prices_df['close'][
            -2]  # NOTE: -2 because -1 is the current incomplete bar in live trading_tools

        # Price 6 months
        start_month_price = prices_df['close'][-126]  # trading_tools days only

        # Calculate momentum factor
        start_month_change = (current_price - start_month_price) / start_month_price

        return round(start_month_change, 4)

    def run(self, symbol_list, prices_dict):
        logi(f"Calculating momentum....")
        momentum_df = pd.DataFrame()
        lookback_days = 400
        i = 1
        for symbol in symbol_list:
            # logd(f"Calculating momentum for {symbol}... ({i}/{len(symbol_list)})")

            # Get prices
            if symbol not in prices_dict:
                logw(f"No prices for {symbol}")
                continue
            prices_df = prices_dict[symbol]

            # Fetch price history
            start_date = datetime.today() - timedelta(days=lookback_days)
            prices_df = prices_df[prices_df.index >= start_date]
            store_csv(CACHE_DIR, f"{symbol}_prices.csv", prices_df)

            # Calculate momentum factor
            momentum_factor = self.calculate_momentum_factor(symbol, prices_df)

            row = pd.DataFrame({'symbol': [symbol], 'momentum_change': [momentum_factor]})
            momentum_df = pd.concat([momentum_df, row], axis=0, ignore_index=True)
            i += 1

        # sort by highest momentum
        momentum_df.sort_values(by=["momentum_change"], ascending=[False], inplace=True)

        return momentum_df

