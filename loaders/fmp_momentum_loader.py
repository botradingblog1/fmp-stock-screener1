import pandas as pd
from config import *
from utils.fmp_client import FmpClient
from utils.log_utils import *
import time
from datetime import datetime, timedelta
import numpy as np
from utils.df_utils import cap_outliers


class FmpMomentumLoader:
    def __init__(self, fmp_api_key):
        self.fmp_client = FmpClient(fmp_api_key)

    def calculate_momentum_factor(self, prices_df):
        # Current price (latest)
        current_price = prices_df['close'][-2]  # NOTE: -2 because -1 is the current incomplete bar in live trading

        # Price 6 months ago
        six_month_price = prices_df['close'][-126]  # trading days only

        # Price 12 months ago
        twelve_month_price = prices_df['close'][-252]  # trading days only

        # Calculate momentum factor
        six_month_momentum = (current_price / six_month_price - 1) - RISK_FREE_RATE / 2
        twelve_month_momentum = (current_price / twelve_month_price - 1) - RISK_FREE_RATE

        # Weighted Momentum Factor calculation
        momentum_factor = 0.5 * six_month_momentum + 0.5 * twelve_month_momentum
        return momentum_factor

    def fetch(self, symbol_list, prices_dict):
        momentum_df = pd.DataFrame()
        lookback_days = 400
        i = 1
        for symbol in symbol_list:
            logd(f"Calculating momentum for {symbol}... ({i}/{len(symbol_list)})")

            # Get prices
            if symbol not in prices_dict:
                logw(f"No prices for {symbol}")
                continue
            prices_df = prices_dict[symbol]

            # Fetch price history
            start_date = datetime.today() - timedelta(days=lookback_days)
            prices_df = prices_df[prices_df.index >= start_date]
            print(len(prices_df))

            # Check minimum length
            if len(prices_df) < 252:
                logw(f"Not enough price data for {symbol}")
                continue

            # Calculate momentum factor
            momentum_factor = self.calculate_momentum_factor(prices_df)
            if momentum_factor < MIN_MOMENTUM_FACTOR:
                logw(f"{symbol} does not meet minimum Momentum Factor of {MIN_MOMENTUM_FACTOR}")
                continue

            row = pd.DataFrame({'symbol': [symbol], 'momentum_factor': [momentum_factor]})
            momentum_df = pd.concat([momentum_df, row], axis=0, ignore_index=True)
            i += 1

        # Cap outliers
        momentum_df = cap_outliers(momentum_df, 'momentum_factor')

        return momentum_df

