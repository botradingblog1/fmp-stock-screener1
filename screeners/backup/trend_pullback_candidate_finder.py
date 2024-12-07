import numpy as np
import pandas as pd
import os
from botrading.data_loaders.tiingo_data_loader import TiingoDataLoader, TiingoDailyInterval
from data_loaders.market_symbol_loader import MarketSymbolLoader
from utils.plot_utils import plot_pullback_chart
from utils.file_utils import delete_files_in_directory
from utils.log_utils import *
from utils.indicator_utils import *
from config import *
from datetime import datetime, timedelta



PULLBACK_PLOTS_DIR = "C:\\dev\\trading\\data\\trend_pullbacks\\plots"



class TrendPullbackFinder:
    def __init__(self, tiingo_api_key: str):
        self.data_loader = TiingoDataLoader(tiingo_api_key)

    def calculate_indicators(self, df: pd.DataFrame):
        long_ema_window = 10
        short_ema_window = 3

        # Calculate indicators
        df['trend_slope'] = calculate_trend(df)
        df['adx'] = calculate_adx(df, length=14)
        df['rsi'] = calculate_rsi(df, window=14)
        df[f"ema_short"] = calculate_ema(df, window=short_ema_window)
        df[f"ema_long"] = calculate_ema(df, window=long_ema_window)
        df.dropna(inplace=True)

        return df

    def find_uptrend_pullbacks(self, df):
        # Define conditions for a strong uptrend
        adx_lookback = 5
        strong_uptrend = (df['trend_slope'] > 0.1) & (df['adx'].shift(adx_lookback) > 25)
        pullback_condition = (df[f"ema_short"] < df[f"ema_long"])

        # Create a signal column for potential pullback entries
        df['long_signal'] = np.where(strong_uptrend & pullback_condition, 1, 0)

        return df

    def find_downtrend_pullbacks(self, df):
        # Define conditions for a strong downtrend
        adx_lookback = 5
        strong_downtrend = (df['trend_slope'] < -0.1) & (df['adx'].shift(adx_lookback) > 25)
        pullback_condition = (df[f"ema_short"] > df[f"ema_long"])

        # Create a signal column for potential pullback entries
        df['short_signal'] = np.where(strong_downtrend & pullback_condition, 1, 0)

        return df

    def find_trend_pullbacks(self):
        logi("Performing trend pullback analysis...")
        delete_files_in_directory(PULLBACK_PLOTS_DIR)

        # Load symbols
        symbol_loader = MarketSymbolLoader()
        symbol_list_df = symbol_loader.fetch_sp500_symbols(cache_file=True, cache_dir="cache")
        symbol_list = symbol_list_df['symbol'].unique()

        # Set start and end dates
        start_date = datetime.today() - timedelta(days=400)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date = datetime.today()
        end_date_str = end_date.strftime("%Y-%m-%d")

        # Load daily price info
        logi("Fetching prices...")
        prices_dict = self.data_loader.fetch_multiple_end_of_day_prices(symbol_list, start_date_str, end_date_str,
                                                                        interval=TiingoDailyInterval.DAILY,
                                                                        cache_data=True, cache_dir=CACHE_DIR)

        for symbol in symbol_list:
            print(f"Processing {symbol}")
            if symbol not in prices_dict:
                print(f"symbol {symbol} not found in prices_dict")
                continue

            prices_df = prices_dict[symbol]
            if prices_df is None or len(prices_df) < 200:
                logi(f"No price data fetched for symbol {symbol}.")
                continue
            prices_df.reset_index(inplace=True)

            # Calculate indicators
            prices_df = self.calculate_indicators(prices_df)

            # Detect pullbacks
            prices_df = self.find_uptrend_pullbacks(prices_df)
            prices_df = self.find_downtrend_pullbacks(prices_df)

            # Check if any of the last signals are active before plotting
            has_long_signal = prices_df['long_signal'].iloc[-1] == 1 or prices_df['long_signal'].iloc[-2] == 1
            has_short_signal = prices_df['short_signal'].iloc[-1] == 1 or prices_df['short_signal'].iloc[-2] == 1

            if has_long_signal or has_short_signal:
                # Plot chart only if there is a signal
                plot_pullback_chart(symbol, prices_df, PULLBACK_PLOTS_DIR, file_name=f"{symbol}_pullback_chart.png")

        logi("Done with trend pullback analysis.")
