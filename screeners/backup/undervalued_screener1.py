import pandas as pd
from utils.log_utils import *
import numpy as np
from botrading.data_processing.data_processing_tools import add_kernel_reg_smoothed_line, compute_slope
import pandas_ta as ta

"""
 Trend screener checks if the price is below short-term EMA and that last trend is bullish
"""

# Configuration
SHORT_TERM_EMA = 20
MIN_TREND_SLOPE = 0.01
KERNEL_REG_BANDWIDTH = 3


class UndervaluedScreener1:
    def get_current_trend(self, symbol: str, prices_df: pd.DataFrame):
        # Add smoothed line
        prices_df = add_kernel_reg_smoothed_line(prices_df, column_list=['adj_close'], bandwidth=KERNEL_REG_BANDWIDTH, var_type='c')

        # Calculate slope
        prices_df = compute_slope(prices_df, target_col='adj_close_smoothed', slope_col='adj_close_smoothed_slope',
                                  window_size=3)

        # Get last slope value
        trend_slope = prices_df['adj_close_smoothed_slope'].iloc[-1]

        return trend_slope

    def run(self, symbol_list, prices_dict):
        logi(f"Calculating trends....")
        trends_df = pd.DataFrame()

        i = 1
        for symbol in symbol_list:
            logd(f"Calculating trends for {symbol}... ({i}/{len(symbol_list)})")

            # Get prices
            if symbol not in prices_dict:
                logw(f"No prices for {symbol}")
                continue
            prices_df = prices_dict[symbol]

            # Calculate current trend
            current_trend = self.get_current_trend(symbol, prices_df)

            # Check price below EMA
            prices_df[f'EMA_{SHORT_TERM_EMA}'] = ta.ema(prices_df['close'], length=SHORT_TERM_EMA)
            prices_df[f'price_below_ema_{SHORT_TERM_EMA}'] = np.where((prices_df['close'] < prices_df[f'EMA_{SHORT_TERM_EMA}']), 1, 0)
            price_below_ema = prices_df[f'price_below_ema_{SHORT_TERM_EMA}'].iloc[-1]

            row = pd.DataFrame({'symbol': [symbol],
                                'price_below_ema': [price_below_ema],
                                'short_term_trend': [current_trend]})
            trends_df = pd.concat([trends_df, row], axis=0, ignore_index=True)
            i += 1

        # Perform filters
        trends_df = trends_df[trends_df['price_below_ema'] == 1]
        trends_df = trends_df[trends_df['short_term_trend'] >= MIN_TREND_SLOPE]

        return trends_df


