import pandas as pd
from config import *
from utils.fmp_client import FmpClient
from utils.log_utils import *
from utils.file_utils import *
import time
from datetime import datetime
import os

ANALYST_RATINGS_CACHE_DIR = os.path.join("cache", "analyst_ratings")


# Loads analyst ratings from FMP
class FmpAnalystRatingsLoader:
    def __init__(self, fmp_api_key):
        self.fmp_client = FmpClient(fmp_api_key)

    def aggregate_rating_counts(self, symbol, grades_df):
        strong_buy_count = 0
        buy_count = 0
        outperform_count = 0
        sell_count = 0
        strong_sell_count = 0
        underperform_count = 0
        hold_count = 0

        for index, row in grades_df.iterrows():
            grade = row['newGrade']
            if grade == "Strong Buy":
                strong_buy_count += 1
            elif grade == "Buy" or grade == "Long-Term Buy" or grade == "Conviction Buy":
                buy_count += 1
            elif grade == "Outperform" or grade == "Perform" or grade == "Overweight":
                outperform_count += 1
            elif grade == "Strong Sell":
                strong_sell_count += 1
            elif grade == "Sell" or grade == "Long-Term Sell" or grade == "Conviction Sell":
                sell_count += 1
            elif grade == "Underperform" or grade == "Underweight":
                underperform_count += 1
            elif grade == "Hold" or grade == "Equal-Weight":
                hold_count += 1

        bullish_count = 2 * strong_buy_count + buy_count + outperform_count
        bearish_count = 2 * strong_sell_count + sell_count + underperform_count
        total_rating = bullish_count - bearish_count

        grades_count_df = pd.DataFrame({
            'symbol': [symbol],
            'strong_buy_count': [strong_buy_count],
            'buy_count': [buy_count],
            'outperform_count': [buy_count],
            'sell_count': [sell_count],
            'strong_sell_count': [strong_sell_count],
            'underperform_count': [underperform_count],
            'hold_count': [hold_count],
            'bullish_count': [bullish_count],
            'bearish_count': [bearish_count],
            'total_rating': [total_rating],
        })
        return grades_count_df

    def fetch(self, symbol_list, num_lookback_days=60):
        #  Fetch all symbols
        symbols = symbol_list
        #  Iterate through symbols
        i = 1
        results_df = pd.DataFrame({})
        for symbol in symbols:
            logd(f"Loading analyst ratings for {symbol}... ({i}/{len(symbol_list)})")

            today_str = datetime.today().strftime("%Y-%m-%d")
            file_name = f"{symbol}_{today_str}_analyst_ratings.csv"
            path = os.path.join(ANALYST_RATINGS_CACHE_DIR, file_name)
            if os.path.exists(path):
                # Load from cache
                grades_df = pd.read_csv(path)
            else:
                # Fetch remotely
                grades_df = self.fmp_client.get_analyst_ratings(symbol)
                if grades_df is None or len(grades_df) == 0:
                    logw(f"No grades for {symbol}")
                    continue

            # Filter out data more than x months in the past
            cutoff_date = pd.Timestamp.now() - pd.DateOffset(days=num_lookback_days)
            grades_df = grades_df[(grades_df['date'] >= cutoff_date) & grades_df['date'].notna()]

            # Aggregate counts
            grades_df = self.aggregate_rating_counts(symbol, grades_df)

            # Store grades for review
            store_csv(CACHE_DIR, f"{symbol}_analyst_ratings.csv", grades_df)

            #  Add individual stock results to all results
            total_rating = grades_df['total_rating'].iloc[0]
            hold_count = grades_df['hold_count'].iloc[0]
            bullish_count = grades_df['bullish_count'].iloc[0]
            bearish_count = grades_df['bearish_count'].iloc[0]
            row = pd.DataFrame({'symbol': [symbol], 'bullish_count': [bullish_count], 'hold_count': [hold_count],
                                'bearish_count': [bearish_count], 'analyst_rating_score': [total_rating]})
            results_df = pd.concat([results_df, row], axis=0, ignore_index=True)

            i += 1

            # Throttle for API limit
            time.sleep(API_REQUEST_DELAY)

        return results_df
