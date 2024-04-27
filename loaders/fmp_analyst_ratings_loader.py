import pandas as pd
from config import *
from utils.fmp_client import FmpClient
import time


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

    def fetch(self, symbol_list):
        num_months_data_cutoff = 3

        #  Fetch all symbols
        symbols = symbol_list
        #  Iterate through symbols
        all_grades_df = pd.DataFrame({})
        for symbol in symbols:
            print(f"Loading analyst ratings for {symbol}...")

            # Fetch analyst data
            grades_df = self.fmp_client.fetch_analyst_ratings(symbol)
            if grades_df is None or len(grades_df) == 0:
                print(f"No grades for {symbol}")
                continue

            # Filter out data more than x months in the past
            cutoff_date = pd.Timestamp.now() - pd.DateOffset(months=num_months_data_cutoff)
            grades_df = grades_df[(grades_df['date'] >= cutoff_date) & grades_df['date'].notna()]

            # Aggregate counts
            grades_df = self.aggregate_rating_counts(symbol, grades_df)

            #  Add individual stock results to all results
            all_grades_df = pd.concat([all_grades_df, grades_df], axis=0, ignore_index=True)

            # Throttle for API limit
            time.sleep(api_request_delay)

        # Sort by total rating
        all_grades_df.sort_values(by=['total_rating'], ascending=[False], inplace=True)

        # Store results
        file_name = "analyst_ratings.csv"
        path = os.path.join(RESULTS_DIR, file_name)
        all_grades_df.to_csv(path)

        return all_grades_df
