import pandas as pd
from config import *
from utils.fmp_client import FmpClient
from utils.log_utils import *
from utils.df_utils import cap_outliers
from datetime import datetime, timedelta
import time
import numpy as np


class FmpSocialSentimentLoader:
    def __init__(self, fmp_api_key):
        self.fmp_client = FmpClient(fmp_api_key)

    def calculate_social_sentiment_score(self, sentiment_df):
        mean_sentiment = sentiment_df['stocktwitsSentiment'].mean()

        return mean_sentiment

    def fetch(self, symbol_list):
        #  Iterate through symbols
        results_df = pd.DataFrame({})
        for symbol in symbol_list:
            logd(f"Loading social media sentiment for {symbol}...")

            # Fetch social sentiment
            social_sentiment_df = self.fmp_client.get_social_sentiment(symbol)
            if social_sentiment_df is None or len(social_sentiment_df) == 0:
                print(f"No social sentiment for {symbol}")
                sentiment_score = 0
            else:
                # Filter - only keep records from last quarter days
                start_date = datetime.today() - timedelta(days=90)
                social_sentiment_df = social_sentiment_df[social_sentiment_df['date'] >= start_date]

                # Calculate score
                sentiment_score = self.calculate_social_sentiment_score(social_sentiment_df)
                if np.isnan(sentiment_score):
                    sentiment_score = 0

            row = pd.DataFrame({'symbol': [symbol], 'social_sentiment_score': [sentiment_score]})
            results_df = pd.concat([results_df, row], axis=0, ignore_index=True)

            # Throttle for API limit
            time.sleep(API_REQUEST_DELAY)

        # Cap values
        results_df = cap_outliers(results_df, 'social_sentiment_score')

        return results_df


