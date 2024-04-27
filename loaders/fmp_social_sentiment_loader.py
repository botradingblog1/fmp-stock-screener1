import pandas as pd
from config import *
from utils.fmp_client import FmpClient
import time
from utils.log_utils import *
from datetime import datetime, timedelta
import re
import time
import requests
from datetime import datetime, timedelta
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
from typing import Tuple
device = "cuda:0" if torch.cuda.is_available() else "cpu"

FINBERT_MAX_TOKENS = 512
tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert").to(device)
labels = ["positive", "negative", "neutral"]

os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"


class FmpSocialSentimentLoader:
    def __init__(self, fmp_api_key):
        self.fmp_client = FmpClient(fmp_api_key)

    def fetch(self, symbol_list):
        #  Iterate through symbols
        all_social_sentiment_df = pd.DataFrame({})
        for symbol in symbol_list:
            logd(f"Loading social media sentiment for {symbol}...")

            # Fetch social sentiment
            social_sentiment_df = self.fmp_client.get_social_sentiment(symbol)
            if social_sentiment_df is None or len(social_sentiment_df) == 0:
                print(f"No social sentiment for {symbol}")
                continue

            #  Add individual stock results to all results
            all_social_sentiment_df = pd.concat([all_social_sentiment_df, social_sentiment_df], axis=0, ignore_index=True)

            # Throttle for API limit
            time.sleep(api_request_delay)

        # Calculate likes ratio (likes / posts) - not needed. We can use 'stocktwitsSentiment'
        # all_social_sentiment_df['social_sentiment_ratio'] = round(all_social_sentiment_df['stocktwitsLikes'] / all_social_sentiment_df['stocktwitsPosts'], 2)

        # Filter - only keep records from last 30 days
        start_date = datetime.today() - timedelta(days=30)
        all_social_sentiment_df = all_social_sentiment_df[all_social_sentiment_df['date'] >= start_date]

        # Store results
        file_name = "all_social_sentiment_df.csv"
        path = os.path.join(CACHE_DIR, file_name)
        all_social_sentiment_df.to_csv(path)

        # Group by stocks and calculate mean / std
        social_sentiment_stats_df = all_social_sentiment_df.groupby('symbol')['stocktwitsSentiment'].agg(['mean', 'std']).reset_index()
        file_name = "social_sentiment_stats_df.csv"
        path = os.path.join(CACHE_DIR, file_name)
        social_sentiment_stats_df.to_csv(path)

        return social_sentiment_stats_df


