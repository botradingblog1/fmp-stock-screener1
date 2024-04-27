import pandas as pd
from config import *
from utils.fmp_client import FmpClient
import time
from utils.log_utils import *
import hashlib
from langdetect import detect, LangDetectException
from bs4 import BeautifulSoup
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


# Loads stock news from FMP
class FmpStockNewsLoader:
    def __init__(self, fmp_api_key):
        self.fmp_client = FmpClient(fmp_api_key)

    def generate_unique_id(self, row):
        # Concatenate the required fields
        record_string = f"{row['symbol']}{row['publishedDate']}{row['title']}{row['site']}"
        # Encode to a byte string before hashing
        encoded_string = record_string.encode()
        # Create the MD5 hash and return the hexadecimal string
        return hashlib.md5(encoded_string).hexdigest()

    def detect_english(self, text):
        try:
            return detect(text) == 'en'
        except LangDetectException:  # Handle exception if language detection fails
            return False

    def filter_non_english_news_items(self, news_df):
        # Filter out all news articles not in English language
        news_df['is_english'] = news_df['text'].apply(self.detect_english)
        news_df = news_df[news_df['is_english']]

        # Drop the 'is_english' column if it's no longer needed
        news_df.drop(columns=['is_english'], inplace=True)
        return news_df

    def clean_article_text(self, text):
        # Remove line breaks, tabs, and multiple whitespace characters
        text = re.sub(r'\s+', ' ', text)
        return text

    def fetch_full_article_text(self, row):
        symbol = row['symbol']
        url = row['url']
        print(f"Fetching full article for {symbol}: {url}")
        try:
            if url is None or url == "":
                return ""

            # Fetch text
            response = requests.get(url)
            response.raise_for_status()  # This will raise an HTTPError

            # If you get a successful response, proceed to parse the text
            soup = BeautifulSoup(response.content, 'lxml')
            article_text = ' '.join(p.get_text().strip() for p in soup.find_all('p'))

            # Clean the text
            article_text = self.clean_article_text(article_text)

            return article_text
        except requests.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except Exception as err:
            print(f"An error occurred: {err}")
        return ""

    def detect_news_sentiment(self, row):
        news_text = f"{row['title']} {row['full_text']}"
        if news_text:
            # Truncate the news_text to the max_length the model can handle
            tokens = tokenizer(news_text, return_tensors="pt", max_length=512, truncation=True,
                               padding="max_length").to(device)

            # Forward pass, get model output
            with torch.no_grad():  # Turn off gradients for inference
                outputs = model(tokens["input_ids"], attention_mask=tokens["attention_mask"])

            # Softmax the result to get probability distributions
            probabilities = torch.nn.functional.softmax(outputs.logits, dim=-1)

            # Get the highest probability index and corresponding probability
            sentiment_idx = torch.argmax(probabilities, dim=1).item()
            probability = probabilities[0][sentiment_idx].item()

            # Assign positive or negative sign based on sentiment
            # Assuming labels are ordered as ["negative", "neutral", "positive"]
            if labels[sentiment_idx] == "positive":
                return probability  # Positive sentiment
            elif labels[sentiment_idx] == "negative":
                return -probability  # Negative sentiment
            else:
                return 0.0  # Neutral sentiment has no impact score
        else:
            return 0.0  # Return 0.0 for empty or None text

    def fetch(self, symbol_list):
        #  Iterate through symbols
        all_news_df = pd.DataFrame({})
        for symbol in symbol_list:
            logd(f"Loading stock news for {symbol}...")

            # Fetch news
            limit = 5
            news_df = self.fmp_client.get_stock_news(symbol, limit)
            if news_df is None or len(news_df) == 0:
                print(f"No news for {symbol}")
                continue

            #  Add individual stock results to all results
            all_news_df = pd.concat([all_news_df, news_df], axis=0, ignore_index=True)

            # Throttle for API limit
            time.sleep(api_request_delay)

        # Fetch full news articles
        all_news_df['full_text'] = all_news_df.apply(self.fetch_full_article_text, axis=1)

        # Get news sentiment
        all_news_df['sentiment_score'] = all_news_df.apply(self.detect_news_sentiment, axis=1)

        # Store results
        file_name = "stock_news.csv"
        path = os.path.join(CACHE_DIR, file_name)
        all_news_df.to_csv(path)

        # Group by 'symbol' and calculate mean and std of 'sentiment_score'
        news_sentiment_stats_df = all_news_df.groupby('symbol')['sentiment_score'].agg(['mean', 'std']).reset_index()
        file_name = "news_sentiment_stats_df.csv"
        path = os.path.join(CACHE_DIR, file_name)
        news_sentiment_stats_df.to_csv(path)

        return news_sentiment_stats_df
