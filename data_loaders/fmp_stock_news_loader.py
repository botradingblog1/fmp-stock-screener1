import pandas as pd
from config import *
from utils.fmp_client import FmpClient
from utils.log_utils import *
from utils.file_utils import *
from utils.df_utils import cap_outliers
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

    def fetch_all_full_text(self, news_df):
        news_df['full_text'] = news_df.apply(self.fetch_full_article_text, axis=1)
        return news_df

    def detect_news_sentiment(self, row):
        news_text = f"{row['title']} {row['text']}"
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

    def calculate_news_sentiment_score(self, news_df):
        if news_df is None or len(news_df) == 0:
            return 0

        avg_sentiment = news_df['news_sentiment'].mean()
        article_count = len(news_df)

        news_sentiment_score = avg_sentiment * article_count

        return news_sentiment_score

    def fetch(self, symbol_list, news_article_limit):
        #  Iterate through symbols
        results_df = pd.DataFrame({})
        i = 1
        for symbol in symbol_list:
            logd(f"Loading stock news for {symbol}... ({i}/{len(symbol_list)})")

            # Fetch news
            news_df = self.fmp_client.get_stock_news(symbol, news_article_limit)
            if news_df is None or len(news_df) == 0:
                logw(f"No news for {symbol}")
                continue

            # Filter - only keep news from last 30 days
            start_date = datetime.today() - timedelta(days=30)
            news_df = news_df[news_df['publishedDate'] >= start_date]

            if len(news_df) == 0:
                logw(f"No news stories in the last month for {symbol}")
                news_sentiment_score = 0
            else:

                # Filter non-english news articles
                news_df = self.filter_non_english_news_items(news_df)

                # Fetch full news articles
                #news_df = self.fetch_all_full_text(news_df)

                # Detect news sentiment
                news_df['news_sentiment'] = news_df.apply(self.detect_news_sentiment, axis=1)
                store_csv(CACHE_DIR, f"{symbol}_news.csv", news_df)

                # Calculate score
                news_sentiment_score = self.calculate_news_sentiment_score(news_df)

            row = pd.DataFrame({'symbol': [symbol], 'news_sentiment_score': [news_sentiment_score]})
            results_df = pd.concat([results_df, row], axis=0, ignore_index=True)

            i += 1
            # Throttle for API limit
            time.sleep(API_REQUEST_DELAY)

        # Cap values
        results_df = cap_outliers(results_df, 'news_sentiment_score')

        return results_df
