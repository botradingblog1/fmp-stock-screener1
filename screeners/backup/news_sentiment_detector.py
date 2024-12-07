from langdetect import detect, LangDetectException
import re
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import json
from utils.file_utils import *
device = "cuda:0" if torch.cuda.is_available() else "cpu"

FINBERT_MAX_TOKENS = 512
tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert").to(device)
labels = ["positive", "negative", "neutral"]

os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"


class NewsSentimentDetector:
    def detect_english(self, text):
        try:
            return detect(text) == 'en'
        except LangDetectException:  # Handle exception if language detection fails
            return False

    def filter_non_english_news_items(self, news_df):
        # Filter out all news articles not in English language
        news_df['is_english'] = news_df['description'].apply(self.detect_english)
        news_df = news_df[news_df['is_english']]

        # Drop the 'is_english' column if it's no longer needed
        news_df.drop(columns=['is_english'], inplace=True)
        return news_df

    def clean_article_text(self, text):
        # Remove line breaks, tabs, and multiple whitespace characters
        text = re.sub(r'\s+', ' ', text)
        return text

    def detect_sentiment_for_article(self, row):
        news_text = f"{row['title']} {row['description']}"
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
            # Assuming labels are ordered as ["positive", "negative", "neutral"]
            if labels[sentiment_idx] == "positive":
                return probability  # Positive sentiment
            elif labels[sentiment_idx] == "negative":
                return -probability  # Negative sentiment
            else:
                return 0.0  # Neutral sentiment has no impact score
        else:
            return 0.0  # Return 0.0 for empty or None text

    def detect_news_sentiment(self, news_df):
        print(f"Detecting news sentiment...")
        news_df['news_sentiment'] = 0

        # Filter non-english news articles
        news_df = self.filter_non_english_news_items(news_df)
        if news_df is None or len(news_df) == 0:
            print(f"No English news articles found")
            return news_df

        # Detect news sentiment using FinBERT
        news_df['news_sentiment'] = news_df.apply(self.detect_sentiment_for_article, axis=1)

        return news_df


