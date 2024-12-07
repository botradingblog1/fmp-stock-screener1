import re
import nltk
import gensim
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from botrading.data_loaders.tiingo_data_loader import TiingoDataLoader
from screeners.backup.news_sentiment_detector import NewsSentimentDetector
from utils.log_utils import *
from utils.file_utils import *
from datetime import timedelta, time


# Download necessary NLTK data
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')

# Tag list
earnings_tags = "record profits,boom,earnings growth,revenue growth,beat,miss,profit surge,net income increase,revenue shortfall"
market_expansion_tags = "emerging market,geographical expansion,global expansion,global presence,market penetration,expansion strategy,market entry,market opportunities,market opportunity"
product_launch_tags = "product launch,technology breakthrough,AI-powered technology,Next-gen technology,revolutionary product, disruptive technology, AI-driven solution"
legal_issues_tags = "legal battle,class action lawsuit,legal challenge,lawsuit,SEC investigation,antitrust investigation,regulatory hurdles"
analyst_rating_tags = "rating change,upgraded,downgraded,analyst upgrade,analyst downgrade,price target increase,price target cut,outperform rating"
merger_tags = "merger,announced merger,acquisition,partnership,alliance,joint venture,takeover bid"
dividend_buyback_tags = "dividend payout,dividend hike,dividend increase,special dividend,stock buyback,stock repurchase,share repurchase "
layoff_tags = "layoffs,layoff,workforce reduction,job cuts,restructuring,downsizing,office closure,plant closure"
fundraising_tags = "raise capital,fundraising,venture capital,equity financing,funding secured,"
consumer_behavior_tags = "consumer demand surge,surge in consumer demand,retail growth,e-commerce growth,popular among"

tag_list = [earnings_tags, market_expansion_tags, product_launch_tags, legal_issues_tags, analyst_rating_tags, merger_tags,
            dividend_buyback_tags, layoff_tags, fundraising_tags, consumer_behavior_tags]


RESULTS_DIR = "C:\\dev\\trading\\data\\news_catalysts"


class NewsCatalystFinder:
    def __init__(self, tiingo_api_key: str):
        self.data_loader = TiingoDataLoader(tiingo_api_key)
        self.news_sentiment_detector = NewsSentimentDetector()
        self.stop_words = set(stopwords.words('english'))
        self.lemmatizer = WordNetLemmatizer()

    # Preprocess function: clean, tokenize, remove stop words, and lemmatize
    def preprocess_text(self, text):
        # Remove special characters and lower the text
        text = re.sub(r'\W', ' ', text)
        text = text.lower()

        # Tokenize
        tokens = gensim.utils.simple_preprocess(text, deacc=True)  # Tokenizes and removes punctuations

        # Remove stop words and lemmatize
        tokens = [self.lemmatizer.lemmatize(token) for token in tokens if token not in self.stop_words]

        return tokens

    def fetch_news_articles(self, tags, limit=20):
        # Define start and end dates for that day and the day before
        start_date_str = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        end_date_str = (datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d')

        # Fetch news
        news_df = self.data_loader.fetch_news_articles_by_tags(tags,
                                                                start_date_str,
                                                                end_date_str,
                                                                news_article_limit=limit,
                                                                cache_data=False,
                                                                cache_dir=CACHE_DIR)
        if news_df is not None and len(news_df) > 0:
            news_df['content'] = news_df['title'] + news_df['description']

        return news_df

    def find_catalysts(self):
        # Fetch news by tags
        combined_news_df = pd.DataFrame()
        for tags in tag_list:
            logd(f"Fetching news for tags: {tags}...")
            # Fetch news articles
            news_df = self.fetch_news_articles(tags, limit=50)
            if news_df is None or len(news_df) == 0:
                continue

            # Detect news sentiment
            #news_df = self.news_sentiment_detector.detect_news_sentiment(news_df)

            # Convert the publishedDate to datetime
            #news_df['publishedDate'] = pd.to_datetime(news_df['publishedDate'], utc=True)

            # Define market close and open times in UTC
            market_close_time = time(21, 0)  # 4:00 PM EST = 9:00 PM UTC
            pre_market_open_time = time(8, 0)  # 4:00 AM EST = 8:00 AM UTC

            # Drop rows where publishedDate is NaT (invalid dates)
            news_df = news_df.dropna(subset=['publishedDate'])

            # Filter for articles published between market close and pre-market open
            news_df = news_df[news_df['publishedDate'].apply(
                lambda x: (x.time() >= market_close_time or x.time() < pre_market_open_time)
            )]

            # Filter out stories that have a weak sentiment
            news_sentiment_threshold = 0.6
            #news_df = news_df[(news_df['news_sentiment'] > news_sentiment_threshold) | (news_df['news_sentiment'] < -news_sentiment_threshold)]

            # Combine all news
            combined_news_df = pd.concat([combined_news_df, news_df], axis=0, ignore_index=True)
        # Store news analysis
        store_csv(RESULTS_DIR, "combined_news_tags.csv", combined_news_df)

        logi("News catalyst finder completed.")
