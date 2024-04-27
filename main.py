from utils.file_utils import *
from loaders.fmp_stock_list_loader import FmpStockListLoader
from loaders.fmp_analyst_ratings_loader import FmpAnalystRatingsLoader
from loaders.fmp_stock_news_loader import FmpStockNewsLoader
from loaders.fmp_social_sentiment_loader import FmpSocialSentimentLoader
from loaders.fmp_dividend_loader import FmpDividendLoader

# Advanced stock screener considering factors, dividend yield and analyst ratings


# Get API key from environment variables
if 'FMP_API_KEY' not in os.environ:
    print('FMP_API_KEY not set - exiting')
    exit(0)
FMP_API_KEY = os.environ['FMP_API_KEY']


def main_loop():
    #while True: todo
    # Load stock list
    stock_list_loader = FmpStockListLoader(FMP_API_KEY)
    securities_df = stock_list_loader.fetch()

    # Filter for stocks
    stocks_df = securities_df[securities_df['type'] == 'stock']

    # Filter by exchange
    mask = (stocks_df['exchangeShortName'] == "NASDAQ") | (stocks_df['exchangeShortName'] == "NYSE") | (stocks_df['exchangeShortName'] == "AMEX")
    stocks_df = stocks_df[mask]
    # todo
    stocks_df = stocks_df.head(5)
    symbol_list = stocks_df['symbol'].unique()

    # Load dividend info
    dividend_loader = FmpDividendLoader(FMP_API_KEY)
    dividend_stats_df = dividend_loader.fetch(symbol_list)
    print(dividend_stats_df)

    """
    # Load social sentiment
    social_sentiment_loader = FmpSocialSentimentLoader(FMP_API_KEY)
    social_sentiment_stats_df = social_sentiment_loader.fetch(symbol_list)
    print(social_sentiment_stats_df)
    
    
    # Load stock news
    stock_news_loader = FmpStockNewsLoader(FMP_API_KEY)
    stock_news_df = stock_news_loader.fetch(symbol_list)
    print(stock_news_df)
    
    # Load analyst ratings
    fmp_analyst_ratings_loader = FmpAnalystRatingsLoader(FMP_API_KEY)
    analyst_grades_df = fmp_analyst_ratings_loader.fetch(symbol_list)
    print(analyst_grades_df)
    """

if __name__ == "__main__":
    create_output_directories()

    # Run endless loop
    main_loop()


