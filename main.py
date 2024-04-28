from utils.file_utils import *
from loaders.fmp_stock_list_loader import FmpStockListLoader
from loaders.fmp_analyst_ratings_loader import FmpAnalystRatingsLoader
from loaders.fmp_stock_news_loader import FmpStockNewsLoader
from loaders.fmp_social_sentiment_loader import FmpSocialSentimentLoader
from loaders.fmp_dividend_loader import FmpDividendLoader
from loaders.fmp_price_loader import FmpPriceLoader
from loaders.fmp_momentum_loader import FmpMomentumLoader
from loaders.fmp_growth_loader import FmpGrowthLoader
from loaders.fmp_quality_loader import FmpQualityLoader
import pandas as pd
from utils.df_utils import normalize_dataframe
from config import *
from utils.log_utils import *

# Advanced stock screener considering factors, dividend yield and analyst ratings


# Get API key from environment variables
if 'FMP_API_KEY' not in os.environ:
    print('FMP_API_KEY not set - exiting')
    exit(0)
FMP_API_KEY = os.environ['FMP_API_KEY']


def merge_dataframes(symbol_list, df_list):
    # Initialize the merged dataframe with the symbol list to ensure all symbols are included
    merged_df = pd.DataFrame(symbol_list, columns=['symbol'])

    # Merge each dataframe one by one
    for df in df_list:
        # Ensure that the merging dataframe has the 'symbol' column
        if 'symbol' in df.columns:
            merged_df = pd.merge(merged_df, df, on='symbol', how='left')
        else:
            print("Warning: DataFrame missing 'symbol' column, skipping...")

    return merged_df


def calculate_bo_score(df):
    df['bo_score'] = df['momentum_factor'] * MOMENTUM_WEIGHT + \
                     df['growth_factor'] * GROWTH_WEIGHT + \
                     df['quality_factor'] * QUALITY_WEIGHT + \
                     df['analyst_rating_score'] * ANALYST_RATINGS_WEIGHT + \
                     df['avg_dividend_yield'] * DIVIDEND_YIELD_WEIGHT + \
                     df['social_sentiment_score'] * SOCIAL_SENTIMENT_WEIGHT + \
                     df['news_sentiment_score'] * NEWS_SENTIMENT_WEIGHT


def run():
    # Load stock list
    stock_list_loader = FmpStockListLoader(FMP_API_KEY)
    securities_df = stock_list_loader.fetch()

    # todo
    securities_df = securities_df.head(30)
    symbol_list = securities_df['symbol'].unique()

    # Load price info
    price_loader = FmpPriceLoader(FMP_API_KEY)
    prices_dict = price_loader.fetch(symbol_list)

    # Get all symbols that have prices
    symbol_list = list(prices_dict.keys())

    # Load Quality factor
    quality_loader = FmpQualityLoader(FMP_API_KEY)
    quality_df = quality_loader.fetch(symbol_list)
    
    # Load Growth factor
    growth_loader = FmpGrowthLoader(FMP_API_KEY)
    growth_df = growth_loader.fetch(symbol_list)
    
    # Load Momentum factor
    momentum_loader = FmpMomentumLoader(FMP_API_KEY)
    momentum_df = momentum_loader.fetch(symbol_list, prices_dict)

    # Filter symbols that have momentum
    symbol_list = momentum_df['symbol'].unique()

    # Load analyst ratings
    analyst_ratings_loader = FmpAnalystRatingsLoader(FMP_API_KEY)
    analyst_grades_df = analyst_ratings_loader.fetch(symbol_list)

    # Filter symbols that have minimum analyst ratings score
    symbol_list = analyst_grades_df['symbol'].unique()

    # Load dividend info
    dividend_loader = FmpDividendLoader(FMP_API_KEY)
    dividend_stats_df = dividend_loader.fetch(symbol_list, prices_dict)

    # Load social sentiment
    social_sentiment_loader = FmpSocialSentimentLoader(FMP_API_KEY)
    social_sentiment_stats_df = social_sentiment_loader.fetch(symbol_list)

    # Load stock news
    stock_news_loader = FmpStockNewsLoader(FMP_API_KEY)
    stock_news_df = stock_news_loader.fetch(symbol_list)

    # Merge all dataframes by symbol
    df_list = [quality_df,
        growth_df,
        momentum_df,
        analyst_grades_df,
        dividend_stats_df,
        social_sentiment_stats_df,
        stock_news_df]
    merged_df = merge_dataframes(df_list)

    # Normalize score values
    norm_df = normalize_dataframe(merged_df)

    # Calculate B/O score (no, not 'Body Odor';)
    bo_score_df = calculate_bo_score(norm_df)

    # Store results
    file_name = "bo_score_df.csv"
    path = os.path.join(RESULTS_DIR, file_name)
    bo_score_df.to_csv(path)

    logi('All done')


if __name__ == "__main__":
    create_output_directories()

    # Run this thing!
    run()


