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
from utils.df_utils import *
from config import *
from utils.log_utils import *


# Advanced stock screener considering factors, dividend yield and analyst ratings


# Get API key from environment variables
if 'FMP_API_KEY' not in os.environ:
    print('FMP_API_KEY not set - exiting')
    exit(0)
FMP_API_KEY = os.environ['FMP_API_KEY']


def calculate_bo_score(df):
    df['bo_score'] = df['momentum_factor'] * MOMENTUM_WEIGHT + \
                     df['growth_factor'] * GROWTH_WEIGHT + \
                     df['quality_factor'] * QUALITY_WEIGHT + \
                     df['analyst_rating_score'] * ANALYST_RATINGS_WEIGHT + \
                     df['avg_dividend_yield'] * DIVIDEND_YIELD_WEIGHT + \
                     df['social_sentiment_score'] * SOCIAL_SENTIMENT_WEIGHT + \
                     df['news_sentiment_score'] * NEWS_SENTIMENT_WEIGHT

    # Sort by score
    df.sort_values(by=['bo_score'], ascending=[False], inplace=True)
    return df


def run():
    # Load stock list
    stock_list_loader = FmpStockListLoader(FMP_API_KEY)
    securities_df = stock_list_loader.fetch()

    # Get all price info so we can filter by min price
    price_loader = FmpPriceLoader(FMP_API_KEY)
    all_prices_df = price_loader.fetch_all()
    all_prices_df = all_prices_df[all_prices_df['lastSalePrice'] >= MIN_PRICE]

    # Inner join the dataframes
    merged_df = merge_dataframes_how([all_prices_df, securities_df], how='inner')
    symbol_list = merged_df['symbol'].unique()

    # Load price info
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

    # Filter symbols that have analyst ratings score
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
    merged_df = merge_dataframes(symbol_list, df_list)

    # Normalize score values
    norm_df = normalize_dataframe(merged_df)

    # Calculate B/O score (no, not 'Body Odor' ;)
    bo_score_df = calculate_bo_score(norm_df)

    # Round values
    bo_score_df = round_dataframe_columns(bo_score_df, precision=ROUND_PRECISION)

    # Only keep top items - optional
    # bo_score_df = bo_score_df.head(500)

    # Store results
    file_name = "bo_score_df.csv"
    path = os.path.join(RESULTS_DIR, file_name)
    bo_score_df.to_csv(path)

    logi('All done')


if __name__ == "__main__":
    create_output_directories()

    # Run this thing!
    run()


