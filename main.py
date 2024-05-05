from utils.file_utils import *
from loaders.FmpStockListLoader import FmpStockListLoader
from loaders.FmpAnalystRatingsLoader import FmpAnalystRatingsLoader
from loaders.FmpStockNewsLoader import FmpStockNewsLoader
from loaders.FmpSocialSentimentLoader import FmpSocialSentimentLoader
from loaders.FmpDividendLoader import FmpDividendLoader
from loaders.FmpPriceLoader import FmpPriceLoader
from loaders.FmpMomentumLoader import FmpMomentumLoader
from loaders.FmpGrowthLoader import FmpGrowthLoader
from loaders.FmpQualityLoader import FmpQualityLoader
from utils.df_utils import *
from utils.file_utils import get_os_variable
from utils.log_utils import *
import schedule


# Advanced stock screener considering factors, dividend yield and analyst ratings


# Get API key from environment variables
FMP_API_KEY = get_os_variable('FMP_API_KEY')


def calculate_bo_score(df):
    df['bo_score'] = df['momentum_factor'] * MOMENTUM_WEIGHT + \
                     df['growth_factor'] * GROWTH_WEIGHT + \
                     df['quality_factor'] * QUALITY_WEIGHT + \
                     df['analyst_rating_score'] * ANALYST_RATINGS_WEIGHT + \
                     df['avg_dividend_yield'] * DIVIDEND_YIELD_WEIGHT + \
                     df['news_sentiment_score'] * NEWS_SENTIMENT_WEIGHT

    # Sort by score
    df.sort_values(by=['bo_score'], ascending=[False], inplace=True)
    return df


def run():
    # Load stock list
    stock_list_loader = FmpStockListLoader(FMP_API_KEY)
    stock_list_df = stock_list_loader.fetch_list(
        exchange_list=EXCHANGE_LIST,
        min_market_cap=MIN_MARKET_CAP,
        min_price=MIN_PRICE,
        max_beta=MAX_BETA,
        min_volume=MIN_VOLUME,
        country=COUNTRY,
        stock_list_limit=STOCK_LIST_LIMIT
    )
    symbol_list = stock_list_df['symbol'].unique()

    # Load price info
    price_loader = FmpPriceLoader(FMP_API_KEY)
    prices_dict = price_loader.fetch(symbol_list)

    # Get all symbols that have prices
    symbol_list = list(prices_dict.keys())

    # Load Quality factor
    quality_loader = FmpQualityLoader(FMP_API_KEY)
    quality_df = quality_loader.fetch(symbol_list)

    # Filter out stocks based on min score
    quality_df = quality_df[quality_df['quality_factor'] >= MIN_QUALITY_FACTOR]
    symbol_list = quality_df['symbol'].unique()
    
    # Load Growth factor
    growth_loader = FmpGrowthLoader(FMP_API_KEY)
    growth_df = growth_loader.fetch(symbol_list)

    # Filter out stocks based on min score
    growth_df = growth_df[growth_df['growth_factor'] >= MIN_GROWTH_FACTOR]
    symbol_list = growth_df['symbol'].unique()
    
    # Load Momentum factor
    momentum_loader = FmpMomentumLoader(FMP_API_KEY)
    momentum_df = momentum_loader.fetch(symbol_list, prices_dict)

    # Filter out stocks based on min score
    momentum_df = momentum_df[momentum_df['momentum_factor'] >= MIN_MOMENTUM_FACTOR]
    symbol_list = momentum_df['symbol'].unique()

    # Load analyst ratings
    analyst_ratings_loader = FmpAnalystRatingsLoader(FMP_API_KEY)
    analyst_grades_df = analyst_ratings_loader.fetch(symbol_list)

    # Filter out stocks based on min score
    analyst_grades_df = analyst_grades_df[analyst_grades_df['analyst_rating_score'] >= MIN_ANALYST_RATINGS_SCORE]
    symbol_list = analyst_grades_df['symbol'].unique()

    # Load dividend info
    dividend_loader = FmpDividendLoader(FMP_API_KEY)
    dividend_stats_df = dividend_loader.fetch(symbol_list, prices_dict)

    # Load social sentiment
    #social_sentiment_loader = FmpSocialSentimentLoader(FMP_API_KEY)
    #social_sentiment_stats_df = social_sentiment_loader.fetch(symbol_list)
    social_sentiment_stats_df = pd.DataFrame({'symbol': [''], 'social_sentiment_score': [0] })

    # Load stock news
    stock_news_loader = FmpStockNewsLoader(FMP_API_KEY)
    stock_news_df = stock_news_loader.fetch(symbol_list, NEWS_ARTICLE_LIMIT)

    # Merge all dataframes by symbol
    df_list = [quality_df,
        growth_df,
        momentum_df,
        analyst_grades_df,
        dividend_stats_df,
        stock_news_df]
    merged_df = merge_dataframes(symbol_list, df_list)

    # Normalize score values
    norm_df = normalize_dataframe(merged_df)

    # Calculate B/O score (no, not 'Body Odor' ;)
    bo_score_df = calculate_bo_score(norm_df)

    # Round values
    bo_score_df = round_dataframe_columns(bo_score_df, precision=ROUND_PRECISION)

    # Store results
    file_name = "bo_score_df.csv"
    path = os.path.join(RESULTS_DIR, file_name)
    bo_score_df.to_csv(path)

    logi('All done')


def perform_cleanup():
    # Cleanup log file to avoid excessive growth
    delete_file(CACHE_DIR, LOG_FILE_NAME)


def schedule_events():
    schedule.every().monday.at('01:00').do(run)

    schedule.every().sunday.at('01:00').do(perform_cleanup)


if __name__ == "__main__":
    create_output_directories()
    setup_logger(LOG_FILE_NAME)

    # Run this thing!
    run()

    """
    #  Schedule events
    schedule_events()

    #  Check schedule
    while True:
        schedule.run_pending()
        time.sleep(10)  # Check time every x seconds
    """

