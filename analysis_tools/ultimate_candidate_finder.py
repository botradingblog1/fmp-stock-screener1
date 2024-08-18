from utils.file_utils import *
from data_loaders.fmp_stock_list_loader import FmpStockListLoader
from data_loaders.fmp_analyst_ratings_loader import FmpAnalystRatingsLoader
from data_loaders.fmp_stock_news_loader import FmpStockNewsLoader
from data_loaders.fmp_social_sentiment_loader import FmpSocialSentimentLoader
from data_loaders.fmp_dividend_loader import FmpDividendLoader
from data_loaders.fmp_price_loader import FmpPriceLoader
from data_loaders.fmp_momentum_loader import FmpMomentumLoader
from data_loaders.fmp_growth_loader import FmpGrowthLoader
from data_loaders.fmp_quality_loader import FmpQualityLoader
from utils.df_utils import *
from utils.log_utils import *


class UltimateCandidateFinder:
    def __init__(self, fmp_api_key):
        self.fmp_api_key = fmp_api_key

    def calculate_ultimate_score(self, df, profile):
        df['ultimate_score'] = df['momentum_factor'] * profile['MOMENTUM_WEIGHT'] + \
                               df['growth_factor'] * profile['GROWTH_WEIGHT'] + \
                               df['analyst_rating_score'] * profile['ANALYST_RATINGS_WEIGHT'] + \
                               df['news_sentiment_score'] * profile['NEWS_SENTIMENT_WEIGHT']

        # Sort results by score
        df.sort_values(by=['ultimate_score'], ascending=[False], inplace=True)
        return df

    def find_candidates(self):
        # Load stock list
        stock_list_loader = FmpStockListLoader(self.fmp_api_key)
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
        price_loader = FmpPriceLoader(self.fmp_api_key)
        prices_dict = price_loader.fetch(symbol_list)

        # Get all symbols that have prices
        symbol_list = list(prices_dict.keys())

        # Load Quality factor
        #quality_loader = FmpQualityLoader(FMP_API_KEY)
        #quality_df = quality_loader.fetch(symbol_list)

        # Filter out stocks based on min score
        #quality_df = quality_df[quality_df['quality_factor'] >= MIN_QUALITY_FACTOR]
        #symbol_list = quality_df['symbol'].unique()

        # Load Growth factor
        growth_loader = FmpGrowthLoader(self.fmp_api_key)
        growth_df = growth_loader.fetch(symbol_list)

        # Filter out stocks based on min score
        growth_df = growth_df[growth_df['growth_factor'] >= MIN_GROWTH_FACTOR]
        symbol_list = growth_df['symbol'].unique()

        # Load Momentum factor
        momentum_loader = FmpMomentumLoader(self.fmp_api_key)
        momentum_df = momentum_loader.fetch(symbol_list, prices_dict)

        # Filter out stocks based on min score
        momentum_df = momentum_df[momentum_df['momentum_factor'] >= MIN_MOMENTUM_FACTOR]
        symbol_list = momentum_df['symbol'].unique()

        # Load analyst ratings
        analyst_ratings_loader = FmpAnalystRatingsLoader(self.fmp_api_key)
        analyst_grades_df = analyst_ratings_loader.fetch(symbol_list)

        # Filter out stocks based on min score
        analyst_grades_df = analyst_grades_df[
            analyst_grades_df['analyst_rating_score'] >= MIN_ANALYST_RATINGS_SCORE]
        symbol_list = analyst_grades_df['symbol'].unique()

        # Load stock news
        stock_news_loader = FmpStockNewsLoader(self.fmp_api_key)
        stock_news_df = stock_news_loader.fetch(symbol_list, NEWS_ARTICLE_LIMIT)

        # Merge all dataframes by symbol
        df_list = [growth_df,
                   momentum_df,
                   analyst_grades_df,
                   stock_news_df]
        merged_df = merge_dataframes(symbol_list, df_list)

        # Normalize score values
        norm_df = normalize_dataframe(merged_df)

        # Calculate B/O score
        ultimate_score_df = self.calculate_ultimate_score(norm_df, PROFILE)

        # Round values
        ultimate_score_df = round_dataframe_columns(ultimate_score_df, precision=ROUND_PRECISION)

        # Get top 100
        ultimate_score_df = ultimate_score_df.head(100)

        # Store results
        file_name = f"ultimate_screener_results_{PROFILE_NAME}.csv"
        path = os.path.join(RESULTS_DIR, file_name)
        ultimate_score_df.to_csv(path)

        logi(f"Ultimate screener candidates saved to {path}")
        