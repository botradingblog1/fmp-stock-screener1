from data_loaders.fmp_stock_list_loader import FmpStockListLoader
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
from botrading.data_loaders.market_symbol_loader import MarketSymbolLoader
from screeners.momentum_screener1 import MomentumScreener1
from screeners.growth_screener1 import GrowthScreener1
from screeners.earnings_estimate_screener1 import EarningsEstimateScreener1
from screeners.fifty_two_week_low_screener import FiftyTwoWeekLowScreener
from data_loaders.fmp_analyst_ratings_loader import FmpAnalystRatingsLoader
from data_loaders.fmp_growth_loader1 import FmpGrowthLoader1
from utils.df_utils import *
from utils.log_utils import *
from utils.file_utils import *
from datetime import datetime, timedelta

"""
    All purpose company profiel finder, current revenue earnings growth, 
    future earnings estimates and analyst ratings. Calculates weighted score
"""

# Configuration
CANDIDATES_DIR = "results"
CANDIDATES_FILE_NAME = "profile_info.csv"

MIN_CURRENT_QUARTERLY_REVENUE_GROWTH = 0.1
MIN_CURRENT_QUARTERLY_EARNINGS_GROWTH = 0.1
MIN_AVG_EARNINGS_ESTIMATE_PERCENT = 0.1
MIN_NUM_EARNINGS_ANALYSTS = 3

# Weight configuration for each component
WEIGHTS = {
    'avg_eps_growth_percent': 0.2,
    'last_quarter_revenue_growth': 0.1,
    'last_quarter_earnings_growth': 0.1,
    'analyst_rating_score': 0.2,
    'news_sentiment_score': 0.1
}


class ProfileBuilder:
    def __init__(self, fmp_api_key: str):
        self.market_symbol_loader = MarketSymbolLoader()
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)
        self.growth_loader = FmpGrowthLoader1(fmp_api_key)
        self.fmp_analyst_ratings_loader = FmpAnalystRatingsLoader(fmp_api_key)
        self.momentum_screener = MomentumScreener1()
        self.fifty_two_week_low_screener = FiftyTwoWeekLowScreener()
        self.earnings_estimate_screener = EarningsEstimateScreener1()
        self.growth_screener = GrowthScreener1()

    def build_profiles(self):
        logi("Building company profiles")
        symbol_list = ["NDAQ", "ABT", "CTVA", "COO", "ARMK", "CNH", "CSX", "GEN", "AXTA", "BALL", "BIIB", "ADM", "BK", "BKNG", "DHI"]

        # Clean up previous candidates file
        delete_file(CANDIDATES_DIR, CANDIDATES_FILE_NAME)

        # Fetch price history
        start_date = datetime.today() - timedelta(days=DAILY_DATA_FETCH_PERIODS)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date = datetime.today()
        end_date_str = end_date.strftime("%Y-%m-%d")

        # Load growth data
        growth_data_dict = self.growth_loader.fetch(symbol_list)

        # Check min earnings/revenue growth and growth acceleration
        growth_df = self.growth_screener.run(growth_data_dict,
                                             min_quarterly_earnings_growth=None,
                                             min_quarterly_revenue_growth=None)
        if growth_df is None or len(growth_df) == 0:
            logi("Growth screener returned no results")
            return
        logi(f"Growth screener returned {len(growth_df)} items.")

        # Merge growth
        #merged_df = pd.merge(undervalued_df, growth_df, on='symbol', how='inner')
        symbol_list = growth_df['symbol'].unique()

        # Fetch future earnings estimates
        earnings_estimate_data_dict = self.fmp_data_loader.fetch_multiple_analyst_earnings_estimates(symbol_list,
                                                                                                     period="quarter",
                                                                                                     limit=100)

        # Screener for earnings estimates
        earnings_estimates_df = self.earnings_estimate_screener.run(earnings_estimate_data_dict,
                                                                    min_avg_estimate_percent=None,
                                                                    min_num_analysts=None)
        merged_df = pd.merge(growth_df, earnings_estimates_df, on='symbol', how='inner')
        symbol_list = merged_df['symbol'].unique()

        # Get analyst ratings
        analyst_ratings_df = self.fmp_analyst_ratings_loader.fetch(symbol_list)
        merged_df = pd.merge(merged_df, analyst_ratings_df, on='symbol', how='inner')

        # Determine news sentiment
        #news_sentiment_df = self.fmp_stock_news_loader.fetch(symbol_list, news_article_limit=30)
        #merged_df = pd.merge(merged_df, news_sentiment_df, on='symbol', how='inner')

        if merged_df.empty:
            logi(f"Candidates file is empty")
            return

        # Normalize the different metrics before calculating the score
        for column in ['avg_eps_growth_percent', 'last_quarter_revenue_growth',
                       'last_quarter_earnings_growth', 'analyst_rating_score']:
            merged_df[f'norm_{column}'] = normalize_series(merged_df[column])
        # 'news_sentiment_score'

        # Calculate weighted score
        merged_df['weighted_score'] = (
            WEIGHTS['avg_eps_growth_percent'] * merged_df['norm_avg_eps_growth_percent'] +
            WEIGHTS['last_quarter_revenue_growth'] * merged_df['norm_last_quarter_revenue_growth'] +
            WEIGHTS['last_quarter_earnings_growth'] * merged_df['norm_last_quarter_earnings_growth'] +
            WEIGHTS['analyst_rating_score'] * merged_df['norm_analyst_rating_score']
        )
        #WEIGHTS['news_sentiment_score'] * merged_df['news_sentiment_score'] * 100

        # Drop all columns that start with 'norm_'
        columns_to_drop = [col for col in merged_df.columns if col.startswith('norm_')]
        merged_df.drop(columns=columns_to_drop, inplace=True)

        # Sort by weighted score in descending order
        merged_df = merged_df.sort_values(by='weighted_score', ascending=False)

        # Store results
        os.makedirs(CANDIDATES_DIR, exist_ok=True)
        path = os.path.join(CANDIDATES_DIR, CANDIDATES_FILE_NAME)
        merged_df.to_csv(path, index=False)

        logi(f"Profile candidates saved to {path}")
