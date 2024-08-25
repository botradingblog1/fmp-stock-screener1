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
    Deep Discount, Growth potential looks at 52-week low, current revenue earnings growth, 
    future earnings estimates and analyst ratings. Calculates weighted score
"""

# Configuration
DEEP_DISCOUNT_GROWTH_CANDIDATES_DIR = "C:\\dev\\trading\\data\\deep_discount_growth\\candidates"
DEEP_DISCOUNT_GROWTH_CANDIDATES_FILE_NAME = "deep_discount_growth_candidates.csv"

MIN_PRICE_DROP_PERCENT = 0.5
MIN_CURRENT_QUARTERLY_REVENUE_GROWTH = 0.1
MIN_CURRENT_QUARTERLY_EARNINGS_GROWTH = 0.1
MIN_AVG_EARNINGS_ESTIMATE_PERCENT = 0.1
MIN_NUM_EARNINGS_ANALYSTS = 3

# Weight configuration for each component
WEIGHTS = {
    'price_drop_percent': 0.3,
    'avg_eps_growth_percent': 0.3,
    'last_quarter_revenue_growth': 0.1,
    'last_quarter_earnings_growth': 0.1,
    'analyst_ratings_score': 0.2
}


class DeepDiscountGrowthCandidateFinder:
    def __init__(self, fmp_api_key: str):
        self.market_symbol_loader = MarketSymbolLoader()
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)
        self.growth_loader = FmpGrowthLoader1(fmp_api_key)
        self.fmp_analyst_ratings_loader = FmpAnalystRatingsLoader(fmp_api_key)
        self.momentum_screener = MomentumScreener1()
        self.fifty_two_week_low_screener = FiftyTwoWeekLowScreener()
        self.earnings_estimate_screener = EarningsEstimateScreener1()
        self.growth_screener = GrowthScreener1()

    def normalize_series(self, series):
        """Normalize a pandas series to a 0-1 range."""
        return (series - series.min()) / (series.max() - series.min())

    def find_candidates(self):
        logi("Finding undervalued blue chips")
        # Clean up previous candidates file
        delete_file(DEEP_DISCOUNT_GROWTH_CANDIDATES_DIR, DEEP_DISCOUNT_GROWTH_CANDIDATES_FILE_NAME)

        # Load symbol list
        symbols_df = self.market_symbol_loader.fetch_russell1000_symbols(cache_file=True)
        #symbols_df = self.market_symbol_loader.fetch_nasdaq100_symbols(cache_file=True)
        symbol_list = list(symbols_df['symbol'].unique())

        # Fetch price history
        start_date = datetime.today() - timedelta(days=DAILY_DATA_FETCH_PERIODS)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date = datetime.today()
        end_date_str = end_date.strftime("%Y-%m-%d")
        prices_dict = self.fmp_data_loader.fetch_multiple_daily_prices_by_date(symbol_list,
                                                                               start_date_str,
                                                                               end_date_str,
                                                                               cache_data=True,
                                                                               cache_dir=CACHE_DIR)
        symbol_list = list(prices_dict.keys())

        # 52-week low screener
        undervalued_df = self.fifty_two_week_low_screener.run(symbol_list,
                                                              prices_dict,
                                                              min_price_drop_percent=MIN_PRICE_DROP_PERCENT)
        logi(f"Fifty-two week low screener returned {len(undervalued_df)} items.")
        symbol_list = undervalued_df['symbol'].unique()

        # Load growth data
        growth_data_dict = self.growth_loader.fetch(symbol_list)

        # Check min earnings/revenue growth and growth acceleration
        growth_df = self.growth_screener.run(growth_data_dict,
                                             MIN_CURRENT_QUARTERLY_EARNINGS_GROWTH,
                                             MIN_CURRENT_QUARTERLY_REVENUE_GROWTH)
        if growth_df is None or len(growth_df) == 0:
            logi("Growth screener returned no results")
            return
        logi(f"Growth screener returned {len(growth_df)} items.")

        # Merge growth
        merged_df = pd.merge(undervalued_df, growth_df, on='symbol', how='inner')
        symbol_list = merged_df['symbol'].unique()

        # Fetch future earnings estimates
        earnings_estimate_data_dict = self.fmp_data_loader.fetch_multiple_analyst_earnings_estimates(symbol_list,
                                                                                                     period="quarter",
                                                                                                     cache_data=True,
                                                                                                     cache_dir=CACHE_DIR)

        # Screener for earnings estimates
        earnings_estimates_df = self.earnings_estimate_screener.run(earnings_estimate_data_dict,
                                                                    MIN_AVG_EARNINGS_ESTIMATE_PERCENT,
                                                                    MIN_NUM_EARNINGS_ANALYSTS)
        merged_df = pd.merge(merged_df, earnings_estimates_df, on='symbol', how='inner')
        symbol_list = merged_df['symbol'].unique()

        # Get analyst ratings
        analyst_ratings_df = self.fmp_analyst_ratings_loader.fetch(symbol_list)

        # Merge dataframes
        merged_df = pd.merge(merged_df, analyst_ratings_df, on='symbol', how='inner')

        if merged_df.empty:
            logi(f"Candidates file is empty")
            return

        # Normalize the different metrics before calculating the score
        for column in ['price_drop_percent', 'avg_eps_growth_percent', 'last_quarter_revenue_growth',
                       'last_quarter_earnings_growth', 'analyst_rating_score']:
            merged_df[f'norm_{column}'] = self.normalize_series(merged_df[column])

        # Calculate weighted score
        merged_df['weighted_score'] = (
            WEIGHTS['price_drop_percent'] * merged_df['norm_price_drop_percent'] +
            WEIGHTS['avg_eps_growth_percent'] * merged_df['norm_avg_eps_growth_percent'] +
            WEIGHTS['last_quarter_revenue_growth'] * merged_df['norm_last_quarter_revenue_growth'] +
            WEIGHTS['last_quarter_earnings_growth'] * merged_df['norm_last_quarter_earnings_growth'] +
            WEIGHTS['analyst_rating_score'] * merged_df['norm_analyst_rating_score']
        )

        # Sort by weighted score in descending order
        merged_df = merged_df.sort_values(by='weighted_score', ascending=False)

        # Store results
        os.makedirs(DEEP_DISCOUNT_GROWTH_CANDIDATES_DIR, exist_ok=True)
        path = os.path.join(DEEP_DISCOUNT_GROWTH_CANDIDATES_DIR, DEEP_DISCOUNT_GROWTH_CANDIDATES_FILE_NAME)
        merged_df.to_csv(path, index=False)

        logi(f"Blue chip candidates saved to {path}")