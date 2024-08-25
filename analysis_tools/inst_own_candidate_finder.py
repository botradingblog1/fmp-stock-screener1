from data_loaders.fmp_stock_list_loader import FmpStockListLoader
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
from screeners.momentum_screener1 import MomentumScreener1
from screeners.growth_screener1 import GrowthScreener1
from screeners.fifty_two_week_low_screener import FiftyTwoWeekLowScreener
from data_loaders.fmp_inst_own_data_loader import FmpInstOwnDataLoader
from data_loaders.fmp_growth_loader1 import FmpGrowthLoader1
from utils.df_utils import *
from utils.log_utils import *
from utils.file_utils import *
from datetime import datetime, timedelta
from config import *


# Configuration
INST_OWN_CANDIDATES_DIR = "C:\\dev\\trading\\data\\inst_own\\candidates"
INST_OWN_CANDIDATES_FILE_NAME = "inst_own_candidates.csv"

# Momentum, total invested Δ, investor holdings Δ, last quarter revenue growth Δ, last quarter earnings growth Δ
INST_OWN_SCORE_WEIGHTS = [0.4, 0.2, 0.1, 0.2, 0.1]


class InstOwnCandidateFinder:
    def __init__(self, fmp_api_key: str):
        self.stock_list_loader = FmpStockListLoader(fmp_api_key)
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)
        self.growth_loader = FmpGrowthLoader1(fmp_api_key)
        self.fmp_inst_own_loader = FmpInstOwnDataLoader(fmp_api_key)
        self.momentum_screener = MomentumScreener1()
        self.undervalued_screener = UndervaluedScreener2()
        self.growth_screener = GrowthScreener1()

    def find_candidates(self):
        # Clean up previous candidates file
        delete_file(INST_OWN_CANDIDATES_DIR, INST_OWN_CANDIDATES_FILE_NAME)

        # Load stock list
        stock_list_df = self.stock_list_loader.fetch_list(
            exchange_list=EXCHANGE_LIST,
            min_market_cap=MIN_MARKET_CAP,
            min_price=MIN_PRICE,
            max_beta=MAX_BETA,
            min_volume=MIN_VOLUME,
            country=COUNTRY,
            stock_list_limit=STOCK_LIST_LIMIT
        )
        symbol_list = stock_list_df['symbol'].unique()

        # Fetch price history
        start_date = datetime.today() - timedelta(days=DAILY_DATA_FETCH_PERIODS)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date = datetime.today()
        end_date_str = end_date.strftime("%Y-%m-%d")
        prices_dict = self.fmp_data_loader.fetch_multiple_daily_prices_by_date(symbol_list, start_date_str, end_date_str,
                                                                               cache_data=True, cache_dir=CACHE_DIR)

        # Get all symbols that have prices
        symbol_list = list(prices_dict.keys())

        """ NOTE: Instead of biggest winners, it may be better to find undervalued stocks 
        # Momentum screener
        momentum_df = self.momentum_screener.run(symbol_list, prices_dict)

        # Keep the top x percent
        stocks_to_keep = int(len(momentum_df) * 0.66)
        momentum_df = momentum_df.head(stocks_to_keep)

        logi(f"Momentum screener returned {len(momentum_df)} items.")
        symbol_list = momentum_df['symbol'].unique()
        """
        # Undervalued screener
        undervalued_df = self.undervalued_screener.run(symbol_list, prices_dict, MIN_PRICE_DROP_PERCENT)
        logi(f"Undervalued screener returned {len(undervalued_df)} items.")
        symbol_list = undervalued_df['symbol'].unique()
        
        # Undervalued screener
        #undervalued_df = self.undervalued_screener.run(symbol_list, prices_dict)
        #logi(f"Undervalued screener returned {len(undervalued_df)} items.")
        #symbol_list = undervalued_df['symbol'].unique()

        # Get institutional ownership
        inst_own_results_df = self.fmp_inst_own_loader.run(symbol_list)

        # Merge dataframes
        merged_df = pd.merge(undervalued_df, inst_own_results_df, on='symbol', how='inner')

        # Load growth data
        growth_data_dict = self.growth_loader.fetch(symbol_list)

        # Check min earnings/revenue growth and growth acceleration
        growth_df = self.growth_screener.run(growth_data_dict)
        if growth_df is None or len(growth_df) == 0:
            logi("Growth screener returned no results")
            return
        logi(f"Growth screener returned {len(growth_df)} items.")

        # Merge growth
        merged_df = pd.merge(merged_df, growth_df, on='symbol', how='inner')

        """
        # Normalize columns
        column_list = ['momentum_change',
                        'total_invested_change',
                        'investors_holding_change',
                        'last_quarter_revenue_growth',
                        'last_quarter_earnings_growth']
        #merged_norm_df = normalize_columns(merged_df.copy(), column_list)
        """

        # Sort by total institutional ownership invested
        merged_df.sort_values(by="total_invested_change", ascending=False, inplace=True)

        # Calculate weighted score
        #merged_df['weighted_score'] = merged_norm_df[column_list].mul(INST_OWN_SCORE_WEIGHTS).sum(axis=1)

        # Store results
        os.makedirs(INST_OWN_CANDIDATES_DIR, exist_ok=True)
        path = os.path.join(INST_OWN_CANDIDATES_DIR, INST_OWN_CANDIDATES_FILE_NAME)
        merged_df.to_csv(path)

        logi(f"Institutional ownership candidates saved to {path}")
