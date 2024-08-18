from data_loaders.fmp_stock_list_loader import FmpStockListLoader
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
from botrading.data_loaders.market_symbol_loader import MarketSymbolLoader
from screeners.momentum_screener1 import MomentumScreener1
from screeners.growth_screener1 import GrowthScreener1
from screeners.undervalued_screener2 import UndervaluedScreener2
from data_loaders.fmp_analyst_ratings_loader import FmpAnalystRatingsLoader
from data_loaders.fmp_growth_loader1 import FmpGrowthLoader1
from utils.df_utils import *
from utils.log_utils import *
from utils.file_utils import *
from datetime import datetime, timedelta


# Configuration
BLUE_CHIP_BARGAIN_CANDIDATES_DIR = "C:\\dev\\trading\\data\\blue_chip_value\\candidates"
BLUE_CHIP_BARGAIN_CANDIDATES_FILE_NAME = "blue_chip_BARGAIN_candidates.csv"


class BlueChipBargainCandidateFinder:
    def __init__(self, fmp_api_key: str):
        self.market_symbol_loader = MarketSymbolLoader()
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)
        self.growth_loader = FmpGrowthLoader1(fmp_api_key)
        self.fmp_analyst_ratings_loader = FmpAnalystRatingsLoader(fmp_api_key)
        self.momentum_screener = MomentumScreener1()
        self.undervalued_screener = UndervaluedScreener2()
        self.growth_screener = GrowthScreener1()

    def find_candidates(self):
        logi("Finding undervalued blue chips")
        # Clean up previous candidates file
        delete_file(BLUE_CHIP_BARGAIN_CANDIDATES_DIR, BLUE_CHIP_BARGAIN_CANDIDATES_FILE_NAME)

        # Load Blue Chip stocks
        snp_500_symbols_df = self.market_symbol_loader.fetch_sp500_symbols(cache_file=True)
        nasdaq_100_symbols_df = self.market_symbol_loader.fetch_nasdaq100_symbols(cache_file=True)
        snp_500_symbol_list = list(snp_500_symbols_df['symbol'].unique())
        nasdaq_100_symbol_list = list(nasdaq_100_symbols_df['symbol'].unique())

        # Combine and eliminate dups
        symbol_list = list(set(snp_500_symbol_list + nasdaq_100_symbol_list))

        # Fetch price history
        start_date = datetime.today() - timedelta(days=DAILY_DATA_FETCH_PERIODS)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date = datetime.today()
        end_date_str = end_date.strftime("%Y-%m-%d")
        prices_dict = self.fmp_data_loader.fetch_multiple_daily_prices_by_date(symbol_list, start_date_str, end_date_str,
                                                                               cache_data=True, cache_dir=CACHE_DIR)
        symbol_list = list(prices_dict.keys())

        # Undervalued screener
        undervalued_df = self.undervalued_screener.run(symbol_list, prices_dict)
        logi(f"Undervalued screener returned {len(undervalued_df)} items.")
        symbol_list = undervalued_df['symbol'].unique()

        # Load growth data
        growth_data_dict = self.growth_loader.fetch(symbol_list)

        # Check min earnings/revenue growth and growth acceleration
        growth_df = self.growth_screener.run(growth_data_dict)
        if growth_df is None or len(growth_df) == 0:
            logi("Growth screener returned no results")
            return
        logi(f"Growth screener returned {len(growth_df)} items.")

        # Merge growth
        merged_df = pd.merge(undervalued_df, growth_df, on='symbol', how='inner')
        symbol_list = merged_df['symbol'].unique()

        # Get analyst ratings
        analyst_ratings_df = self.fmp_analyst_ratings_loader.fetch(symbol_list)

        # Merge dataframes
        merged_df = pd.merge(merged_df, analyst_ratings_df, on='symbol', how='inner')

        # Sort by 52-week price drop
        merged_df.sort_values(by="price_drop_percent", ascending=False, inplace=True)

        # Store results
        os.makedirs(BLUE_CHIP_BARGAIN_CANDIDATES_DIR, exist_ok=True)
        path = os.path.join(BLUE_CHIP_BARGAIN_CANDIDATES_DIR, BLUE_CHIP_BARGAIN_CANDIDATES_FILE_NAME)
        merged_df.to_csv(path)

        logi(f"Blue chip candidates saved to {path}")
