from config import *
from threading import Lock
from utils.log_utils import *
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
from utils.file_utils import store_csv


class UniverseSelector:
    """
    Singleton class for universe selection
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance  # Always return the instance

    def __init__(self, fmp_api_key: str=None):
        # Check if the instance already exists
        if not hasattr(self, 'initialized'):
            self.initialized = True
            self.is_running = False
            self.fmp_data_loader = FmpDataLoader(fmp_api_key)
            self.symbol_list = []
            self.stock_info_df = {}
            self.lock = Lock()

    def perform_selection(self):
        try:
            logi("Running universe selection....")

            # Load list of stocks
            stock_list_df = self.fmp_data_loader.fetch_stock_screener_results(
                exchange_list=EXCHANGE_LIST,
                price_more_than=PRICE_MORE_THAN,
                price_lower_than=PRICE_LESS_THAN,
                volume_more_than=VOLUME_MORE_THAN,
                market_cap_lower_than=MARKET_CAP_LOWER_THAN,
                is_etf=False,
                is_fund=False,
                is_actively_trading=True,
                limit=STOCK_SCREENER_LIMIT
            )
            if stock_list_df is None or len(stock_list_df) == 0:
                logw(f"Stock screener didn't return any data")
                exit(0)

            # Filter out stocks from other exchanges
            stock_list_df = stock_list_df[~stock_list_df['symbol'].str.contains(r'\.\w{1,4}$')]

            # Filter rows where 'industry' is in the preferred industries list
            #stock_list_df = stock_list_df[stock_list_df['industry'].isin(INDUSTRY_LIST)]
            # Store list of stocks
            store_csv(CACHE_DIR, 'stock_list_df.csv', stock_list_df)

            symbol_list = stock_list_df['symbol'].unique()
            with self.lock:
                self.symbol_list = symbol_list
                self.stock_info_df = stock_list_df

        except Exception as e:
            loge(f"Error running universe selection: {str(e)}")

    def stop(self):
        self.is_running = False
        logi("Trade log shut down")

    def get_symbol_list(self):
        with self.lock:
            return self.symbol_list

    def get_stock_info(self):
        with self.lock:
            return self.stock_info_df
