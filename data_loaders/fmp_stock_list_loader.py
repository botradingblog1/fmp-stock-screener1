import pandas as pd
from config import *
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
from utils.log_utils import *
from utils.file_utils import *


class FmpStockListLoader:
    def __init__(self, fmp_api_key):
        self.fmp_client = FmpDataLoader(fmp_api_key)

    def fetch_list(self, exchange_list, min_market_cap, min_price, max_beta, min_volume, country, stock_list_limit):
        logi(f"Fetching stock list")
        stock_list_df = self.fmp_client.fetch_stock_screener_results(exchange_list=exchange_list,
                                                                     market_cap_more_than=min_market_cap,
                                                                     price_more_than=min_price,
                                                                     beta_lower_than=max_beta,
                                                                     volume_more_than=min_volume,
                                                                     is_actively_trading=True,
                                                                     is_fund=False,
                                                                     is_etf=False,
                                                                     country=country,
                                                                     limit=stock_list_limit)

        if stock_list_df is None or len(stock_list_df) == 0:
            logw("No stocks returned from FMP")
            return None

        # Filter out stocks from other exchanges - not sure why they are being returned...
        stock_list_df = stock_list_df[~stock_list_df['symbol'].str.contains(r'\.\w{1,4}$')]

        # Cache locally
        store_csv(CACHE_DIR, "stock_list.csv", stock_list_df)
        logi(f"Stock list cached at {os.path.join(CACHE_DIR, 'stock_list.csv')}")
        logi(f"Stock list returned {len(stock_list_df)} stocks")
        return stock_list_df

