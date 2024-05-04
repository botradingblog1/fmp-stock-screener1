import pandas as pd
from config import *
from utils.fmp_client import FmpClient
from utils.log_utils import *
from utils.file_utils import *


class FmpStockListLoader:
    def __init__(self, fmp_api_key):
        self.fmp_client = FmpClient(fmp_api_key)

    def fetch_list(self, exchange_list, min_market_cap, min_price, max_beta, min_volume, country, stock_list_limit):
        logi(f"Fetching stock list")
        stock_list_df = self.fmp_client.fetch_stock_screener_results(exchange_list=exchange_list,
                                                                     market_cap_more_than=min_market_cap,
                                                                     priceMoreThan=min_price,
                                                                     beta_lower_than=max_beta,
                                                                     volume_more_than=min_volume,
                                                                     country=country,
                                                                     limit=stock_list_limit)
        # Cache locally
        store_csv(CACHE_DIR, "stock_list.csv", stock_list_df)
        logi(f"Stock list cached at {os.path.join(CACHE_DIR, 'stock_list.csv')}")
        logi(f"Stock list returned {len(stock_list_df)} stocks")
        return stock_list_df

