from utils.log_utils import *
from utils.fmp_client import *
from config import *
import pandas as pd


# Loads list of available stocks from FMP
class FmpStockListLoader:
    def __init__(self, fmp_api_key):
        self.fmp_api_key = fmp_api_key

    def fetch_stock_list(self):
        file_name = "fmp_stock_list.csv"
        path = os.path.join(CACHE_DIR, file_name)
        if os.path.exists(path):
            securities_df = pd.read_csv(path)
        else:
            # Load remotely
            client = FmpClient(self.fmp_api_key)
            securities_df = client.fetch_tradable_list()
            if securities_df is None or len(securities_df) == 0:
                loge("No securities found - exiting")
                exit(0)

            # Filter for stocks
            securities_df = securities_df[securities_df['type'] == 'stock']

            # Filter by exchange
            mask = (securities_df['exchangeShortName'] == "NASDAQ") | (securities_df['exchangeShortName'] == "NYSE") | (
                    securities_df['exchangeShortName'] == "AMEX")
            securities_df = securities_df[mask]

            # Cache locally
            securities_df.to_csv(path)
        return securities_df

    def fetch(self):
        # Fetch list of stocks
        securities_df = self.fetch_stock_list()
        return securities_df
