import pandas as pd
from config import *
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
from data_loaders.fmp_stock_list_loader import FmpStockListLoader
from utils.log_utils import *
from utils.file_utils import *
from datetime import datetime, timedelta
import os

"""
Finds stocks with the highest average monthly returns

"""

# Configuration
HIGHEST_RETURN_CANDIDATES_DIR = "C:\\dev\\trading\\data\\highest_returns\\candidates"
HIGHEST_RETURN_CANDIDATES_FILE_NAME = "highest_return_candidates.csv"
RETURN_LOOKBACK_PERIOD = 180  # Based on https://www.bauer.uh.edu/rsusmel/phd/jegadeesh-titman93.pdf
MIN_AVG_MONTHLY_RETURN = 0.01  # Minimum average monthly return
MIN_LOWEST_MONTHLY_RETURN = -0.2  # Minimum lowest monthly return
MAX_MONTHLY_COV_RETURN = 3.0  # Maximum Coefficient of Variation for monthly returns


class HighestReturnsFinder:
    # Finds the highest monthly returns in a set of stocks
    def __init__(self, fmp_api_key: str):
        self.stock_list_loader = FmpStockListLoader(fmp_api_key)
        self.dat_loader = FmpDataLoader(fmp_api_key)

    def calculate_metrics(self, symbol, prices_df):
        # Resample prices to monthly frequency and calculate monthly returns
        monthly_prices = prices_df['close'].resample('M').last()
        monthly_returns = monthly_prices.pct_change().dropna()

        # Calculate average monthly returns
        avg_monthly_return = monthly_returns.mean()

        # Calculate highest monthly return
        highest_monthly_return = monthly_returns.max()

        # Calculate lowest monthly return
        lowest_monthly_return = monthly_returns.min()

        # Calculate standard deviation of monthly returns
        std_dev_monthly_return = monthly_returns.std()

        # Calculate coefficient of variation (CV)
        cv = std_dev_monthly_return / avg_monthly_return

        # Calculate annualized return
        annualized_return = (1 + avg_monthly_return) ** 12 - 1

        # Calculate highest average return score
        highest_avg_return_score = highest_monthly_return - cv

        metrics = {
            'symbol': symbol,
            'avg_monthly_return': avg_monthly_return,
            'highest_monthly_return': highest_monthly_return,
            'lowest_monthly_return': lowest_monthly_return,
            'cv': cv,
            'annualized_return': annualized_return,
            'std_dev_monthly_return': std_dev_monthly_return,
            'highest_avg_return_score': highest_avg_return_score
        }

        return metrics

    def find_candidates(self):
        logi(f"Calculating metrics....")
        metrics_df = pd.DataFrame()

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

        prices_dict = self.dat_loader.fetch_multiple_daily_prices_by_date(symbol_list, start_date_str, end_date_str,
                                                                        cache_data=True, cache_dir=CACHE_DIR)

        i = 1
        for symbol in symbol_list:
            # Get prices
            if symbol not in prices_dict:
                logw(f"No prices for {symbol}")
                continue
            prices_df = prices_dict[symbol]

            # Calculate metrics
            metrics = self.calculate_metrics(symbol, prices_df)

            row = pd.DataFrame(metrics, index=[0])
            metrics_df = pd.concat([metrics_df, row], axis=0, ignore_index=True)
            i += 1

        # Apply filters
        metrics_df = metrics_df[metrics_df['lowest_monthly_return'] >= MIN_LOWEST_MONTHLY_RETURN]
        metrics_df = metrics_df[metrics_df['avg_monthly_return'] >= MIN_AVG_MONTHLY_RETURN]
        metrics_df = metrics_df[metrics_df['cv'] <= MAX_MONTHLY_COV_RETURN]

        # Sort by highest average monthly returns
        metrics_df.sort_values(by=["highest_avg_return_score"], ascending=[False], inplace=True)

        # Store for review
        os.makedirs(HIGHEST_RETURN_CANDIDATES_DIR, exist_ok=True)
        store_csv(HIGHEST_RETURN_CANDIDATES_DIR, HIGHEST_RETURN_CANDIDATES_FILE_NAME, metrics_df)

        logi("Done analyzing highest avg monthly returns candidates")

        return metrics_df
