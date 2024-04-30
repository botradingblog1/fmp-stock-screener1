import pandas as pd
from config import *
from utils.fmp_client import FmpClient
from utils.log_utils import *
import time
from datetime import datetime, timedelta
import numpy as np


class FmpDividendLoader:
    def __init__(self, fmp_api_key):
        self.fmp_client = FmpClient(fmp_api_key)

    def calculate_yearly_returns(self, symbol, prices_df):
        try:
            # Resample the data to get the last trading day of each year
            yearly_prices_df = prices_df.resample('Y').last()

            # Calculate the yearly returns as percent change of the 'close' price
            yearly_prices_df['annual_close_returns'] = round(yearly_prices_df['close'].pct_change() * 100, 2)
            yearly_prices_df.dropna(inplace=True)
            yearly_prices_df = yearly_prices_df[['annual_close_returns']]

            # Store for review
            file_name = f"{symbol}_annual_returns.csv"
            path = os.path.join(CACHE_DIR, file_name)
            yearly_prices_df.to_csv(path)

            # Calculate average and standard deviation of the yearly returns
            avg_annual_return = yearly_prices_df['annual_close_returns'].mean()
            std_annual_return = yearly_prices_df['annual_close_returns'].std()

            return avg_annual_return, std_annual_return
        except Exception as ex:
            print(ex)
            return None, None

    def calculate_average_price_per_year(self, prices_df):
        try:
            # Resample and calculate the average yearly price
            average_prices_df = prices_df['close'].resample('Y').mean()
            return average_prices_df
        except Exception as ex:
            print(f"Exception in calculating average prices: {ex}")
            return None

    def calculate_dividend_yield(self, dividends_df, prices_df):
        try:
            # Total dividend payments per year
            annual_dividends_df = dividends_df.resample('Y')['adjDividend'].agg(['sum', 'count'])

            # Calculate average price per year
            avg_prices_df = self.calculate_average_price_per_year(prices_df)

            # Calculate average dividend payment
            annual_dividends_df['average_per_payment'] = annual_dividends_df['sum'] / annual_dividends_df['count']
            annual_dividends_df['average_price'] = avg_prices_df
            annual_dividends_df['dividend_yield'] = round(
                (annual_dividends_df['sum'] / annual_dividends_df['average_price']) * 100, 2)  # Yield in percentage

            return annual_dividends_df
        except Exception as ex:
            print(f"Exception in calculating dividends and yield: {ex}")
            return None

    def cap_values(self, value, min_val, max_val):
        return max(min_val, min(value, max_val))

    def fetch(self, symbol_list, prices_dict):
        dividend_results = []
        # Used to cap outlier values
        MAX_DIVIDEND_YIELD = 1000
        MIN_DIVIDEND_YIELD = 0
        LOOKBACK_DAYS = 365 * 3
        i = 1
        for symbol in symbol_list:
            logd(f"Fetching dividends for {symbol}...  ({i}/{len(symbol_list)})")

            # Get prices
            if symbol not in prices_dict:
                logw(f"No prices for dividend calculation for {symbol}")
                avg_dividend_yield = 0
            else:
                prices_df = prices_dict[symbol]

                # Fetch dividends
                dividends_df = self.fmp_client.fetch_dividends(symbol)
                if dividends_df is None or len(dividends_df) == 0:
                    logw(f"Not enough dividend data for {symbol}")
                    avg_dividend_yield = 0
                else:
                    # Filter by date
                    start_date = datetime.today() - timedelta(days=LOOKBACK_DAYS)
                    dividends_df = dividends_df[dividends_df.index >= start_date]
                    if len(dividends_df) == 0:
                        logw(f"Not enough dividend data for {symbol}")
                        avg_dividend_yield = 0
                    else:
                        # Filter prices by start date
                        prices_df = prices_df[prices_df.index >= start_date]

                        # Calculate dividend yield
                        annual_dividends_df = self.calculate_dividend_yield(dividends_df, prices_df)
                        # Cap dividend values
                        avg_dividend_yield = self.cap_values(annual_dividends_df['dividend_yield'].mean(), MIN_DIVIDEND_YIELD,
                                                        MAX_DIVIDEND_YIELD)
                        std_dividend_yield = self.cap_values(annual_dividends_df['dividend_yield'].std(), MIN_DIVIDEND_YIELD,
                                                        MAX_DIVIDEND_YIELD)

            dividend_results.append({'symbol': symbol, 'avg_dividend_yield': avg_dividend_yield})

            i += 1

            # Throttle for API limit
            time.sleep(API_REQUEST_DELAY)

        dividend_stats_df = pd.DataFrame(dividend_results)

        # Store results
        file_name = "dividend_results.csv"
        path = os.path.join(RESULTS_DIR, file_name)
        dividend_stats_df.to_csv(path)

        return dividend_stats_df

