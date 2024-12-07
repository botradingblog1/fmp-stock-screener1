import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from config import *
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
from botrading.utils.df_utils import replace_inf_values, save_dataframe_to_csv
from botrading.utils.date_utils import create_date_range
from utils.file_utils import *
from botrading.base.enums import TimeInterval
import empyrical as ep


# Configuration
ETF_PERFORMANCE_CANDIDATES_DIR = "C:\\dev\\trading\\data\\etf_performance\\candidates"
ETF_PERFORMANCE_CANDIDATES_FILE_NAME = "etf_performance_candidates.csv"
PRECISION = 4

# Fund screener config - Updated to uppercase
BENCHMARK_SYMBOL = "SPY"
EXCHANGE_LIST = "nyse,nasdaq,amex"
IS_ETF = True  # If this flag is true, turn off 'is_fund'
IS_FUND = False  # If this flag is true, turn off 'is_etf'
LIMIT = 3000
BETA_LOWER_THAN = 5.0
VOLUME_MORE_THAN = 1000
PRICE_MORE_THAN = 5.0
PRICE_LOWER_THAN = 10000
MARKET_CAP_MORE_THAN = 10000
COUNTRY = "US"
NUM_YEARS = 10  # Number of years of price data analyzed


class EtfPerformanceScreener():
    def __init__(self, fmp_api_key: str):
        self.fmp_api_key = fmp_api_key
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)

    def find_candidates(self):
        # Clean up previous candidates file
        delete_file(ETF_PERFORMANCE_CANDIDATES_DIR, ETF_PERFORMANCE_CANDIDATES_FILE_NAME)

        # Load ETFs (assuming the data_loader and other parameters are properly set)
        fund_data_df = self.fmp_data_loader.fetch_stock_screener_results(
            exchange_list=EXCHANGE_LIST,
            market_cap_more_than=MARKET_CAP_MORE_THAN,
            price_more_than=PRICE_MORE_THAN,
            price_lower_than=PRICE_LOWER_THAN,
            beta_lower_than=BETA_LOWER_THAN,
            volume_more_than=VOLUME_MORE_THAN,
            is_etf=IS_ETF,
            is_fund=IS_FUND,
            is_actively_trading=True,
            limit=LIMIT
        )

        # Filter out stocks from other exchanges
        stock_list_df = fund_data_df[~fund_data_df['symbol'].str.contains(r'\.\w{1,4}$')]

        # Generate symbol list
        symbol_list = stock_list_df['symbol'].unique().tolist()

        # Add market index to the symbol list
        if BENCHMARK_SYMBOL not in symbol_list:
            symbol_list.append(BENCHMARK_SYMBOL)

        # Get start- and end-dates for fetching prices
        start_date_str, end_date_str = create_date_range(TimeInterval.DAY, num_past_periods=365 * NUM_YEARS, date_format='%Y-%m-%d')

        # Fetch prices
        fund_prices_dict = self.fmp_data_loader.fetch_multiple_daily_prices_by_date(symbol_list,
                                                                                    start_date_str,
                                                                                    end_date_str,
                                                                                    cache_data=True,
                                                                                    cache_dir=CACHE_DIR)

        # Calculate daily returns and merge dataframes based on the date
        returns_dict = {}
        for symbol, prices_df in fund_prices_dict.items():
            prices_df.reset_index(inplace=True)
            prices_df['date'] = pd.to_datetime(prices_df['date'])
            prices_df.set_index('date', inplace=True)
            prices_df['adj_close'] = prices_df['adj_close'].ffill()  # Fill missing prices
            prices_df['daily_return'] = prices_df['adj_close'].pct_change().fillna(0)
            prices_df = replace_inf_values(prices_df)
            returns_dict[symbol] = prices_df[['daily_return']]

        # Merge all dataframes on date
        aligned_returns_df = pd.DataFrame()
        for symbol, df in returns_dict.items():
            if aligned_returns_df.empty:
                aligned_returns_df = df.rename(columns={'daily_return': symbol})
            else:
                aligned_returns_df = aligned_returns_df.join(df.rename(columns={'daily_return': symbol}), how='outer')

        # Initialize metrics dictionary
        metrics = {
            'etf': [],
            'alpha': [],
            'beta': [],
            'volatility': [],
            'max_drawdown': [],
            'avg_annual_return': [],
            'avg_3yr_return': [],
            'avg_5yr_return': [],
            'avg_10yr_return': [],
            'return_y1': [],
            'return_y2': [],
            'return_y3': [],
            'return_y4': [],
            'return_y5': [],
            'return_y6': [],
            'return_y7': [],
            'return_y8': [],
            'return_y9': [],
            'return_y10': []
        }

        # Calculate metrics using Empyrical
        for symbol in symbol_list:
            print(f"Now calculating metrics for {symbol}...")
            alpha, beta = ep.alpha_beta(aligned_returns_df[symbol], aligned_returns_df[BENCHMARK_SYMBOL])
            alpha = round(alpha * 100, PRECISION)
            beta = round(beta, PRECISION)

            volatility = round(ep.annual_volatility(aligned_returns_df[symbol]), PRECISION) * 100
            max_drawdown = round(ep.max_drawdown(aligned_returns_df[symbol]), PRECISION) * 100

            # Calculate average annual return
            total_return = (aligned_returns_df[symbol] + 1).prod() - 1
            years = NUM_YEARS  # The number of years of data
            avg_annual_return = round((1 + total_return) ** (1 / years) - 1, PRECISION) * 100

            # Calculate average 3-year, 5-year, and 10-year returns
            avg_3yr_return = round(ep.annual_return(aligned_returns_df[symbol].last('3Y')) * 100, PRECISION)
            avg_5yr_return = round(ep.annual_return(aligned_returns_df[symbol].last('5Y')) * 100, PRECISION)
            avg_10yr_return = round(ep.annual_return(aligned_returns_df[symbol].last('10Y')) * 100, PRECISION)

            metrics['etf'].append(symbol)
            metrics['alpha'].append(alpha)
            metrics['beta'].append(beta)
            metrics['volatility'].append(volatility)
            metrics['max_drawdown'].append(max_drawdown)
            metrics['avg_annual_return'].append(avg_annual_return)
            metrics['avg_3yr_return'].append(avg_3yr_return)
            metrics['avg_5yr_return'].append(avg_5yr_return)
            metrics['avg_10yr_return'].append(avg_10yr_return)

            # Calculate annual returns for each year
            for year in range(1, NUM_YEARS + 1):
                year_start_date = datetime.today() - timedelta(days=365 * year)
                year_end_date = year_start_date + timedelta(days=365)
                year_returns = aligned_returns_df[symbol][
                    (aligned_returns_df.index >= year_start_date) & (aligned_returns_df.index <= year_end_date)]
                annual_return = ((year_returns + 1).prod() - 1) if not year_returns.empty else None
                metrics[f'return_y{year}'].append(round(annual_return * 100, PRECISION) if annual_return is not None else None)

        # Create a dataframe from the dictionary
        metrics_df = pd.DataFrame(metrics)

        # Remove any rows that have infinity values to remove outliers
        metrics_df = metrics_df.replace([np.inf, -np.inf], np.nan).dropna()

        # Sort by metrics
        metrics_df.sort_values(by=['avg_10yr_return', 'max_drawdown', 'volatility'], ascending=[False, True, True], inplace=True)

        # Store output file
        os.makedirs(ETF_PERFORMANCE_CANDIDATES_DIR, exist_ok=True)
        save_dataframe_to_csv(metrics_df, ETF_PERFORMANCE_CANDIDATES_DIR, ETF_PERFORMANCE_CANDIDATES_FILE_NAME)
