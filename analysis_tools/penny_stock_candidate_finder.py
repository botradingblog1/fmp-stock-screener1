import numpy as np
import pandas as pd
import os
from botrading.data_loaders.tiingo_data_loader import TiingoDataLoader, TiingoDailyInterval
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
from data_loaders.fmp_analyst_ratings_loader import FmpAnalystRatingsLoader
from data_loaders.fmp_price_target_loader import FmpPriceTargetLoader
from data_loaders.fmp_inst_own_data_loader import FmpInstOwnDataLoader
from data_loaders.fmp_analyst_estimates_loader import FmpAnalystEstimatesLoader
from utils.plot_utils import plot_pullback_chart
from utils.file_utils import *
from utils.log_utils import *
from utils.indicator_utils import *
from config import *
from datetime import datetime, timedelta

PLOTS_DIR = "C:\\dev\\trading\\data\\penny_stocks\\plots"
CANDIDATES_DIR = "C:\\dev\\trading\\data\\penny_stocks\\candidates"

BENCHMARK_SYMBOL = "SPY"
EXCHANGE_LIST = "nyse,nasdaq,amex"
LIMIT = 3000
VOLUME_MORE_THAN = 1000
PRICE_MORE_THAN = 1.0
PRICE_LOWER_THAN = 10.0
MARKET_CAP_LOWER_THAN = 100000000
COUNTRY = "US"
NUM_YEARS = 10  # Number of years of price data analyzed


class PennyStockFinder:
    def __init__(self, tiingo_api_key: str, fmp_api_key: str):
        self.data_loader = TiingoDataLoader(tiingo_api_key)
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)
        self.analyst_ratings_loader = FmpAnalystRatingsLoader(fmp_api_key)
        self.price_target_loader = FmpPriceTargetLoader(fmp_api_key)
        self.inst_own_loader = FmpInstOwnDataLoader(fmp_api_key)
        self.estimate_loader = FmpAnalystEstimatesLoader(fmp_api_key)

    def calculate_indicators(self, df: pd.DataFrame):
        long_ema_window = 10
        short_ema_window = 3

        # Calculate indicators
        df['trend_slope'] = calculate_trend(df)
        df['adx'] = calculate_adx(df, length=14)
        df['rsi'] = calculate_rsi(df, window=14)
        df[f"ema_short"] = calculate_ema(df, window=short_ema_window)
        df[f"ema_long"] = calculate_ema(df, window=long_ema_window)
        df.dropna(inplace=True)

        return df

    def find_candidates(self):
        logi("Finding penny stock candidates...")
        delete_files_in_directory(PLOTS_DIR)

        # Load symbols
        stock_list_df = self.fmp_data_loader.fetch_stock_screener_results(exchange_list=EXCHANGE_LIST,
                                                                         market_cap_lower_than=MARKET_CAP_LOWER_THAN,
                                                                         price_more_than=PRICE_MORE_THAN,
                                                                         price_lower_than=PRICE_LOWER_THAN,
                                                                         is_actively_trading=True,
                                                                         is_fund=False,
                                                                         is_etf=False,
                                                                         country=COUNTRY,
                                                                         limit=1000)

        if stock_list_df is None or len(stock_list_df) == 0:
            logw("No stocks returned from FMP")
            return None
        symbol_list = stock_list_df['symbol']

        # Set start and end dates
        start_date = datetime.today() - timedelta(days=400)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date = datetime.today()
        end_date_str = end_date.strftime("%Y-%m-%d")

        results = []
        for symbol in symbol_list:
            print(f"Processing {symbol}")
            stock_info_df = stock_list_df[stock_list_df['symbol'] == symbol]

            stats = {
                'symbol': symbol,
                'company_name': stock_info_df['companyName'].iloc[0],
                'market_cap': stock_info_df['marketCap'].iloc[0],
                'volume': stock_info_df['volume'].iloc[0],
                'industry': stock_info_df['industry'].iloc[0],
                'sector': stock_info_df['sector'].iloc[0],
                'bullish_count': 0,
                'bearish_count': 0,
                'total_score': 0,
                'avg_revenue_growth': 0,
                'avg_net_income_growth': 0,
                'last_revenue_growth': 0,
                'last_net_income_growth': 0,
                'avg_price_target_change_percent': 0,
                'price_target_coefficient_variation': 0,
                'num_price_target_analysts': 0,
                'avg_revenue_change_percent': 0,
                'revenue_change_coefficient_variation': 0,
                'avg_num_analysts_estimates': 0,
                'investors_holding_change': 0,
                'total_invested_change': 0
            }

            # Fetch analyst ratings
            analyst_ratings_df = self.analyst_ratings_loader.fetch([symbol], num_lookback_days=60)
            if analyst_ratings_df is not None and not analyst_ratings_df.empty:
                stats['analyst_rating_score'] = analyst_ratings_df['analyst_rating_score'].iloc[0]

            # Fetch revenue growth
            growth_df = self.fmp_data_loader.get_income_growth(symbol, period="quarter")
            if growth_df is None or len(growth_df) == 0:
                continue

            # Filter records for the last year
            start_date = datetime.today() - timedelta(days=365)
            growth_df = growth_df[growth_df['date'] > start_date]
            if len(growth_df) == 0:
                continue

            # Calculate stats
            stats['avg_revenue_growth'] = growth_df['growthRevenue'].mean()
            stats['avg_net_income_growth'] = growth_df['growthNetIncome'].mean()
            stats['last_revenue_growth'] = growth_df['growthRevenue'].iloc[0]
            stats['last_net_income_growth'] = growth_df['growthNetIncome'].iloc[0]

            # Load prices
            prices_df = self.data_loader.fetch_end_of_day_prices(symbol, start_date_str, end_date_str,
                                                                interval=TiingoDailyInterval.DAILY,
                                                                cache_data=True, cache_dir=CACHE_DIR)
            if prices_df is None or len(prices_df) < 200:
                continue
            prices_df.reset_index(inplace=True)

            # Calculate indicators
            prices_df = self.calculate_indicators(prices_df)

            # Fetch price targets
            price_target_df = self.price_target_loader.load([symbol], prices_df, lookback_days=60)
            if price_target_df is not None and not price_target_df.empty:
                stats['avg_price_target_change_percent'] = price_target_df['avg_price_target_change_percent'].iloc[0]
                stats['price_target_coefficient_variation'] = price_target_df['price_target_coefficient_variation'].iloc[0]
                stats['num_price_target_analysts'] = price_target_df['num_price_target_analysts'].iloc[0]

            # Fetch analyst estimates
            estimates_df, estimate_results = self.estimate_loader.load(symbol, "annual")
            if estimate_results:
                stats['avg_revenue_change_percent'] = estimate_results['avg_revenue_change_percent']
                stats['revenue_change_coefficient_variation'] = estimate_results['revenue_change_coefficient_variation']
                stats['avg_num_analysts_estimates'] = estimate_results['avg_num_analysts']

            # Fetch institutional ownership
            inst_own_df = self.inst_own_loader.run([symbol])
            if inst_own_df is not None and not inst_own_df.empty:
                stats['investors_holding_change'] = inst_own_df['investors_holding_change'].iloc[0]
                stats['total_invested_change'] = inst_own_df['total_invested_change'].iloc[0]

            results.append(stats)

        # Convert stats to dataframe
        results_df = pd.DataFrame(results)

        # Calculate weighted score (example: simple weighting based on various factors)
        results_df['weighted_score'] = (
            results_df['avg_revenue_growth'] * 0.3 +
            results_df['analyst_rating_score'] * 0.3 +
            results_df['avg_price_target_change_percent'] * 0.2 +
            results_df['avg_revenue_change_percent'] * 0.1 +
            results_df['investors_holding_change'] * 0.1
        )

        # Sort by weighted score in descending order
        results_df = results_df.sort_values(by='weighted_score', ascending=False)

        # Keep the top 100 candidates
        top_100_df = results_df.head(100)

        # Plot charts for the top 10 candidates
        for index, row in top_100_df.head(10).iterrows():
            symbol = row['symbol']
            file_name = f"{symbol}_chart.png"
            plot_pullback_chart(symbol, prices_df, PLOTS_DIR, file_name=file_name)

        # Store the results to a CSV file
        file_name = f"penny_stock_candidates_{datetime.today().strftime('%Y-%m-%d')}.csv"
        store_csv(CACHE_DIR, top_100_df, file_name)

        logi("Done with penny stock analysis.")
