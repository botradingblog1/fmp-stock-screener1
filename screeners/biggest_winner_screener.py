import pandas as pd
from utils.log_utils import *
from utils.file_utils import *
import time
from datetime import datetime, timedelta
import os
from universe_selection.universe_selector import UniverseSelector
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
from screeners.analyst_ratings_screener import AnalystRatingsScreener
from screeners.price_target_screener import PriceTargetScreener
from screeners.estimated_eps_screener import EstimatedEpsScreener
from screeners.institutional_ownership_screener import InstitutionalOwnershipScreener
from report_generators.company_report_generator import CompanyReportGenerator
from utils.file_utils import *
from sklearn.preprocessing import MinMaxScaler
from ai_clients.openai_client import OpenAiClient
from utils.fmp_utils import *
import json


# Considers 6-month returns, see https://www.bauer.uh.edu/rsusmel/phd/jegadeesh-titman93.pdf
# ‘Returns to Buying Winners and Selling Losers’ (Jegadeesh/Titman)


class BiggestWinnerScreener:
    def __init__(self, fmp_api_key: str, openai_api_key: str):
        self.universe_selector = UniverseSelector(fmp_api_key)
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)
        self.analyst_ratings_screener = AnalystRatingsScreener(fmp_api_key)
        self.price_target_screener = PriceTargetScreener(fmp_api_key)
        self.estimated_eps_screener = EstimatedEpsScreener(fmp_api_key)
        self.inst_own_screener = InstitutionalOwnershipScreener(fmp_api_key)
        self.openai_client = OpenAiClient(openai_api_key)
        self.report_generator = CompanyReportGenerator(fmp_api_key, openai_api_key)

    def calculate_momentum(self, symbol_list, prices_dict):
        results = []
        for symbol in symbol_list:
            if symbol not in prices_dict:
                continue
            prices_df = prices_dict[symbol]
            # Check minimum length
            if len(prices_df) < 2:
                logw(f"Not enough price data for {symbol}")
                continue
            # Current price (latest)
            current_price = prices_df['close'].iloc[-1]

            # Price start month
            start_month_price = prices_df['close'].iloc[0]

            # Calculate momentum factor
            lookback_return = ((current_price - start_month_price) / start_month_price) * 100
            lookback_return = round(lookback_return, 2)

            result_row = {'symbol': symbol, 'lookback_return': lookback_return}
            results.append(result_row)

        # Convert to dataframe
        results_df = pd.DataFrame(results)
        return results_df

    def fetch_quarterly_revenue_growth(self, symbol_list: list, lookback_periods: int = 4):
        results = []
        for symbol in symbol_list:
            # Fetch quarterly income statements
            income_df = self.fmp_data_loader.fetch_income_statement(symbol, period="quarterly")
            if income_df is None or income_df.empty:
                continue

            # Ensure dates are sorted ascending for correct pct_change calculation
            income_df = income_df.sort_values(by='date', ascending=True)

            # Keep only rows with positive revenue
            income_df = income_df[income_df['revenue'] > 0].copy()

            # Take the most recent lookback_periods quarters
            income_df = income_df.tail(lookback_periods)
            # if income_df is None or income_df.empty or len(income_df) < 2:
            # Skip if not enough data for calculation
            #    continue

            # Calculate percentage growth for each quarter
            income_df['quarterly_revenue_growth'] = income_df['revenue'].pct_change(1)

            # Compute the average growth over the period
            avg_quarterly_revenue_growth = round(income_df['quarterly_revenue_growth'].mean() * 100, 2)

            # Append result
            result_row = {'symbol': symbol, 'avg_quarterly_revenue_growth': avg_quarterly_revenue_growth}
            results.append(result_row)

        # Convert results to a DataFrame
        results_df = pd.DataFrame(results)
        return results_df

    def screen_candidates(self):
        logd(f"MetaScreener.screen_candidates")

        # Universe selection
        self.universe_selector.perform_selection(industry_list=None)
        stock_list_df = self.universe_selector.get_stock_info()

        # Exclude biotech industry
        stock_list_df = stock_list_df[~ stock_list_df['industry'].isin(BIOTECH_INDUSTRY_LIST)]
        symbol_list = stock_list_df['symbol'].unique()
        #symbol_list = symbol_list[0:3]

        # Fetch prices - switch to one month
        start_date = datetime.today() - timedelta(days=30)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date = datetime.today()
        end_date_str = end_date.strftime("%Y-%m-%d")
        prices_dict = self.fmp_data_loader.fetch_multiple_daily_prices_by_date(symbol_list, start_date_str, end_date_str, cache_data=True, cache_dir=CACHE_DIR)

        # Calculate momentum
        momentum_df = self.calculate_momentum(symbol_list, prices_dict)
        momentum_df.sort_values(by=['lookback_return'], ascending=[False], inplace=True)

        # Get top results
        momentum_df = momentum_df.head(10)
        symbol_list = momentum_df['symbol'].unique()

        # Run price target screener
        price_target_results_df = self.price_target_screener.screen_candidates(symbol_list, min_ratings_count=0)

        # Run analyst ratings screener
        analyst_ratings_results_df = self.analyst_ratings_screener.screen_candidates(symbol_list, min_ratings_count=0)

        # Fetch revenue growth
        quarterly_revenue_growth_df = self.fetch_quarterly_revenue_growth(symbol_list)

        # Run estimated revenue growth
        estimated_revenue_df = fetch_future_revenue_growth(self.fmp_data_loader, symbol_list, period="annual")

        # Get institutional ownership
        inst_own_results_df = self.inst_own_screener.screen_candidates(symbol_list)

        # Get ratios
        combined_ratios_df = fetch_multiple_ratios(self.fmp_data_loader, symbol_list, period="quarterly")

        # Merge all detailed results on symbol
        stats_df = momentum_df.merge(analyst_ratings_results_df, on='symbol', how='left')
        stats_df = stats_df.merge(price_target_results_df, on='symbol', how='left')
        stats_df = stats_df.merge(quarterly_revenue_growth_df, on='symbol', how='left')
        stats_df = stats_df.merge(estimated_revenue_df, on='symbol', how='left')
        stats_df = stats_df.merge(inst_own_results_df, on='symbol', how='left')
        stats_df = stats_df.merge(combined_ratios_df, on='symbol', how='left')

        # Handle missing values
        stats_df = stats_df.fillna(0)

        # Filter minimums
        """
        stats_df = stats_df[stats_df['avg_quarterly_revenue_growth'] >= 2.0]
        stats_df = stats_df[stats_df['bullish_count'] >= 0]
        stats_df = stats_df[stats_df['investors_put_call_ratio'] < 1.0]
        stats_df = stats_df[stats_df['price_earnings_ratio'] <= 50.0]
        """

        """
        # Invert P/E ratio (avoid division by zero)
        stats_df['inverted_price_earnings_ratio'] = 1 / stats_df['price_earnings_ratio'].replace(0, np.nan).fillna(1e-6)

        # Normalize columns
        columns_to_normalize = [
            'avg_quarterly_revenue_growth',
            'total_grades_rating',
            'avg_price_target_change',
            'avg_estimated_revenue_change',
            'inverted_price_earnings_ratio',  # Include the inverted P/E ratio
        ]
        scaler = MinMaxScaler()
        normalized_data = scaler.fit_transform(stats_df[columns_to_normalize])
        normalized_columns_df = pd.DataFrame(normalized_data, columns=[f'norm_{col}' for col in columns_to_normalize])

        # Concatenate normalized columns to the original DataFrame
        stats_df = pd.concat([stats_df.reset_index(drop=True), normalized_columns_df], axis=1)

        # Calculate weighted score using the normalized columns
        stats_df['weighted_score'] = (
            stats_df['norm_avg_price_target_change'] * 0.4 +
            stats_df['norm_avg_quarterly_revenue_growth'] * 0.2 +
            stats_df['norm_total_grades_rating'] * 0.1 +
            stats_df['norm_avg_estimated_revenue_change'] * 0.2 +
            stats_df['norm_inverted_price_earnings_ratio'] * 0.1  # Use normalized inverted P/E
        )

        # Sort by weighted score
        #stats_df = stats_df.sort_values(by='weighted_score', ascending=False)

        # Drop the norm columns and any intermediate columns
        columns_to_drop = [col for col in stats_df.columns if col.startswith('norm_')]
        stats_df.drop(columns=columns_to_drop, inplace=True)

        # Reset index
        stats_df.reset_index(drop=True, inplace=True)
        """

        # Pick columns
        stats_df = stats_df[
            ['symbol', 'lookback_return', 'price_earnings_ratio', 'avg_quarterly_revenue_growth',
             'avg_estimated_revenue_change', 'avg_revenue_estimate_analysts',
             'bullish_count', 'hold_count', 'avg_price_target_change',
             'num_price_target_analysts',
             'investors_holding', 'investors_holding_change',
             'investors_put_call_ratio']]

        # Pick top stocks
        stats_df = stats_df.head(10)

        # Store results
        file_name = f"biggest_winner_results.csv"
        store_csv(RESULTS_DIR, file_name, stats_df)

        symbol_list = stats_df['symbol'].unique()

        # Run report generator
        for symbol in symbol_list:
            # Generate report
            self.report_generator.generate_report(symbol, reports_dir=REPORTS_DIR)

        return stats_df
