from data_loaders.market_symbol_loader import MarketSymbolLoader
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
from config import *
from datetime import datetime, timedelta
import pandas as pd
import os
from sklearn.preprocessing import MinMaxScaler
from utils.log_utils import *
from utils.indicator_utils import add_kernel_reg_smoothed_line, compute_slope
import numpy as np
import json

# Configuration
CANDIDATES_DIR = "C:\\dev\\trading\\data\\estimated_growth\\candidates"
CANDIDATES_FILE_NAME = "estimated_growth_candidates.csv"

# Stock screener config
EXCHANGE_LIST = "nyse,nasdaq,amex"
PRICE_MORE_THAN = 1.0  # Don't want to trade below a buck
PRICE_LESS_THAN = 1000.0  # Am I Buffett, or what???
VOLUME_MORE_THAN = 5000  # Need a bit of liquidity to trade
COUNTRY = "US"
STOCK_SCREENER_LIMIT = 3000

# List of growth industries for universe selection
INDUSTRY_LIST = [
    'Semiconductors', 'Consumer Electronics', 'Software - Infrastructure',
    'Internet Content & Information', 'Biotechnology', 'Telecommunications Services',
    'Communication Equipment', 'Information Technology Services',
    'Medical - Diagnostics & Research', 'Medical - Instruments & Supplies',
    'Computer Hardware', 'Gambling, Resorts & Casinos', 'Electronic Gaming & Multimedia',
    'Renewable Utilities', 'Solar', 'Technology Distributors', 'Medical - Equipment & Services',
    'Internet Software/Services', 'Software - Application', 'Luxury Goods'
]

class EstimatedGrowthCandidateFinder:
    def __init__(self, fmp_api_key):
        self.symbol_loader = MarketSymbolLoader()
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)

    def fetch_price_data(self, symbol_list: list):
        start_date = (datetime.now() - timedelta(days=400)).strftime('%Y-%m-%d')
        end_date = datetime.now().strftime('%Y-%m-%d')
        prices_dict = self.fmp_data_loader.fetch_multiple_daily_prices_by_date(
            symbol_list, start_date, end_date, cache_data=True, cache_dir=CACHE_DIR
        )

        results = []
        for symbol, df in prices_dict.items():
            if df.empty:
                continue
            logd(f"Calculating trend for {symbol}")
            # Add smoothed close trend and slope
            df = add_kernel_reg_smoothed_line(
                df, column_list=['close'], output_cols=['close_smoothed'], bandwidth=9, var_type='c'
            )
            df = compute_slope(
                df, target_col='close_smoothed', slope_col='close_smoothed_slope', window_size=3
            )
            df['lt_trend_up'] = np.where(df['close_smoothed_slope'] > 0.01, 1, 0)

            # Add monthly low indicator
            window = 20
            df['monthly_low'] = df['close'].rolling(window=window).min()
            df['near_monthly_low'] = np.where(
                df['close'] <= df['monthly_low'] * 1.2, 1, 0
            )

            # Filter
            if df['lt_trend_up'].iloc[-1] < 1:
                continue

            result = {'symbol': symbol}
            result['lt_trend_up'] = df['lt_trend_up'].iloc[-1]
            result['near_monthly_low'] = df['near_monthly_low'].iloc[-1]

            results.append(result)

        # Convert to dataframe
        results_df = pd.DataFrame(results)

        return results_df

    def fetch_analyst_estimates(self, symbol_list):
        estimates = self.fmp_data_loader.fetch_multiple_analyst_earnings_estimates(symbol_list, period='annual')
        results = []
        for symbol, df in estimates.items():
            logd(f"Calculating estimates for {symbol}")
            if df.empty:
                continue
            df['date'] = pd.to_datetime(df['date'])
            df = df[df['date'] >= pd.Timestamp.now()]
            df = df.sort_values('date')

            # Calculate changes and averages
            result = {'symbol': symbol, 'avg_revenue_change': None, 'avg_net_income_change': None,
                      'avg_num_estimate_analysts': None}
            df['revenue_change'] = df['estimatedRevenueAvg'].pct_change() * 100
            df['net_income_change'] = df['estimatedNetIncomeAvg'].pct_change() * 100
            result['avg_revenue_change'] = round(df['revenue_change'].mean(), 2)
            result['avg_net_income_change'] = round(df['net_income_change'].mean(), 2)
            result['avg_num_estimate_analysts'] = round(df['numberAnalystEstimatedRevenue'].mean(), 2)

            # Filter: Positive revenue and net income growth with sufficient analysts
            if result['avg_revenue_change'] > 0 and \
               result['avg_net_income_change'] > 0 and \
               result['avg_num_estimate_analysts'] >= 3:
                results.append(result)
        # Convert to dataframe
        results_df = pd.DataFrame(results)

        return results_df

    def fetch_price_targets(self, symbol_list):
        results = []
        for symbol in symbol_list:
            df = self.fmp_data_loader.fetch_price_targets(symbol)
            if df is None or df.empty:
                continue
            # Convert to datetime with UTC awareness
            df['publishedDate'] = pd.to_datetime(df['publishedDate'])
            comparison_date = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=60)

            # Filter based on timezone-aware comparison
            df = df[df['publishedDate'] >= comparison_date]
            df['priceTargetChangePercent'] = ((df['adjPriceTarget'] - df['priceWhenPosted']) / df['priceWhenPosted']) * 100
            result = {'symbol': symbol, 'avg_price_target_change_percent': None}
            result['avg_price_target_change_percent'] = round(df['priceTargetChangePercent'].mean(), 2)

            # Filter: Positive price target change
            if result['avg_price_target_change_percent'] > 0:
                results.append(result)

        # Convert to DataFrame
        results_df = pd.DataFrame(results)

        return results_df

    def fetch_company_outlook(self, symbol_list):
        os.makedirs(CACHE_DIR, exist_ok=True)  # Ensure the cache directory exists
        results = []

        for symbol in symbol_list:
            cache_file = os.path.join(CACHE_DIR, f"{symbol}_outlook.json")

            # Check if cache exists
            if os.path.exists(cache_file):
                with open(cache_file, 'r') as f:
                    company_outlook = json.load(f)
                    logi(f"Loaded company outlook for {symbol} from cache.")
            else:
                # Fetch data from API
                company_outlook = self.fmp_data_loader.fetch_company_outlook(symbol)
                if not company_outlook:
                    logw(f"No company outlook data available for {symbol}.")
                    continue

                # Save to cache
                with open(cache_file, 'w') as f:
                    json.dump(company_outlook, f)
                    logi(f"Cached company outlook for {symbol}.")

            # Parse the fetched or cached data
            result = {
                "symbol": symbol,
                'company_name': '',
                'description': '',
                'website': None,
                'price': None,
                'market_cap': None,
                'pe_ratio': None
            }
            profile = company_outlook.get('profile')
            if profile:
                result["company_name"] = profile.get('companyName')
                result["description"] = profile.get('description')
                result["website"] = profile.get('website')
                result["price"] = profile.get('price')
                result["market_cap"] = profile.get('mktCap')

            ratios = company_outlook.get('ratios')
            if ratios:
                first_ratios = ratios[0]
                result["pe_ratio"] = first_ratios.get('peRatioTTM')

            results.append(result)

        # Convert results to DataFrame
        company_info_df = pd.DataFrame(results)

        return company_info_df

    def find_candidates(self):
        # Fetch stock screener info
        stock_list_df = self.fmp_data_loader.fetch_stock_screener_results(
            exchange_list=EXCHANGE_LIST, price_more_than=PRICE_MORE_THAN, price_lower_than=PRICE_LESS_THAN,
            volume_more_than=VOLUME_MORE_THAN, is_etf=False, is_fund=False, is_actively_trading=True, limit=STOCK_SCREENER_LIMIT
        )
        if stock_list_df.empty:
            logw("Stock screener returned no data.")
            return
        stock_list_df = stock_list_df[stock_list_df['industry'].isin(INDUSTRY_LIST)]
        # Select columns
        stock_list_df = stock_list_df[['symbol', 'companyName', 'marketCap', 'industry', 'beta']]
        symbol_list = stock_list_df['symbol'].unique()
        #symbol_list = symbol_list[0:5]

        # Fetch and process price data
        prices_df = self.fetch_price_data(symbol_list)

        # Merge with stock list to retain only the symbols that passed the price data filters
        merged_df = stock_list_df.merge(prices_df, on='symbol', how='inner')

        # Update symbol list
        symbol_list = merged_df['symbol'].unique()

        # Fetch analyst estimates
        analyst_estimates_df = self.fetch_analyst_estimates(symbol_list)

        # Fetch price targets
        price_targets_df = self.fetch_price_targets(symbol_list)

        # Fetch company outlook
        company_outlook_df = self.fetch_company_outlook(symbol_list)

        # Merge data
        merged_df = merged_df.merge(analyst_estimates_df, on='symbol', how='inner')
        merged_df = merged_df.merge(price_targets_df, on='symbol', how='inner')
        merged_df = merged_df.merge(company_outlook_df, on='symbol', how='inner')

        if merged_df.empty:
            logw("No candidates found after merging data.")
            return

        # Handle missing values
        merged_df = merged_df.fillna(0)

        # Adjust pe_ratio (invert it since lower is better)
        merged_df['inv_pe_ratio'] = merged_df['pe_ratio'].replace(0, np.nan)
        merged_df['inv_pe_ratio'] = 1 / merged_df['inv_pe_ratio']
        merged_df['inv_pe_ratio'] = merged_df['inv_pe_ratio'].replace(np.nan, 0)

        # Normalize columns
        columns_to_normalize = [
            'avg_revenue_change',
            'avg_net_income_change',
            'avg_price_target_change_percent',
            'inv_pe_ratio'
        ]
        scaler = MinMaxScaler()
        normalized_data = scaler.fit_transform(merged_df[columns_to_normalize])
        normalized_columns = pd.DataFrame(normalized_data, columns=[f'norm_{col}' for col in columns_to_normalize])

        # Concatenate normalized columns to the original DataFrame
        merged_df = pd.concat([merged_df.reset_index(drop=True), normalized_columns], axis=1)

        # Store original columns
        original_columns = merged_df.columns.tolist()

        # Calculate weighted score using the normalized columns
        merged_df['weighted_score'] = (
            merged_df['norm_avg_revenue_change'] * 0.4 +
            merged_df['norm_avg_net_income_change'] * 0.2 +
            merged_df['norm_avg_price_target_change_percent'] * 0.2 +
            merged_df['norm_inv_pe_ratio'] * 0.2
        )

        # Sort by weighted score
        final_df = merged_df.sort_values(by='weighted_score', ascending=False)

        # Drop the norm columns and any intermediate columns
        columns_to_drop = [col for col in final_df.columns if col.startswith('norm_') or col in ['inv_pe_ratio']]
        final_df.drop(columns=columns_to_drop, inplace=True)

        # Reset index
        final_df.reset_index(drop=True, inplace=True)

        # Store results
        os.makedirs(CANDIDATES_DIR, exist_ok=True)
        path = os.path.join(CANDIDATES_DIR, CANDIDATES_FILE_NAME)
        final_df.to_csv(path, index=False)
        logi(f"Candidate results saved to {path}")

        return final_df
