from data_loaders.market_symbol_loader import MarketSymbolLoader
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
from data_loaders.fmp_analyst_ratings_loader import FmpAnalystRatingsLoader
from data_loaders.fmp_earnings_estimate_loader import FmpEarningsEstimateLoader
from data_loaders.fmp_price_target_loader import FmpPriceTargetLoader
from data_loaders.fifty_two_week_low_loader import FiftyTwoWeekLowLoader
from config import *
from datetime import datetime, timedelta
import pandas as pd
import os
from sklearn.preprocessing import MinMaxScaler
from utils.log_utils import *


class PriceTargetCandidateFinder:
    def __init__(self, fmp_api_key):
        self.symbol_loader = MarketSymbolLoader()
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)
        self.fmp_analyst_ratings_loader = FmpAnalystRatingsLoader(fmp_api_key)
        self.fmp_earnings_estimate_loader = FmpEarningsEstimateLoader(fmp_api_key)
        self.fmp_price_target_loader = FmpPriceTargetLoader(fmp_api_key)
        self.fifty_two_week_low_loader = FiftyTwoWeekLowLoader()

    def find_candidates(self):
        # Fetch market symbols
        symbols_df = self.symbol_loader.fetch_russell1000_symbols(cache_file=True)
        symbol_list = symbols_df['symbol'].unique()

        # Fetch daily prices
        start_date = datetime.today() - timedelta(days=400)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date = datetime.today()
        end_date_str = end_date.strftime("%Y-%m-%d")
        prices_dict = self.fmp_data_loader.fetch_multiple_daily_prices_by_date(symbol_list,
                                                                               start_date_str,
                                                                               end_date_str,
                                                                               cache_data=True,
                                                                               cache_dir=CACHE_DIR)

        # Add 52-week low
        fifty_two_week_low_df = self.fifty_two_week_low_loader.load(symbol_list, prices_dict, min_price_drop_percent=0.2)
        if fifty_two_week_low_df.empty:
            logi(f"Fifty-week-low screener didn't return any data.")
            exit(0)
        symbol_list = fifty_two_week_low_df['symbol'].unique()

        # Fetch price targets
        price_targets_df = self.fmp_price_target_loader.load(symbol_list, prices_dict, lookback_days=60)
        if price_targets_df is not None and not price_targets_df.empty:
            merged_df = pd.merge(fifty_two_week_low_df, price_targets_df, on='symbol', how='inner')

        # Fetch quarterly earnings estimates
        eps_estimates_quarter_df = self.fmp_earnings_estimate_loader.load(symbol_list,
                                                                          period="quarter",
                                                                          num_future_periods=4,
                                                                          min_avg_estimate_percent=0.05)
        if eps_estimates_quarter_df is not None and not eps_estimates_quarter_df.empty:
            eps_estimates_quarter_df.rename(columns={'avg_eps_growth_percent': 'avg_eps_growth_quarter_percent',
                                                     'avg_num_analysts': 'avg_num_analysts_quarter'}, inplace=True)
            merged_df = pd.merge(merged_df, eps_estimates_quarter_df, on='symbol', how='inner')

        # Fetch annual earnings estimates
        eps_estimates_annual_df = self.fmp_earnings_estimate_loader.load(symbol_list,
                                                                         num_future_periods=4,
                                                                         period="annual",
                                                                         min_avg_estimate_percent=0.05)
        if eps_estimates_annual_df is not None and not eps_estimates_annual_df.empty:
            eps_estimates_annual_df.rename(columns={'avg_eps_growth_percent': 'avg_eps_growth_annual_percent',
                                                    'avg_num_analysts': 'avg_num_analysts_annual'}, inplace=True)
            merged_df = pd.merge(merged_df, eps_estimates_annual_df, on='symbol', how='inner')

        # Fetch analyst ratings -> analyst_rating_score
        analyst_ratings_df = self.fmp_analyst_ratings_loader.load(symbol_list, num_lookback_days=60)
        if analyst_ratings_df is not None and not analyst_ratings_df.empty:
            merged_df = pd.merge(merged_df, analyst_ratings_df, on='symbol', how='inner')

        if len(merged_df) == 0:
            print("No matching stocks found")
            return None

        # Copy original columns to preserve their values
        original_columns = merged_df.copy()

        # Normalize columns
        columns_to_normalize = [
            'price_drop_percent',
            'avg_price_target_change_percent',
            'avg_eps_growth_quarter_percent',
            'avg_eps_growth_annual_percent',
            'analyst_rating_score'
        ]
        scaler = MinMaxScaler()
        normalized_columns = pd.DataFrame(scaler.fit_transform(merged_df[columns_to_normalize]),
                                          columns=[f'norm_{col}' for col in columns_to_normalize])

        # Concatenate normalized columns to the original DataFrame
        merged_df = pd.concat([merged_df, normalized_columns], axis=1)

        # Calculate weighted score using the normalized columns
        merged_df['weighted_score'] = (
            merged_df['norm_price_drop_percent'] * 0.2 +
            merged_df['norm_avg_price_target_change_percent'] * 0.4 +
            merged_df['norm_avg_eps_growth_quarter_percent'] * 0.1 +
            merged_df['norm_avg_eps_growth_annual_percent'] * 0.1 +
            merged_df['norm_analyst_rating_score'] * 0.2
        )

        # Restore original columns and include weighted_score
        final_df = pd.concat([original_columns, merged_df['weighted_score']], axis=1)

        # Drop the norm columns
        final_df.drop(columns=[col for col in final_df if col.startswith('norm_')], inplace=True)

        # Sort by weighted score
        final_df = final_df.sort_values(by='weighted_score', ascending=False)

        # Store results
        os.makedirs(RESULTS_DIR, exist_ok=True)
        path = os.path.join(RESULTS_DIR, "candidates.csv")
        final_df.to_csv(path, index=False)

        return final_df
