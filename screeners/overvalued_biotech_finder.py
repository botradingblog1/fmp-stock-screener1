import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from utils.log_utils import *
from utils.report_utils import *
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
from screeners.price_target_screener import PriceTargetScreener
from screeners.institutional_ownership_screener import InstitutionalOwnershipScreener
from report_generators.company_report_generator import CompanyReportGenerator
from universe_selection.universe_selector import UniverseSelector
import os

USE_INSTITUTIONAL_OWNERSHIP_API = True







class OvervaluedBioTechFinder:
    def __init__(self, fmp_api_key: str, openai_api_key: str):
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)
        self.universe_selector = UniverseSelector(fmp_api_key)
        self.price_target_screener = PriceTargetScreener(fmp_api_key)
        self.inst_own_screener = InstitutionalOwnershipScreener(fmp_api_key)
        self.report_generator = CompanyReportGenerator(fmp_api_key, openai_api_key)

    def check_near_fifty_two_week_high(self, symbol_list: list, prices_dict: dict):
        """
        Check if the current price of each symbol is near its 52-week high.

        Args:
            symbol_list (list): List of stock symbols.
            prices_dict (dict): Dictionary with symbols as keys and corresponding price data as pandas DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing symbols and a boolean indicating if the current price is near the 52-week high.
        """
        results = []

        for symbol in symbol_list:
            # Ensure the symbol exists in prices_dict
            prices_df = prices_dict.get(symbol)
            if prices_df is None or prices_df.empty:
                loge(f"No price data available for {symbol}")
                continue

            # Calculate the 52-week high
            try:
                fifty_two_week_high = prices_df['close'].max()
                current_price = prices_df['close'].iloc[-1]
                near_fifty_two_week_high = current_price >= (fifty_two_week_high * 0.9)  # Within 90% of the max price

                result_row = {
                    'symbol': symbol,
                    'current_price': current_price,
                    'fifty_two_week_high': fifty_two_week_high,
                    'near_fifty_two_week_high': near_fifty_two_week_high
                }
                results.append(result_row)
            except Exception as e:
                loge(f"Error processing {symbol}: {str(e)}")
                continue

        # Convert results to DataFrame
        results_df = pd.DataFrame(results)
        return results_df

    def find_candidates(self):
        logi("Finding overvalued biotech candidates...")

        # Load stock screener results
        self.universe_selector.perform_selection(industry_list=BIOTECH_INDUSTRY_LIST)
        symbol_list = self.universe_selector.get_symbol_list()

        # Sample a few symbols for demonstration
        #symbol_list = symbol_list[:3]

        # Define date range
        start_date = datetime.today() - timedelta(days=365)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date = datetime.today()
        end_date_str = end_date.strftime("%Y-%m-%d")

        # Fetch prices
        prices_dict = self.fmp_data_loader.fetch_multiple_daily_prices_by_date(
            symbol_list, start_date_str, end_date_str, cache_data=True, cache_dir=CACHE_DIR
        )

        # Check near 52-week high
        candidates_df = self.check_near_fifty_two_week_high(symbol_list, prices_dict)
        symbol_list = candidates_df['symbol'].unique()

        # Filter where prices near 52-week high
        candidates_df = candidates_df[candidates_df['near_fifty_two_week_high'] == True]

        price_target_results_df = self.price_target_screener.screen_candidates(symbol_list, min_ratings_count=0)

        inst_own_results_df = self.inst_own_screener.screen_candidates(symbol_list)

        # Merge all detailed results on symbol
        if price_target_results_df is not None:
            candidates_df = candidates_df.merge(price_target_results_df, on='symbol', how='left')
            #candidates_df = candidates_df[candidates_df['avg_price_target_change_percent'] <= -10]

        if inst_own_results_df is not None:
            candidates_df = candidates_df.merge(inst_own_results_df, on='symbol', how='left')
            candidates_df = candidates_df[candidates_df['investors_put_call_ratio'] > 1.0]

        # Handle missing values
        candidates_df = candidates_df.fillna(0)

        # Sort by avg price target
        if 'investors_put_call_ratio' in candidates_df.columns:
            candidates_df.sort_values(by=['investors_put_call_ratio'], ascending=[False], inplace=True)

        # Pick top candidates
        top_candidates_df = candidates_df.head(20)
        if top_candidates_df.empty:
            logi(f"No candidates found for overvalued biotech screener")
            return

        # Store candidates
        os.makedirs(REPORTS_DIR, exist_ok=True)
        file_name = f"overvalued_biotech_stocks.csv"
        path = os.path.join(RESULTS_DIR, file_name)
        top_candidates_df.to_csv(path)

        # Generate reports
        symbol_list = top_candidates_df['symbol'].unique()
        reports_dir = os.path.join(REPORTS_DIR, "overvalued_biotech")
        os.makedirs(reports_dir, exist_ok=True)
        for symbol in symbol_list:
            self.report_generator.generate_report(symbol, report_dir=reports_dir)

        logi("Done with overvalued small caps analysis.")
