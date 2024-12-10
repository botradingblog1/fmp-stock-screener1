import pandas as pd
import numpy as np
import os
from universe_selection.universe_selector import UniverseSelector
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
from screeners.analyst_ratings_screener import AnalystRatingsScreener
from screeners.price_target_screener import PriceTargetScreener
from screeners.estimated_eps_screener import EstimatedEpsScreener
from screeners.institutional_ownership_screener import InstitutionalOwnershipScreener
from report_generators.company_report_generator import CompanyReportGenerator
from config import *
from datetime import datetime
from utils.file_utils import *
from sklearn.preprocessing import MinMaxScaler
from ai_clients.openai_client import OpenAiClient
from utils.log_utils import *
from utils.fmp_utils import *
import json


class MetaScreener:
    def __init__(self, fmp_api_key: str, openai_api_key: str):
        self.universe_selector = UniverseSelector(fmp_api_key)
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)
        self.analyst_ratings_screener = AnalystRatingsScreener(fmp_api_key)
        self.price_target_screener = PriceTargetScreener(fmp_api_key)
        self.estimated_eps_screener = EstimatedEpsScreener(fmp_api_key)
        self.inst_own_screener = InstitutionalOwnershipScreener(fmp_api_key)
        self.openai_client = OpenAiClient(openai_api_key)
        self.report_generator = CompanyReportGenerator(fmp_api_key, openai_api_key)

    def load_and_process_results(self, file_name: str, screener_name: str, num_top_results: int) -> pd.DataFrame:
        """
        Load and process results from a CSV file.

        :param file_name: The name of the file to load.
        :param screener_name: The name of the screener.
        :param num_top_results: Number of top results to retain.
        :return: Processed DataFrame or None if the file doesn't exist.
        """
        file_path = os.path.join(RESULTS_DIR, file_name)
        if not os.path.exists(file_path):
            print(f"Warning: File not found: {file_path}")
            return None

        try:
            df = pd.read_csv(file_path)
            df = df.head(num_top_results)
            df['screener'] = screener_name
            return df[['symbol', 'screener']]
        except Exception as e:
            print(f"Error loading file {file_path}: {e}")
            return None

    def perform_chatgpt_eval(self, stats_df: pd.DataFrame):
        # Pick columns
        df = stats_df.copy()
        df = df[['symbol', 'total_grades_rating', 'avg_price_target_change', 'institutional_investor_score']]
        df.rename(columns={'total_grades_rating': 'total_analyst_grades_rating'}, inplace=True)

        # Convert stats_df to JSON
        stats_json = df.to_json(orient="records")

        role = "Financial Analyst"
        prompt = (
            "Using the provided ratings as well as your background knowledge about the stock, identify the top stock picks "
            "with the highest expected price return for the next year. The evaluation should be based on a 60/40 balance between "
            "the provided ratings and your expertise. For each stock, provide a clear reasoning for the selection. "
            "The output must be a parsable JSON array in the following format: "
            "[{'symbol': '<symbol>', 'chatgpt_rank': '<ChatGPT rank (0 - 1.0)>', 'reasoning': '<reason for selection>'}]. "
            f"Ratings: {stats_json}"
        )

        response = self.openai_client.query(prompt, role, cache_data=False, cache_dir=CACHE_DIR)
        # Try to parse the response
        try:
            response_json = json.loads(response)  # Parse the response JSON

            # Convert response JSON to a DataFrame
            response_df = pd.DataFrame(response_json)

            # Add or update `chatgpt_rank` and `reasoning` columns in stats_df
            stats_df = stats_df.set_index("symbol")  # Set symbol as index for efficient updates
            response_df = response_df.set_index("symbol")  # Ensure both have the same index for alignment

            stats_df["chatgpt_rank"] = response_df["chatgpt_rank"]
            stats_df["reasoning"] = response_df["reasoning"]

            stats_df.reset_index(inplace=True)

        except Exception as ex:
            loge(f"Error parsing response: {response}, {str(ex)}")
            stats_df["chatgpt_rank"] = 0
            stats_df["reasoning"] = ''
            # Optionally handle the error by returning stats_df without the merge
            return stats_df

        return stats_df

    def perform_economic_moat_analysis(self, symbol_list: list):
        """
        Analyze economic moat factors for a given company based on its description.

        :param symbol: Stock symbol of the company.
        :param company_description: Detailed description of the company.
        :return: DataFrame with economic moat factors, or None if an error occurs.
        """

        combined_results_df = pd.DataFrame()
        for symbol in symbol_list:
            # Fetch company outlook from FMP
            company_outlook_data = self.fmp_data_loader.fetch_company_outlook(symbol)
            if not company_outlook_data:
                loge(f"No data returned for symbol {symbol}")
                continue

            # Parse sections
            profile_data = company_outlook_data.get('profile', {})
            company_description = profile_data.get('description', '')
            if company_description is None or company_description == '':
                continue

            role = "Financial Analyst"
            prompt = (
                "Using the provided company description and your background knowledge, "
                "analyze economic moat factors on a scale between 1 and 10. "
                "The output must be a parsable JSON array in the following format: "
                "["
                "{"
                "\"symbol\": \"<symbol>\", "
                "\"first_in_class_product\": <1 to 10>, "
                "\"global_target_market\": <1 to 10>, "
                "\"platform_solution\": <1 to 10>, "
                "\"future_growth_markets\": <1 to 10>, "
                "\"large_income_changes\": <1 to 10>, "
                "\"network_effects\": <1 to 10>, "
                "\"high_switching_costs\": <1 to 10>, "
                "\"increasing_income_streams\": <1 to 10>, "
                "\"patented_technologies\": <1 to 10>"
                "}"
                "] "
                f"Symbol: {symbol}, Description: {company_description}"
            )

            try:
                # Query the OpenAI client for analysis
                response = self.openai_client.query(prompt, role, cache_data=False, cache_dir=CACHE_DIR)

                # Try to parse the response into JSON
                response_json = json.loads(response)

                # Validate that the response contains the expected keys
                required_keys = [
                    "symbol", "first_in_class_product", "global_target_market",
                    "platform_solution", "future_growth_markets", "large_income_changes",
                    "network_effects", "high_switching_costs", "increasing_income_streams",
                    "patented_technologies"
                ]
                missing_key = False
                for key in required_keys:
                    if key not in response_json[0]:
                        logw(f"Missing key '{key}' in the response JSON.")
                        missing_key = True
                        break
                if missing_key:
                    continue
                # Convert the parsed JSON into a DataFrame
                response_df = pd.DataFrame(response_json)

                # Calculate total economic moat score
                response_df['economic_moat_score'] = round((response_df['first_in_class_product'] + \
                                                     response_df['global_target_market'] + \
                                                     response_df['platform_solution'] + \
                                                     response_df['future_growth_markets'] + \
                                                     response_df['large_income_changes'] + \
                                                     response_df['network_effects'] + \
                                                     response_df['high_switching_costs'] + \
                                                     response_df['increasing_income_streams'] + \
                                                     response_df['patented_technologies']) / 9, 2)

                    # Combine results
                combined_results_df = pd.concat([combined_results_df, response_df], axis=0, ignore_index=True)

            except json.JSONDecodeError as e:
                loge(f"JSON decoding error: {response}, {str(e)}")
            except Exception as e:
                loge(f"Error parsing response: {response}, {str(e)}")
        return combined_results_df

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
            #if income_df is None or income_df.empty or len(income_df) < 2:
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
        self.universe_selector.perform_selection(industry_list=BIOTECH_INDUSTRY_LIST)
        symbol_list = self.universe_selector.get_symbol_list()
        #symbol_list = symbol_list[0:20]

        # Run price target screener
        price_target_results_df = self.price_target_screener.screen_candidates(symbol_list, min_ratings_count=2)
        price_target_results_df = price_target_results_df[price_target_results_df['avg_price_target_change'] >= 28.0]
        symbol_list = price_target_results_df['symbol'].unique()

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
        stats_df = price_target_results_df.merge(analyst_ratings_results_df, on='symbol', how='left')
        stats_df = stats_df.merge(quarterly_revenue_growth_df, on='symbol', how='left')
        stats_df = stats_df.merge(estimated_revenue_df, on='symbol', how='left')
        stats_df = stats_df.merge(inst_own_results_df, on='symbol', how='left')
        stats_df = stats_df.merge(combined_ratios_df, on='symbol', how='left')

        # Handle missing values
        stats_df = stats_df.fillna(0)

        # Filter minimums
        stats_df = stats_df[stats_df['avg_quarterly_revenue_growth'] >= 2.0]
        stats_df = stats_df[stats_df['bullish_count'] >= 0]
        stats_df = stats_df[stats_df['investors_put_call_ratio'] < 1.0]
        stats_df = stats_df[stats_df['price_earnings_ratio'] <= 50.0]

        # Perform economic moat factor analysis
        """
        economic_moat_factors_df = self.perform_economic_moat_analysis(symbol_list)
        if economic_moat_factors_df is not None and len(economic_moat_factors_df) > 0:
            # merge with stats_df
            stats_df = stats_df.merge(economic_moat_factors_df, on='symbol', how='left')
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
        stats_df = stats_df.sort_values(by='weighted_score', ascending=False)

        # Drop the norm columns and any intermediate columns
        columns_to_drop = [col for col in stats_df.columns if col.startswith('norm_')]
        stats_df.drop(columns=columns_to_drop, inplace=True)

        # Reset index
        stats_df.reset_index(drop=True, inplace=True)

        # Pick columns
        stats_df = stats_df[['symbol', 'price_earnings_ratio', 'avg_quarterly_revenue_growth', 'avg_estimated_revenue_change',
                             'avg_revenue_estimate_analysts', 'bullish_count', 'hold_count', 'avg_price_target_change', 'num_price_target_analysts',
                             'investors_holding', 'investors_holding_change',
                             'investors_put_call_ratio', 'weighted_score']]

        # Pick top stocks
        stats_df = stats_df.head(50)

        # Store results
        file_name = f"meta_screener_results.csv"
        store_csv(RESULTS_DIR, file_name, stats_df)

        symbol_list = stats_df['symbol'].unique()

        # Run report generator
        for symbol in symbol_list:
            # Generate report
            self.report_generator.generate_report(symbol, reports_dir=REPORTS_DIR)

        return stats_df
