import pandas as pd
import os
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
from screeners.analyst_ratings_screener import AnalystRatingsScreener
from screeners.price_target_screener import PriceTargetScreener
from screeners.institutional_ownership_screener import InstitutionalOwnershipScreener
from config import *
from datetime import datetime
from utils.file_utils import *
from sklearn.preprocessing import MinMaxScaler
from ai_clients.openai_client import OpenAiClient
from utils.log_utils import *
import json


class MetaScreener:
    def __init__(self, fmp_api_key: str, openai_api_key: str):
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)
        self.analyst_ratings_screener = AnalystRatingsScreener(fmp_api_key)
        self.price_target_screener = PriceTargetScreener(fmp_api_key)
        self.inst_own_screener = InstitutionalOwnershipScreener(fmp_api_key)
        self.openai_client = OpenAiClient(openai_api_key)

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
        df = df[['symbol', 'total_grades_rating', 'avg_price_target_change_percent', 'institutional_investor_score']]
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

                # Combine results
                combined_results_df = pd.concat([combined_results_df, response_df], axis=0, ignore_index=True)

            except json.JSONDecodeError as e:
                loge(f"JSON decoding error: {response}, {str(e)}")
            except Exception as e:
                loge(f"Error parsing response: {response}, {str(e)}")
        return combined_results_df


    def screen_candidates(self):
        logd(f"MetaScreener.screen_candidates")
        num_top_results = 20

        # Define screener configurations
        screener_configs = [
            (PRICE_TARGET_RESULTS_FILE_NAME, "price_target_screener"),
            (ANALYST_RATINGS_RESULTS_FILE_NAME, "analyst_ratings_screener"),
            (ESTIMATED_EPS_FILE_NAME, "estimated_eps_screener")
        ]

        # Load and process each screener
        results = []
        for file_name, screener_name in screener_configs:
            df = self.load_and_process_results(file_name, screener_name, num_top_results)
            if df is not None:
                results.append(df)

        # Merge all results
        if results:
            merged_df = pd.concat(results, axis=0, ignore_index=True)
            #print(f"Merged {len(results)} screener results.")
        else:
            logd("No screener data found. Returning empty DataFrame.")
            merged_df = pd.DataFrame(columns=['symbol', 'screener'])

        # Grab detailed stats for combined results
        symbol_list = merged_df['symbol'].unique()

        # Run individual screeners
        analyst_ratings_results_df = self.analyst_ratings_screener.screen_candidates(symbol_list, min_ratings_count=3)
        price_target_results_df = self.price_target_screener.screen_candidates(symbol_list, min_ratings_count=3)
        inst_own_results_df = self.inst_own_screener.screen_candidates(symbol_list)

        # Merge all detailed results on symbol
        stats_df = merged_df[['symbol']].drop_duplicates()
        if analyst_ratings_results_df is not None:
            stats_df = stats_df.merge(analyst_ratings_results_df, on='symbol', how='left')
        if price_target_results_df is not None:
            stats_df = stats_df.merge(price_target_results_df, on='symbol', how='left')
        if inst_own_results_df is not None:
            stats_df = stats_df.merge(inst_own_results_df, on='symbol', how='left')

        # Handle missing values
        stats_df = stats_df.fillna(0)

        # Filter minimums
        stats_df = stats_df[stats_df['avg_price_target_change_percent'] >= 20.0]
        stats_df = stats_df[stats_df['investors_put_call_ratio'] < 1.0]

        # Perform economic moat factor analysis
        symbol_list = stats_df['symbol'].unique()
        economic_moat_factors_df = self.perform_economic_moat_analysis(symbol_list)
        if economic_moat_factors_df is not None and len(economic_moat_factors_df) > 0:
            # merge with stats_df
            stats_df = stats_df.merge(economic_moat_factors_df, on='symbol', how='left')

        # Normalize columns
        columns_to_normalize = [
            'total_grades_rating',
            'avg_price_target_change_percent',
            'institutional_investor_score'
        ]
        scaler = MinMaxScaler()
        normalized_data = scaler.fit_transform(stats_df[columns_to_normalize])
        normalized_columns_df = pd.DataFrame(normalized_data, columns=[f'norm_{col}' for col in columns_to_normalize])

        # Concatenate normalized columns to the original DataFrame
        stats_df = pd.concat([stats_df.reset_index(drop=True), normalized_columns_df], axis=1)

        # Calculate weighted score using the normalized columns
        stats_df['weighted_score'] = (
            stats_df['norm_avg_price_target_change_percent'] * 0.6 +
            stats_df['norm_total_grades_rating'] * 0.2 +
            stats_df['institutional_investor_score'] * 0.2
        )

        # Sort by weighted score
        stats_df = stats_df.sort_values(by='weighted_score', ascending=False)

        # Drop the norm columns and any intermediate columns
        columns_to_drop = [col for col in stats_df.columns if col.startswith('norm_')]
        stats_df.drop(columns=columns_to_drop, inplace=True)

        # Reset index
        stats_df.reset_index(drop=True, inplace=True)

        # Pick columns
        stats_df = stats_df[['symbol', 'strong_buy_count','buy_count','hold_count','avg_price_target_change_percent',
                             'num_price_target_analysts','investors_holding','investors_holding_change',
                             'investors_put_call_ratio', 'investors_put_call_ratio_change','weighted_score',
                             'first_in_class_product','global_target_market','platform_solution',
                             'future_growth_markets', 'large_income_changes', 'network_effects',
                             'high_switching_costs','increasing_income_streams', 'patented_technologies']]

        # Pick top stocks
        stats_df = stats_df.head(100)

        # Store results
        file_name = f"meta_screener_results.csv"
        store_csv(RESULTS_DIR, file_name, stats_df)

        return stats_df


