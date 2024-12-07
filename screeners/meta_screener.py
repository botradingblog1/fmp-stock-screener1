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

    def screen_candidates(self):
        logd(f"MetaScreener.screen_candidates")
        num_top_results = 20

        # Define screener configurations
        screener_configs = [
            (PRICE_TARGET_RESULTS_FILE_NAME, "price_target_screener"),
            (ANALYST_RATINGS_RESULTS_FILE_NAME, "analyst_ratings_screener"),
            (INST_OWN_RESULTS_FILE_NAME, "inst_own_screener")
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
            print(f"Merged {len(results)} screener results.")
        else:
            print("No screener data found. Returning empty DataFrame.")
            merged_df = pd.DataFrame(columns=['symbol', 'screener'])

        # Grab detailed stats for combined results
        symbol_list = merged_df['symbol'].unique()

        # Run individual screeners
        analyst_ratings_results_df = self.analyst_ratings_screener.screen_candidates(symbol_list, min_ratings_count=3)
        analyst_ratings_results_df['screener'] = 'analyst_ratings'

        price_target_results_df = self.price_target_screener.screen_candidates(symbol_list, min_ratings_count=3)
        price_target_results_df['screener'] = 'price_target'

        inst_own_results_df = self.inst_own_screener.screen_candidates(symbol_list)
        price_target_results_df['screener'] = 'institutional_ownership'

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

        # Adjust pe_ratio (invert it since lower is better)
        #merged_df['inv_pe_ratio'] = merged_df['pe_ratio'].replace(0, np.nan)
        #merged_df['inv_pe_ratio'] = 1 / merged_df['inv_pe_ratio']
        #merged_df['inv_pe_ratio'] = merged_df['inv_pe_ratio'].replace(np.nan, 0)

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
            stats_df['norm_total_grades_rating'] * 0.4 +
            stats_df['norm_avg_price_target_change_percent'] * 0.4 +
            stats_df['institutional_investor_score'] * 0.2
        )

        # Sort by weighted score
        stats_df = stats_df.sort_values(by='weighted_score', ascending=False)

        # Drop the norm columns and any intermediate columns
        columns_to_drop = [col for col in stats_df.columns if col.startswith('norm_')]
        stats_df.drop(columns=columns_to_drop, inplace=True)

        # Reset index
        stats_df.reset_index(drop=True, inplace=True)

        # Pick top stocks
        final_stats_df = stats_df.head(20)
        
        # Perform ChatGPT eval
        #final_stats_df = self.perform_chatgpt_eval(stats_df)

        # Sort by ChatGPT rank
        #final_stats_df = final_stats_df.sort_values(by='chatgpt_rank', ascending=False)

        # Store results
        file_name = f"meta_screener_results.csv"
        store_csv(RESULTS_DIR, file_name, final_stats_df)

        return final_stats_df


