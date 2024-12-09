from botrading.data_loaders.fmp_data_loader import FmpDataLoader
import pandas as pd
from utils.log_utils import *
from utils.screener_utils import *
from utils.file_utils import store_csv
import numpy as np


class EstimatedEpsScreener:
    def __init__(self, fmp_api_key):
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)

    def screen_candidates(self, symbol_list: list):
        logd("Starting EstimatedEpsScreener.screen_candidates")
        results = []

        for symbol in symbol_list:
            # Fetch analyst earnings estimates
            estimates_df = self.fmp_data_loader.fetch_analyst_earnings_estimates(
                symbol, period="annual", limit=100
            )

            if estimates_df is None or estimates_df.empty:
                logd(f"No estimates available for {symbol}")
                continue

            # Filter data to include only the last 90 days
            cutoff_date = pd.Timestamp.now()
            estimates_df['date'] = pd.to_datetime(estimates_df['date'], errors='coerce')
            estimates_df = estimates_df[
                (estimates_df['date'] >= cutoff_date) & estimates_df['date'].notna()
                ]

            if estimates_df.empty:
                logd(f"No recent estimates available for {symbol}")
                continue

            # Calculate average estimated EPS change
            estimates_df['avg_estimated_eps_change'] = estimates_df['estimatedEpsAvg'].pct_change() * 100
            avg_estimated_eps_change = estimates_df['avg_estimated_eps_change'].mean(skipna=True)

            # Append stats to results
            row = {'symbol': symbol, 'avg_estimated_eps_change': avg_estimated_eps_change}
            results.append(row)

        # Convert results to DataFrame
        results_df = pd.DataFrame(results)

        if results_df.empty:
            logw("No results found during screening")
            return results_df

        # Handle infinity values
        results_df.replace([np.inf, -np.inf], np.nan, inplace=True)

        # Sort by average estimated EPS change in descending order
        results_df.sort_values(by='avg_estimated_eps_change', ascending=False, inplace=True)

        # Store results
        store_csv(RESULTS_DIR, ESTIMATED_EPS_FILE_NAME, results_df)
        logi(f"Screening results stored in {RESULTS_DIR}/{ESTIMATED_EPS_FILE_NAME}")

        return results_df
