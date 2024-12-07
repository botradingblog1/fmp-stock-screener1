from botrading.data_loaders.fmp_data_loader import FmpDataLoader
import pandas as pd
from utils.log_utils import *
from utils.screener_utils import *
from utils.file_utils import store_csv


class InstitutionalOwnershipScreener:
    def __init__(self, fmp_api_key):
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)

    def screen_candidates(self, symbol_list: list):
        logd(f"InstitutionalOwnershipScreener.screen_candidates")
        combined_stats_df = pd.DataFrame()
        for symbol in symbol_list:
            # Fetch institutional ownership changes
            inst_own_df = self.fmp_data_loader.fetch_institutional_ownership_changes(symbol, cache_data=True, cache_dir=CACHE_DIR)
            if inst_own_df is None or len(inst_own_df) == 0:
                continue

            # Calculate stats
            inst_own_stats_df = calculate_institutional_ownership_stats(symbol, inst_own_df)

            # Combine stats
            combined_stats_df = pd.concat([combined_stats_df, inst_own_stats_df], axis=0, ignore_index=True)

        # Sort by total ratings
        combined_stats_df.sort_values(by=['institutional_investor_score'], ascending=[False])

        # Store results
        store_csv(RESULTS_DIR, INST_OWN_RESULTS_FILE_NAME, combined_stats_df)

        return combined_stats_df
