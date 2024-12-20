from botrading.data_loaders.fmp_data_loader import FmpDataLoader
import pandas as pd
from utils.log_utils import *
from utils.screener_utils import *
from utils.file_utils import store_csv


class PriceTargetScreener:
    def __init__(self, fmp_api_key):
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)

    def screen_candidates(self, symbol_list: list, min_ratings_count: int = 3):
        logd(f"PriceTargetScreener.screen_candidates")
        combined_stats_df = pd.DataFrame()
        for symbol in symbol_list:
            # Fetch price targets
            price_targets_df = self.fmp_data_loader.fetch_price_targets(symbol,
                                                                        cache_data=False,
                                                                        cache_dir=CACHE_DIR)
            if price_targets_df is None or price_targets_df.empty:
                #logd(f"No grades available for {symbol}")
                continue

            # Filter out data more than x months in the past
            cutoff_date = pd.Timestamp.now() - pd.DateOffset(days=120)
            price_targets_df = price_targets_df[(price_targets_df['publishedDate'] >= cutoff_date) & price_targets_df['publishedDate'].notna()]
            if price_targets_df is None or price_targets_df.empty:
                #logd(f"No grades available for {symbol}")
                continue

            # Calculate stats
            price_target_stats_df = calculate_price_target_stats(symbol, price_targets_df)

            # Combine stats
            combined_stats_df = pd.concat([combined_stats_df, price_target_stats_df], axis=0, ignore_index=True)

        # Filter by minimum analyst ratings
        if combined_stats_df.empty:
            logi(f"No stats returned in price target screener")
            return None
        combined_stats_df = combined_stats_df[combined_stats_df['num_price_target_analysts'] >= min_ratings_count]

        # Sort by total buy ratings
        combined_stats_df.sort_values(by=['avg_price_target_change'], ascending=[False], inplace=True)

        # Store results
        store_csv(RESULTS_DIR, PRICE_TARGET_RESULTS_FILE_NAME, combined_stats_df)

        return combined_stats_df
