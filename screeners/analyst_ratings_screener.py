from botrading.data_loaders.fmp_data_loader import FmpDataLoader
import pandas as pd
from utils.log_utils import *
from utils.screener_utils import *
from utils.file_utils import store_csv


class AnalystRatingsScreener:
    def __init__(self, fmp_api_key, ):
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)

    def screen_candidates(self, symbol_list: list, min_ratings_count: int = 3):
        logd(f"AnalystRatingsScreener.screen_candidates")
        combined_grades_df = pd.DataFrame()
        for symbol in symbol_list:
            # Fetch analyst ratings -> analyst_rating_score
            grades_df = self.fmp_data_loader.fetch_analyst_ratings(symbol,
                                                                   cache_data=True,
                                                                   cache_dir=CACHE_DIR)
            if grades_df is None or grades_df.empty:
                #logd(f"No grades available for {symbol}")
                continue

            # Filter out data more than x months in the past
            cutoff_date = pd.Timestamp.now() - pd.DateOffset(days=90)
            grades_df = grades_df[(grades_df['date'] >= cutoff_date) & grades_df['date'].notna()]

            # Aggregate analyst ratings
            grade_stats_df = aggregate_analyst_grades(symbol, grades_df)

            # Combine grade stats
            combined_grades_df = pd.concat([combined_grades_df, grade_stats_df], axis=0, ignore_index=True)

        # Filter by minimum analyst grades
        combined_grades_df = combined_grades_df[combined_grades_df['total_ratings_count'] >= min_ratings_count]

        # Sort by total buy ratings
        combined_grades_df.sort_values(by=['total_grades_rating'], ascending=[False])

        # Store results
        store_csv(RESULTS_DIR, ANALYST_RATINGS_RESULTS_FILE_NAME, combined_grades_df)

        return combined_grades_df
