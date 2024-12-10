from botrading.data_loaders.fmp_data_loader import FmpDataLoader
import pandas as pd
from utils.log_utils import logd, loge
from utils.screener_utils import *
from utils.file_utils import store_csv
from universe_selection.universe_selector import UniverseSelector
from datetime import datetime, timedelta
from report_generators.company_report_generator import CompanyReportGenerator


class OneWeekMomentumScreener:
    def __init__(self, fmp_api_key, openai_api_key):
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)
        self.universe_selector = UniverseSelector(fmp_api_key)
        self.report_generator = CompanyReportGenerator(fmp_api_key, openai_api_key)

    def calculate_momentum_stats(self, symbol_list, prices_dict):
        """
        Calculate weekly momentum statistics for a list of symbols.

        Args:
            symbol_list (list): List of symbols.
            prices_dict (dict): Dictionary with symbol as key and corresponding price data as value.

        Returns:
            pd.DataFrame: DataFrame containing weekly returns for the symbols.
        """
        results = []
        for symbol in symbol_list:
            prices_df = prices_dict.get(symbol)  # Use .get() to avoid KeyError.
            if prices_df is None or prices_df.empty:
                loge(f"No price data available for {symbol}")
                continue

            # Calculate momentum
            try:
                weekly_return = ((prices_df['close'].iloc[-1] - prices_df['close'].iloc[0]) / prices_df['close'].iloc[0]) * 100
                weekly_return = round(weekly_return, 2)
                results.append({'symbol': symbol, 'weekly_return': weekly_return})
            except (IndexError, KeyError) as e:
                loge(f"Error calculating momentum for {symbol}: {str(e)}")
                continue

        # Convert results to DataFrame
        results_df = pd.DataFrame(results)
        return results_df

    def screen_candidates(self):
        """
        Screen candidates based on one-week momentum statistics.

        Returns:
            pd.DataFrame: DataFrame of screened candidates sorted by momentum.
        """
        logd("OneWeekMomentumScreener.screen_candidates")

        # Select universe
        self.universe_selector.perform_selection()
        symbol_list = self.universe_selector.get_symbol_list()
        #symbol_list = symbol_list[:2]  # Limit to top 10 symbols for performance.

        # Define date range
        start_date = datetime.today() - timedelta(days=1)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date = datetime.today()
        end_date_str = end_date.strftime("%Y-%m-%d")

        # Fetch prices
        prices_dict = self.fmp_data_loader.fetch_multiple_daily_prices_by_date(
            symbol_list, start_date_str, end_date_str, cache_data=True, cache_dir=CACHE_DIR
        )

        # Calculate momentum statistics
        momentum_stats_df = self.calculate_momentum_stats(symbol_list, prices_dict)

        # Sort results by highest momentum
        if momentum_stats_df.empty:
            loge("No momentum stats calculated.")
            return pd.DataFrame()

        momentum_stats_df.sort_values(by='weekly_return', ascending=False, inplace=True)

        # Store results to CSV
        store_csv(RESULTS_DIR, "weekly_momentum_results.csv", momentum_stats_df)

        # Generate reports for top symbols
        top_symbols = momentum_stats_df.head(20)['symbol'].tolist()  # Get top 20 symbols by momentum.
        for symbol in top_symbols:
            try:
                reports_dir = os.path.join("reports/one_week_momentum")
                self.report_generator.generate_report(symbol, report_dir=reports_dir)
            except Exception as e:
                loge(f"Error generating report for {symbol}: {str(e)}")

        return momentum_stats_df
