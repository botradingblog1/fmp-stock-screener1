from utils.file_utils import *
from utils.file_utils import get_os_variable
from utils.log_utils import *
from universe_selection.universe_selector import UniverseSelector
from screeners.analyst_ratings_screener import AnalystRatingsScreener
from screeners.price_target_screener import PriceTargetScreener
from screeners.institutional_ownership_screener import InstitutionalOwnershipScreener
from screeners.meta_screener import MetaScreener
from report_generators.company_report_generator import CompanyReportGenerator
import schedule
import time


# Get API key from environment variables
FMP_API_KEY = get_os_variable('FMP_API_KEY')
TIINGO_API_KEY = get_os_variable('TIINGO_API_KEY')
OPENAI_API_KEY = get_os_variable('OPENAI_API_KEY')


def perform_cleanup():
    # Cleanup log file to avoid excessive growth
    delete_file(CACHE_DIR, LOG_FILE_NAME)


def schedule_events():
    """
    #schedule.every().day.at('01:01').do(run_highest_return_finder)
    #schedule.every().day.at('01:15').do(run_ultimate_finder)
    #schedule.every().day.at('01:30').do(run_inst_own_candidate_finder)
    #schedule.every().day.at('01:45').do(run_blue_chip_bargain_candidate_finder)
    #schedule.every().day.at('01:45').do(run_deep_discount_growth_screener)
    schedule.every().day.at('01:01').do(run_price_target_candidate_finder)
    schedule.every().day.at('01:10').do(run_analyst_ratings_candidate_finder)
    schedule.every().day.at('01:30').do(run_trend_pullback_finder)
    schedule.every().day.at('01:45').do(run_penny_stock_finder)
    schedule.every().day.at('03:30').do(run_news_catalyst_finder)


    schedule.every().sunday.at('01:00').do(perform_cleanup)
    """


if __name__ == "__main__":
    create_output_directories()
    setup_logger(LOG_FILE_NAME)

    # Universe selection
    universe_selector = UniverseSelector(FMP_API_KEY)
    universe_selector.perform_selection()
    symbol_list = universe_selector.get_symbol_list()
    #symbol_list = symbol_list[0:20]

    # Run analyst ratings screener
    analyst_ratings_screener = AnalystRatingsScreener(FMP_API_KEY)
    analyst_ratings_results_df = analyst_ratings_screener.screen_candidates(symbol_list, min_ratings_count=3)

    price_target_screener = PriceTargetScreener(FMP_API_KEY)
    price_target_screener.screen_candidates(symbol_list, min_ratings_count=3)

    inst_own_screener = InstitutionalOwnershipScreener(FMP_API_KEY)
    inst_own_screener.screen_candidates(symbol_list)

    # Run meta screener
    meta_screener = MetaScreener(FMP_API_KEY, OPENAI_API_KEY)
    final_stats_df = meta_screener.screen_candidates()
    if final_stats_df is None or final_stats_df.empty:
        logi("No records returned from final stats")
        exit(0)

    symbol_list = final_stats_df['symbol'].unique()

    # Run report generator
    report_generator = CompanyReportGenerator(FMP_API_KEY, OPENAI_API_KEY)
    for symbol in symbol_list:
        # Generate report
        report_generator.generate_report(symbol)


    logd("All done!")

    #  Schedule events - to run the script at regular intervals
    schedule_events()

    #  Check schedule
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check time every x seconds


