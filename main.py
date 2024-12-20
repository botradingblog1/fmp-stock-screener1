from utils.file_utils import *
from utils.file_utils import get_os_variable, delete_files_in_directory
from utils.log_utils import *
from screeners.overvalued_biotech_finder import OvervaluedBioTechFinder
from screeners.one_week_momentum_screener import OneWeekMomentumScreener
from screeners.biggest_winner_screener import BiggestWinnerScreener
from screeners.meta_screener import MetaScreener
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

    # Clean reports
    #delete_files_in_directory(REPORTS_DIR)

    meta_screener = MetaScreener(FMP_API_KEY, OPENAI_API_KEY)
    meta_screener.screen_candidates()

    #screener = BiggestWinnerScreener(FMP_API_KEY, OPENAI_API_KEY)
    #screener.screen_candidates()
    #finder = OvervaluedBioTechFinder(FMP_API_KEY, OPENAI_API_KEY)
    #finder.find_candidates()

    logd("All done!")

    #  Schedule events - to run the script at regular intervals
    schedule_events()

    #  Check schedule
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check time every x seconds


