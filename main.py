from utils.file_utils import *
from utils.file_utils import get_os_variable
from utils.log_utils import *
from analysis_tools.ultimate_candidate_finder import UltimateCandidateFinder
from analysis_tools.highest_returns_candidate_finder import HighestReturnsFinder
from analysis_tools.inst_own_candidate_finder import InstOwnCandidateFinder
from analysis_tools.blue_chip_bargain_candidate_finder import BlueChipBargainCandidateFinder
from analysis_tools.etf_performance_screener import EtfPerformanceScreener
from analysis_tools.deep_discount_growth_potential import DeepDiscountGrowthCandidateFinder
from analysis_tools.price_target_candidate_finder import PriceTargetCandidateFinder
from analysis_tools.analyst_ratings_candidate_finder import AnalystRatingsCandidateFinder
from analysis_tools.profile_builder import ProfileBuilder
from analysis_tools.news_catalyst_finder import NewsCatalystFinder
import schedule
import time


# Get API key from environment variables
FMP_API_KEY = get_os_variable('FMP_API_KEY')
TIINGO_API_KEY = get_os_variable('TIINGO_API_KEY')


def run_news_catalyst_finder():
    catalyst_finder = NewsCatalystFinder(TIINGO_API_KEY)
    catalyst_finder.find_catalysts()


def run_profile_finder():
    profile_finder = ProfileBuilder(fmp_api_key=FMP_API_KEY)
    profile_finder.build_profiles()


def run_price_target_candidate_finder():
    finder = PriceTargetCandidateFinder(fmp_api_key=FMP_API_KEY)
    finder.find_candidates()


def run_analyst_ratings_candidate_finder():
    finder = AnalystRatingsCandidateFinder(fmp_api_key=FMP_API_KEY)
    finder.find_candidates()


def run_deep_discount_growth_screener():
    screener = DeepDiscountGrowthCandidateFinder(FMP_API_KEY)
    screener.find_candidates()


def run_etf_performance_screener():
    screener = EtfPerformanceScreener(FMP_API_KEY)
    screener.find_candidates()


def run_blue_chip_bargain_candidate_finder():
    finder = BlueChipBargainCandidateFinder(FMP_API_KEY)
    finder.find_candidates()


def run_inst_own_candidate_finder():
    finder = InstOwnCandidateFinder(FMP_API_KEY)
    finder.find_candidates()


def run_ultimate_finder():
    finder = UltimateCandidateFinder(FMP_API_KEY)
    finder.find_candidates()


def run_highest_return_finder():
    finder = HighestReturnsFinder(FMP_API_KEY)
    finder.find_candidates()


def perform_cleanup():
    # Cleanup log file to avoid excessive growth
    delete_file(CACHE_DIR, LOG_FILE_NAME)


def schedule_events():
    schedule.every().day.at('01:01').do(run_highest_return_finder)
    schedule.every().day.at('01:15').do(run_ultimate_finder)
    schedule.every().day.at('01:30').do(run_inst_own_candidate_finder)
    schedule.every().day.at('01:45').do(run_blue_chip_bargain_candidate_finder)
    schedule.every().day.at('01:45').do(run_deep_discount_growth_screener)
    schedule.every().day.at('02:00').do(run_analyst_ratings_candidate_finder)
    schedule.every().day.at('03:30').do(run_news_catalyst_finder)
    schedule.every().sunday.at('01:00').do(perform_cleanup)


if __name__ == "__main__":
    create_output_directories()
    setup_logger(LOG_FILE_NAME)

    #run_analyst_ratings_candidate_finder()
    run_news_catalyst_finder()
    """

    run_deep_discount_growth_screener()
    run_inst_own_candidate_finder()
    run_highest_return_finder()
    """

    logd("All done!")

    #  Schedule events - to run the script at regular intervals
    schedule_events()

    #  Check schedule
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check time every x seconds


