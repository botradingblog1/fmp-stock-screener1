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
from analysis_tools.trend_pullback_candidate_finder import TrendPullbackFinder
from analysis_tools.penny_stock_candidate_finder import PennyStockFinder
from analysis_tools.overvalued_stock_candidate_finder import OvervaluedStockCandidateFinder
from analysis_tools.value_stock_candidate_finder import ValueStockCandidateFinder
from analysis_tools.profile_builder import ProfileBuilder
from analysis_tools.market_segment_growth_candidate_finder import MarketSegmentGrowthCandidateFinder
from analysis_tools.news_catalyst_finder import NewsCatalystFinder
from analysis_tools.market_player_stats_fetcher import MarketLeaderStatsFetcher
from analysis_tools.estimated_growth_candidate_finder import  EstimatedGrowthCandidateFinder
import schedule
import time


# Get API key from environment variables
FMP_API_KEY = get_os_variable('FMP_API_KEY')
TIINGO_API_KEY = get_os_variable('TIINGO_API_KEY')


def run_market_leader_stats_fetcher():
    fetcher = MarketLeaderStatsFetcher(fmp_api_key=FMP_API_KEY)
    fetcher.fetch_stats()


def run_market_segment_growth_stock_finder():
    market_segment_growth_stock_finder = MarketSegmentGrowthCandidateFinder(FMP_API_KEY)
    market_segment_growth_stock_finder.find_candidates()


def run_penny_stock_finder():
    penny_stock_finder = PennyStockFinder(FMP_API_KEY)
    penny_stock_finder.find_candidates()


def run_overvalued_stock_finder():
    candidate_finder = OvervaluedStockCandidateFinder(FMP_API_KEY)
    candidate_finder.find_candidates()


def run_value_stock_finder():
    candidate_finder = ValueStockCandidateFinder(FMP_API_KEY)
    candidate_finder.find_candidates()

def run_trend_pullback_finder():
    trend_pullback_finder = TrendPullbackFinder(TIINGO_API_KEY)
    trend_pullback_finder.find_trend_pullbacks()

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


def run_estimated_growth_candidate_finder():
    finder = EstimatedGrowthCandidateFinder(FMP_API_KEY)
    finder.find_candidates()


def perform_cleanup():
    # Cleanup log file to avoid excessive growth
    delete_file(CACHE_DIR, LOG_FILE_NAME)


def schedule_events():
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


if __name__ == "__main__":
    create_output_directories()
    setup_logger(LOG_FILE_NAME)

    run_price_target_candidate_finder()
    #run_estimated_growth_candidate_finder()
    #run_market_leader_stats_fetcher()
    #run_market_segment_growth_stock_finder()
    #run_penny_stock_finder()
    #run_overvalued_stock_finder()
    #run_value_stock_finder()
    #run_etf_performance_screener()
    #run_inst_own_candidate_finder()
    #run_trend_pullback_finder()
    #run_news_catalyst_finder()

    #run_analyst_ratings_candidate_finder()
    #run_news_catalyst_finder()
    #run_deep_discount_growth_screener()
    """


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


