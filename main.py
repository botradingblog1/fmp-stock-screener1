from utils.file_utils import *
from utils.file_utils import get_os_variable
from utils.log_utils import *
from analysis_tools.ultimate_candidate_finder import UltimateCandidateFinder
from analysis_tools.highest_returns_candidate_finder import HighestReturnsFinder
from analysis_tools.inst_own_candidate_finder import InstOwnCandidateFinder
import schedule


# Get API key from environment variables
FMP_API_KEY = get_os_variable('FMP_API_KEY')


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
    schedule.every().sunday.at('01:15').do(run_ultimate_finder())
    schedule.every().sunday.at('01:01').do(run_highest_return_finder())
    schedule.every().sunday.at('01:00').do(perform_cleanup)


if __name__ == "__main__":
    create_output_directories()
    setup_logger(LOG_FILE_NAME)

    run_inst_own_candidate_finder()

    """
    #  Schedule events - to run the script at regular intervals
    schedule_events()

    #  Check schedule
    while True:
        schedule.run_pending()
        time.sleep(10)  # Check time every x seconds
    """

