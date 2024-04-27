import os
from enums import *

# Define directory paths
CACHE_DIR = 'cache'
RESULTS_DIR = 'results'
LOG_DIR = 'logs'

# Data refresh intervals
stock_list_refresh_interval = DataRefreshInterval.WEEKLY
price_data_refresh_interval = DataRefreshInterval.DAILY
analyst_ratings_refresh_interval = DataRefreshInterval.WEEKLY
fundamentals_refresh_interval = DataRefreshInterval.WEEKLY

# Config variables
fmp_calls_per_minute = 300
api_request_delay = 60 / fmp_calls_per_minute

