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

# Weights for B/O Screener Score - should add up to 1.0
momentum_weight = 0.1
growth_weight = 0.2
quality_weight = 0.1
analyst_ratings_weight = 0.15
dividend_yield_weight = 0.2
social_sentiment_weight = 0.05
news_sentiment_weight = 0.1


