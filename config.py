import os
from enums import *

# Define directory paths
CACHE_DIR = 'cache'
RESULTS_DIR = 'results'
LOG_DIR = 'logs'
LOG_FILE_NAME = "stock-screener1-log.txt"

# Variables for coarse selection
EXCHANGE_LIST = "nyse,nasdaq,amex"
MIN_MARKET_CAP = 100 * 10000000  # billion
MIN_PRICE = 10  # dollars
MAX_BETA = 2.0
MIN_VOLUME = 100000  # Average daily trading volume
COUNTRY = 'US'
STOCK_LIST_LIMIT = 3000

# Config variables
FMP_CALLS_PER_MINUTE = 300
API_REQUEST_DELAY = 60 / FMP_CALLS_PER_MINUTE
RISK_FREE_RATE = 0.015  # Annualized average risk-free rate (3-month T-bill average)
ROUND_PRECISION = 4  # Precision for rounding values, number of placed after decimal point

# Screener criteria
MIN_PRICE = 5.0  # Minimum price a security should have
MIN_MOMENTUM_FACTOR = 0.0  # Minimum momentum factor value
MIN_GROWTH_FACTOR = 0.0  # Minimum growth factor value
MIN_DIVIDEND_YIELD = 0.0  # Minimum average dividend yield
MIN_QUALITY_SCORE = 0.0  # Minimum quality score
MIN_ANALYST_RATINGS_SCORE = 0.0   # Minimum average analyst ratings score

# Cap values
OUTLIER_STD_MULTIPLIER = 3  # Number of standard deviations used to deal with outliers
MAX_MOMENTUM_CAP = 1.0  # Cap at 100%
MAX_GROWTH_CAP = 1.0  # Cap at 100%

# Weights for B/O Screener Score - should add up to 1.0
MOMENTUM_WEIGHT = 0.2
GROWTH_WEIGHT = 0.2
QUALITY_WEIGHT = 0.10
ANALYST_RATINGS_WEIGHT = 0.25
DIVIDEND_YIELD_WEIGHT = 0.2
SOCIAL_SENTIMENT_WEIGHT = 0.00
NEWS_SENTIMENT_WEIGHT = 0.05


