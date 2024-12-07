import os
from enums import *

# Define directory paths
CACHE_DIR = 'cache'
RESULTS_DIR = 'results'
REPORTS_DIR = 'reports'
TEMPLATE_DIR = 'templates'
LOG_DIR = 'logs'
LOG_FILE_NAME = "stock-screener1-log.txt"

# File names
ANALYST_RATINGS_RESULTS_FILE_NAME = "analyst_ratings_results.csv"
PRICE_TARGET_RESULTS_FILE_NAME = "price_target_results.csv"
INST_OWN_RESULTS_FILE_NAME = "institutional_ownership_results.csv"

# EDGAR config
EDGAR_IDENTITY = "Chris S. - botradingblog@gmail.com"

# Stock screener config
EXCHANGE_LIST = "nyse,nasdaq,amex"
PRICE_MORE_THAN = 1.0  # Don't wanna trade below a buck
PRICE_LESS_THAN = 1000.0  # Am I Buffett, or what???
MARKET_CAP_LOWER_THAN = 5 * 1000000000  # Upper limit: Small/Medium cap companies
VOLUME_MORE_THAN = 5000  # Need a bit of liquidity to trade
COUNTRY = "US"
STOCK_SCREENER_LIMIT = 3000

# Config variables
DAILY_DATA_FETCH_PERIODS = 500
NEWS_ARTICLE_LIMIT = 50
FMP_CALLS_PER_MINUTE = 1000
API_REQUEST_DELAY = 60 / FMP_CALLS_PER_MINUTE
RISK_FREE_RATE = 0.015  # Annualized average risk-free rate (3-month T-bill average)
ROUND_PRECISION = 4  # Precision for rounding values, number of placed after decimal point

# Screener criteria
MIN_PRICE = 5.0  # Minimum price a security should have
MIN_MOMENTUM_FACTOR = 0  # Minimum momentum factor value
MIN_GROWTH_FACTOR = 0  # Minimum growth factor value
MIN_DIVIDEND_YIELD = 0  # Minimum average dividend yield
MIN_QUALITY_FACTOR = 0  # Minimum quality factor
MIN_ANALYST_RATINGS_SCORE = 1.0   # Minimum average analyst ratings score

# Cap values
OUTLIER_STD_MULTIPLIER = 3  # Number of standard deviations used to deal with outliers
MAX_MOMENTUM_CAP = 1.0  # Cap at 100%
MAX_GROWTH_CAP = 1.0  # Cap at 100%

# Weights for B/O Screener Score - should add up to 1.0
short_term_investment_profile = {
    "MOMENTUM_WEIGHT": 0.5,
    "GROWTH_WEIGHT": 0.05,
    "QUALITY_WEIGHT": 0.05,
    "ANALYST_RATINGS_WEIGHT": 0.2,
    "DIVIDEND_YIELD_WEIGHT": 0.0,
    "SOCIAL_SENTIMENT_WEIGHT": 0.0,
    "NEWS_SENTIMENT_WEIGHT": 0.2,
}

long_term_investment_profile = {
    "MOMENTUM_WEIGHT": 0.0,
    "GROWTH_WEIGHT": 0.03,
    "QUALITY_WEIGHT": 0.03,
    "ANALYST_RATINGS_WEIGHT": 0.3,
    "DIVIDEND_YIELD_WEIGHT": 0.2,
    "SOCIAL_SENTIMENT_WEIGHT": 0.0,
    "NEWS_SENTIMENT_WEIGHT": 0.0,
}

dividend_investment_profile = {
    "MOMENTUM_WEIGHT": 0.1,
    "GROWTH_WEIGHT": 0.01,
    "QUALITY_WEIGHT": 0.01,
    "ANALYST_RATINGS_WEIGHT": 0.2,
    "DIVIDEND_YIELD_WEIGHT": 0.5,
    "SOCIAL_SENTIMENT_WEIGHT": 0.0,
    "NEWS_SENTIMENT_WEIGHT": 0.0,
}

analyst_ratings_investment_profile = {
    "MOMENTUM_WEIGHT": 0.1,
    "GROWTH_WEIGHT": 0.01,
    "QUALITY_WEIGHT": 0.01,
    "ANALYST_RATINGS_WEIGHT": 0.5,
    "DIVIDEND_YIELD_WEIGHT": 0.0,
    "SOCIAL_SENTIMENT_WEIGHT": 0.0,
    "NEWS_SENTIMENT_WEIGHT": 0.2,
}

# Set investment profile
PROFILE = short_term_investment_profile
PROFILE_NAME = "short_term_profile"

MIN_PRICE_DROP_PERCENT = 0.2  # percent