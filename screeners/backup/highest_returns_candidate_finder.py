from data_loaders.fmp_stock_list_loader import FmpStockListLoader
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
from screeners.biggest_winner_screener import MomentumScreener1
from screeners.backup.growth_screener1 import GrowthScreener1
from screeners.backup.earnings_estimate_screener1 import EarningsEstimateScreener1
from screeners.backup.fifty_two_week_low_screener import FiftyTwoWeekLowScreener
from data_loaders.fmp_analyst_ratings_loader import FmpAnalystRatingsLoader
from data_loaders.fmp_growth_loader1 import FmpGrowthLoader1
from data_loaders.fmp_stock_news_loader import FmpStockNewsLoader
from utils.df_utils import *
from utils.log_utils import *
from utils.file_utils import *
from datetime import datetime, timedelta
import os

"""
Finds stocks with the highest average monthly returns

"""

# Configuration
HIGHEST_RETURN_CANDIDATES_DIR = "C:\\dev\\trading\\data\\highest_returns\\candidates"
HIGHEST_RETURN_CANDIDATES_FILE_NAME = "highest_return_candidates.csv"
RETURN_LOOKBACK_PERIOD = 180  # Based on https://www.bauer.uh.edu/rsusmel/phd/jegadeesh-titman93.pdf
MIN_AVG_MONTHLY_RETURN = 0.01  # Minimum average monthly return
MIN_LOWEST_MONTHLY_RETURN = -0.2  # Minimum lowest monthly return
MAX_MONTHLY_COV_RETURN = 3.0  # Maximum Coefficient of Variation for monthly returns
MIN_CURRENT_QUARTERLY_REVENUE_GROWTH = 0.01
MIN_CURRENT_QUARTERLY_EARNINGS_GROWTH = 0.01
MIN_AVG_EARNINGS_ESTIMATE_PERCENT = 0.04
MIN_NUM_EARNINGS_ANALYSTS = 3

# Weight configuration for each component
WEIGHTS = {
    'avg_monthly_return': 0.3,
    'avg_eps_growth_percent': 0.2,
    'last_quarter_revenue_growth': 0.1,
    'last_quarter_earnings_growth': 0.1,
    'analyst_rating_score': 0.2,
    'news_sentiment_score': 0.1
}

class HighestReturnsFinder:
    # Finds the highest monthly returns in a set of stocks
    def __init__(self, fmp_api_key: str):
        self.stock_list_loader = FmpStockListLoader(fmp_api_key)
        self.fmp_stock_news_loader = FmpStockNewsLoader(fmp_api_key)
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)
        self.growth_loader = FmpGrowthLoader1(fmp_api_key)
        self.fmp_analyst_ratings_loader = FmpAnalystRatingsLoader(fmp_api_key)
        self.momentum_screener = MomentumScreener1()
        self.fifty_two_week_low_screener = FiftyTwoWeekLowScreener()
        self.earnings_estimate_screener = EarningsEstimateScreener1()
        self.growth_screener = GrowthScreener1()

    def calculate_metrics(self, symbol, prices_df):
        # Resample prices to monthly frequency and calculate monthly returns
        monthly_prices = prices_df['close'].resample('M').last()
        monthly_returns = monthly_prices.pct_change().dropna()

        # Calculate average monthly returns
        avg_monthly_return = monthly_returns.mean()

        # Calculate highest monthly return
        highest_monthly_return = monthly_returns.max()

        # Calculate lowest monthly return
        lowest_monthly_return = monthly_returns.min()

        # Calculate standard deviation of monthly returns
        std_dev_monthly_return = monthly_returns.std()

        # Calculate coefficient of variation (CV)
        cv = std_dev_monthly_return / avg_monthly_return

        # Calculate annualized return
        annualized_return = (1 + avg_monthly_return) ** 12 - 1

        # Calculate highest average return score
        highest_avg_return_score = highest_monthly_return - cv

        metrics = {
            'symbol': symbol,
            'avg_monthly_return': avg_monthly_return,
            'highest_monthly_return': highest_monthly_return,
            'lowest_monthly_return': lowest_monthly_return,
            'cv': cv,
            'annualized_return': annualized_return,
            'std_dev_monthly_return': std_dev_monthly_return,
            'highest_avg_return_score': highest_avg_return_score
        }

        return metrics

    def find_candidates(self):
        logi(f"Calculating metrics....")
        metrics_df = pd.DataFrame()

        # Load stock list
        stock_list_df = self.stock_list_loader.fetch_list(
            exchange_list=EXCHANGE_LIST,
            min_market_cap=MIN_MARKET_CAP,
            min_price=MIN_PRICE,
            max_beta=MAX_BETA,
            min_volume=MIN_VOLUME,
            country=COUNTRY,
            stock_list_limit=STOCK_LIST_LIMIT
        )
        symbol_list = stock_list_df['symbol'].unique()

        # Fetch price history
        start_date = datetime.today() - timedelta(days=DAILY_DATA_FETCH_PERIODS)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date = datetime.today()
        end_date_str = end_date.strftime("%Y-%m-%d")

        prices_dict = self.fmp_data_loader.fetch_multiple_daily_prices_by_date(symbol_list, start_date_str, end_date_str,
                                                                        cache_data=True, cache_dir=CACHE_DIR)

        i = 1
        for symbol in symbol_list:
            # Get prices
            if symbol not in prices_dict:
                logw(f"No prices for {symbol}")
                continue
            prices_df = prices_dict[symbol]

            # Calculate metrics
            metrics = self.calculate_metrics(symbol, prices_df)

            row = pd.DataFrame(metrics, index=[0])
            metrics_df = pd.concat([metrics_df, row], axis=0, ignore_index=True)
            i += 1

        # Apply filters
        metrics_df = metrics_df[metrics_df['lowest_monthly_return'] >= MIN_LOWEST_MONTHLY_RETURN]
        metrics_df = metrics_df[metrics_df['avg_monthly_return'] >= MIN_AVG_MONTHLY_RETURN]
        metrics_df = metrics_df[metrics_df['cv'] <= MAX_MONTHLY_COV_RETURN]

        # Sort by highest average monthly returns
        metrics_df.sort_values(by=["highest_avg_return_score"], ascending=[False], inplace=True)

        # Keep the top 100
        metrics_df = metrics_df.head(100)
        symbol_list = metrics_df['symbol'].unique()

        # Load growth data
        growth_data_dict = self.growth_loader.fetch(symbol_list)

        # Check min earnings/revenue growth and growth acceleration
        growth_df = self.growth_screener.run(growth_data_dict,
                                             MIN_CURRENT_QUARTERLY_EARNINGS_GROWTH,
                                             MIN_CURRENT_QUARTERLY_REVENUE_GROWTH)
        if growth_df is None or len(growth_df) == 0:
            logi("Growth screener returned no results")
            return
        logi(f"Growth screener returned {len(growth_df)} items.")

        # Merge growth
        merged_df = pd.merge(metrics_df, growth_df, on='symbol', how='inner')
        symbol_list = merged_df['symbol'].unique()

        # Fetch future earnings estimates
        earnings_estimate_data_dict = self.fmp_data_loader.fetch_multiple_analyst_earnings_estimates(symbol_list,
                                                                                                     period="quarter",
                                                                                                     limit=100)

        # Screener for earnings estimates
        earnings_estimates_df = self.earnings_estimate_screener.run(earnings_estimate_data_dict,
                                                                    MIN_AVG_EARNINGS_ESTIMATE_PERCENT,
                                                                    MIN_NUM_EARNINGS_ANALYSTS)
        merged_df = pd.merge(merged_df, earnings_estimates_df, on='symbol', how='inner')
        symbol_list = merged_df['symbol'].unique()

        # Get analyst ratings
        analyst_ratings_df = self.fmp_analyst_ratings_loader.fetch(symbol_list)
        merged_df = pd.merge(merged_df, analyst_ratings_df, on='symbol', how='inner')

        # Determine news sentiment
        news_sentiment_df = self.fmp_stock_news_loader.fetch(symbol_list, news_article_limit=30)
        merged_df = pd.merge(merged_df, news_sentiment_df, on='symbol', how='inner')

        if merged_df.empty:
            logi(f"Candidates file is empty")
            return

        # Normalize the different metrics before calculating the score
        for column in ['avg_monthly_return', 'avg_eps_growth_percent', 'last_quarter_revenue_growth',
                       'last_quarter_earnings_growth', 'analyst_rating_score', 'news_sentiment_score']:
            merged_df[f'norm_{column}'] = normalize_series(merged_df[column])

        # Calculate weighted score
        merged_df['weighted_score'] = (
            WEIGHTS['avg_monthly_return'] * merged_df['norm_avg_monthly_return'] +
            WEIGHTS['avg_eps_growth_percent'] * merged_df['norm_avg_eps_growth_percent'] +
            WEIGHTS['last_quarter_revenue_growth'] * merged_df['norm_last_quarter_revenue_growth'] +
            WEIGHTS['last_quarter_earnings_growth'] * merged_df['norm_last_quarter_earnings_growth'] +
            WEIGHTS['analyst_rating_score'] * merged_df['norm_analyst_rating_score'] +
            WEIGHTS['news_sentiment_score'] * merged_df['news_sentiment_score'] * 100
        )

        # Drop all columns that start with 'norm_'
        columns_to_drop = [col for col in merged_df.columns if col.startswith('norm_')]
        merged_df.drop(columns=columns_to_drop, inplace=True)

        # Sort by weighted score in descending order
        merged_df = merged_df.sort_values(by='weighted_score', ascending=False)

        # Store for review
        os.makedirs(HIGHEST_RETURN_CANDIDATES_DIR, exist_ok=True)
        path = os.path.join(HIGHEST_RETURN_CANDIDATES_DIR, HIGHEST_RETURN_CANDIDATES_FILE_NAME)
        store_csv(HIGHEST_RETURN_CANDIDATES_DIR, HIGHEST_RETURN_CANDIDATES_FILE_NAME, merged_df)

        logi(f"Highest avg monthly returns candidates saved to {path}")

        return metrics_df
