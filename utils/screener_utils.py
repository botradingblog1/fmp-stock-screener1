import pandas as pd
from utils.log_utils import *


def calculate_price_target_stats(symbol: str, price_target_df: pd.DataFrame):
    # Calculate percentage change in adjusted price targets relative to the current price
    avg_price_target = price_target_df['adjPriceTarget'].mean()
    price_target_df['priceTargetChangePercent'] = (price_target_df['adjPriceTarget'] - price_target_df[
        'priceWhenPosted']) / price_target_df['priceWhenPosted'] * 100

    # Calculate average price target percentage change
    avg_price_target_change = price_target_df['priceTargetChangePercent'].mean()

    # Calculate the coefficient of variation (standard deviation / mean) of price target changes
    price_target_coefficient_variation = (
        price_target_df['priceTargetChangePercent'].std() / avg_price_target_change
    ) if avg_price_target_change != 0 else 0

    # Calculate agreement: ratio of positive to negative price target changes
    positive_changes = price_target_df['priceTargetChangePercent'] > 0
    negative_changes = price_target_df['priceTargetChangePercent'] < 0
    if negative_changes.sum() > 0:
        price_target_agreement_ratio = positive_changes.sum() / negative_changes.sum()
    else:
        price_target_agreement_ratio = 0  # Handle cases where there are no negative changes

    # Number of analysts
    num_price_target_analysts = len(price_target_df)

    # Update results
    stats_df = pd.DataFrame({
        'symbol': [symbol],
        'avg_price_target': [round(avg_price_target, 2)],
        'avg_price_target_change': [round(avg_price_target_change, 2)],
        'price_target_coefficient_variation': [round(price_target_coefficient_variation, 2)],
        'price_target_agreement_ratio': [round(price_target_agreement_ratio, 2)],
        'num_price_target_analysts': [round(num_price_target_analysts, 2)]
    })
    return stats_df


def aggregate_analyst_grades(symbol: str, grades_df: pd.DataFrame):
    """
    Used to aggregate analyst grades (buy ratings)
    :param grades_df:
    :param lookback_period:
    :return:
    """

    if grades_df is None or grades_df.empty:
        #logw(f"grades_df is empty")
        return None

    # Aggregate counts
    strong_buy_count = 0
    buy_count = 0
    outperform_count = 0
    sell_count = 0
    strong_sell_count = 0
    underperform_count = 0
    hold_count = 0
    total_ratings_count = 0

    for index, row in grades_df.iterrows():
        grade = row['newGrade']
        if grade == "Strong Buy":
            strong_buy_count += 1
        elif grade == "Buy" or grade == "Long-Term Buy" or grade == "Conviction Buy":
            buy_count += 1
        elif grade == "Outperform" or grade == "Perform" or grade == "Overweight":
            outperform_count += 1
        elif grade == "Strong Sell":
            strong_sell_count += 1
        elif grade == "Sell" or grade == "Long-Term Sell" or grade == "Conviction Sell":
            sell_count += 1
        elif grade == "Underperform" or grade == "Underweight":
            underperform_count += 1
        elif grade == "Hold" or grade == "Equal-Weight":
            hold_count += 1
        total_ratings_count += 1

    bullish_count = 2 * strong_buy_count + buy_count + outperform_count
    bearish_count = 2 * strong_sell_count + sell_count + underperform_count
    total_rating = bullish_count - bearish_count

    grades_stats_df = pd.DataFrame({
        'symbol': [symbol],
        'strong_buy_count': [strong_buy_count],
        'buy_count': [buy_count],
        'outperform_count': [buy_count],
        'sell_count': [sell_count],
        'strong_sell_count': [strong_sell_count],
        'underperform_count': [underperform_count],
        'hold_count': [hold_count],
        'bullish_count': [bullish_count],
        'bearish_count': [bearish_count],
        'total_grades_rating': [total_rating],
        'total_ratings_count': [total_ratings_count]
    })
    return grades_stats_df


def calculate_institutional_ownership_stats(symbol: str, inst_own_df: pd.DataFrame):
    # Get the last record values
    total_invested = inst_own_df['totalInvested'].iloc[0]
    total_invested_change = inst_own_df['totalInvestedChange'].iloc[0]
    investors_holding_change = inst_own_df['investorsHoldingChange'].iloc[0]
    investors_holding = inst_own_df['investorsHolding'].iloc[0]
    put_call_ratio = inst_own_df['putCallRatio'].iloc[0]
    put_call_ratio_change = inst_own_df['putCallRatioChange'].iloc[0]

    # Calculate score
    normalized_put_call_ratio = put_call_ratio / (1 + put_call_ratio)
    investor_score = investors_holding_change * (1 - normalized_put_call_ratio)

    # Create output row
    stats_df = pd.DataFrame({
        'symbol': [symbol],
        'investors_holding': [investors_holding],
        'investors_holding_change': [round(investors_holding_change, 2)],
        'total_invested': [round(total_invested, 0)],
        'total_invested_change': [round(total_invested_change, 2)],
        'investors_put_call_ratio': [round(put_call_ratio, 2)],
        'investors_put_call_ratio_change': [round(put_call_ratio_change, 2)],
        'institutional_investor_score': [round(investor_score, 2)]
    })
    return stats_df
