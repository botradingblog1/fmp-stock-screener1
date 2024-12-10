import pandas as pd
from utils.string_utils import camel_to_snake
import numpy as np
from utils.log_utils import *


def fetch_multiple_ratios(fmp_data_loader: any, symbol_list: list, period="quarterly"):
    combined_ratios_df = pd.DataFrame()
    for symbol in symbol_list:
        ratios_df = fmp_data_loader.get_financial_ratios(symbol, period=period)
        if ratios_df is None or ratios_df.empty:
            continue
        # Get latest ratios for current quarter
        ratios_df = ratios_df.head(1)

        # Rename columns from camelCase to snake_case
        ratios_df.columns = [camel_to_snake(col) for col in ratios_df.columns]

        combined_ratios_df = pd.concat([combined_ratios_df, ratios_df], axis=0, ignore_index=True)
    return combined_ratios_df


def fetch_future_revenue_growth(fmp_data_loader: any, symbol_list: list, period="annual"):
    results = []

    for symbol in symbol_list:
        # Fetch analyst earnings estimates
        estimates_df = fmp_data_loader.fetch_analyst_earnings_estimates(
            symbol, period=period, limit=100
        )

        if estimates_df is None or estimates_df.empty:
            logd(f"No estimates available for {symbol}")
            continue

        # Filter data to include future dates
        cutoff_date = pd.Timestamp.now()
        estimates_df['date'] = pd.to_datetime(estimates_df['date'], errors='coerce')
        estimates_df = estimates_df[
            (estimates_df['date'] >= cutoff_date) & estimates_df['date'].notna()
            ]
        if estimates_df.empty:
            logd(f"No recent estimates available for {symbol}")
            continue

        # Sort by date
        estimates_df.sort_values(by=['date'], ascending=[True], inplace=True)

        # Calculate average estimated revenue change
        estimates_df['avg_estimated_revenue_change'] = estimates_df['estimatedRevenueAvg'].pct_change() * 100
        avg_estimated_revenue_change = estimates_df['avg_estimated_revenue_change'].mean(skipna=True)

        # Calculate average number of analysts for revenue estimate
        avg_revenue_estimate_analysts = estimates_df['numberAnalystEstimatedRevenue'].mean(skipna=True)

        # Append stats to results
        row = {'symbol': symbol,
               'avg_estimated_revenue_change': avg_estimated_revenue_change,
               'avg_revenue_estimate_analysts': avg_revenue_estimate_analysts}
        results.append(row)

    # Convert results to DataFrame
    results_df = pd.DataFrame(results)

    if results_df.empty:
        logw("No results found during screening")
        return results_df

    # Handle infinity values
    results_df.replace([np.inf, -np.inf], np.nan, inplace=True)

    return results_df

