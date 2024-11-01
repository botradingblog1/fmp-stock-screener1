import pandas as pd
from config import *
from utils.log_utils import *
from utils.file_utils import *
from utils.indicator_utils import *
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
import numpy as np

CACHE_DIR = "cache"


class FmpCompanyOutlookLoader:
    """
    Loads the company outlook from FinancialModelingPrep and calculates stats
    """
    def __init__(self, fmp_api_key):
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)

    def aggregate_news_data(self, news_data: list, separator: str = " | "):
        output = ""
        for i, news_item in enumerate(news_data):
            if i > 0:
                output += separator
            published_date = pd.to_datetime(news_item.get('publishedDate', None), errors='coerce')
            if published_date is not pd.NaT:
                published_date_str = published_date.strftime("%Y-%m-%d %H:%M:%S")
            else:
                published_date_str = "N/A"
            title = news_item.get('title', "No Title")
            output += f"{published_date_str}: {title}"
        return output

    def calculate_income_stats(self, income_data: list):
        stats = {}
        if not income_data:
            return stats

        income_stats_df = pd.DataFrame(income_data)
        income_stats_df['date'] = pd.to_datetime(income_stats_df['date'], errors="coerce")
        income_stats_df.sort_values(by='date', ascending=True, inplace=True)

        # Set defaults and round values
        stats['last_revenue'] = round(income_stats_df['revenue'].iloc[-1],
                                      2) if 'revenue' in income_stats_df else np.nan
        stats['last_net_income'] = round(income_stats_df['netIncome'].iloc[-1],
                                         2) if 'netIncome' in income_stats_df else np.nan
        stats['last_cost_expenses'] = round(income_stats_df['costAndExpenses'].iloc[-1],
                                            2) if 'costAndExpenses' in income_stats_df else np.nan

        # Calculate trends on percentage changes
        stats['revenue_trend'] = round(compute_slope_internal(income_stats_df['revenue'].pct_change().dropna().values),
                                       2)
        stats['net_income_trend'] = round(
            compute_slope_internal(income_stats_df['netIncome'].pct_change().dropna().values), 2)
        stats['cost_expenses_trend'] = round(
            compute_slope_internal(income_stats_df['costAndExpenses'].pct_change().dropna().values), 2)
        return stats

    def calculate_balance_sheet_stats(self, balance_sheet_data: list):
        stats = {}
        if not balance_sheet_data:
            return stats

        balance_sheet_df = pd.DataFrame(balance_sheet_data)
        balance_sheet_df['date'] = pd.to_datetime(balance_sheet_df['date'], errors="coerce")
        balance_sheet_df.sort_values(by='date', ascending=True, inplace=True)

        stats['last_total_assets'] = round(balance_sheet_df['totalAssets'].iloc[-1],
                                           2) if 'totalAssets' in balance_sheet_df else np.nan
        stats['last_cash_short_term_investments'] = round(balance_sheet_df['cashAndShortTermInvestments'].iloc[-1],
                                                          2) if 'cashAndShortTermInvestments' in balance_sheet_df else np.nan
        stats['last_total_debt'] = round(balance_sheet_df['totalDebt'].iloc[-1],
                                         2) if 'totalDebt' in balance_sheet_df else np.nan
        stats['last_total_shareholders_equity'] = round(balance_sheet_df['totalStockholdersEquity'].iloc[-1],
                                                        2) if 'totalStockholdersEquity' in balance_sheet_df else np.nan

        # Calculate trends on percentage changes
        stats['total_assets_trend'] = round(
            compute_slope_internal(balance_sheet_df['totalAssets'].pct_change().dropna().values), 2)
        stats['cash_short_term_investments_trend'] = round(
            compute_slope_internal(balance_sheet_df['cashAndShortTermInvestments'].pct_change().dropna().values), 2)
        stats['total_debt_trend'] = round(
            compute_slope_internal(balance_sheet_df['totalDebt'].pct_change().dropna().values), 2)
        stats['total_shareholders_equity_trend'] = round(
            compute_slope_internal(balance_sheet_df['totalStockholdersEquity'].pct_change().dropna().values), 2)
        return stats

    def calculate_cashflow_stats(self, cashflow_data: list):
        stats = {}
        if not cashflow_data:
            return stats

        cashflow_df = pd.DataFrame(cashflow_data)
        cashflow_df['date'] = pd.to_datetime(cashflow_df['date'], errors="coerce")
        cashflow_df.sort_values(by='date', ascending=True, inplace=True)

        stats['last_operating_cashflow'] = round(cashflow_df['operatingCashFlow'].iloc[-1],
                                                 2) if 'operatingCashFlow' in cashflow_df else np.nan
        stats['last_capital_expenditure'] = round(cashflow_df['capitalExpenditure'].iloc[-1],
                                                  2) if 'capitalExpenditure' in cashflow_df else np.nan
        stats['last_free_cashflow'] = round(cashflow_df['freeCashFlow'].iloc[-1],
                                            2) if 'freeCashFlow' in cashflow_df else np.nan
        stats['last_net_cash_for_investing'] = round(cashflow_df['netCashUsedForInvestingActivites'].iloc[-1],
                                                     2) if 'netCashUsedForInvestingActivites' in cashflow_df else np.nan

        # Calculate trends on percentage changes
        stats['operating_cashflow_trend'] = round(
            compute_slope_internal(cashflow_df['operatingCashFlow'].pct_change().dropna().values), 2)
        stats['capital_expenditure_trend'] = round(
            compute_slope_internal(cashflow_df['capitalExpenditure'].pct_change().dropna().values), 2)
        stats['free_cashflow_trend'] = round(
            compute_slope_internal(cashflow_df['freeCashFlow'].pct_change().dropna().values), 2)
        stats['net_cash_for_investing_trend'] = round(
            compute_slope_internal(cashflow_df['netCashUsedForInvestingActivites'].pct_change().dropna().values), 2)
        return stats

    def load(self, symbol: str):
        results = {}

        # Fetch company outlook
        company_outlook_data = self.fmp_data_loader.fetch_company_outlook(symbol)
        if not company_outlook_data:
            loge(f"No data returned for symbol {symbol}")
            return results

        # Parse sections
        results['profile'] = company_outlook_data.get('profile', {})

        # Add news data
        news_data = company_outlook_data.get('stockNews', [])
        results['news_data'] = news_data
        results['news_headlines'] = self.aggregate_news_data(news_data)

        # Get financial ratios
        ratios_list = company_outlook_data.get('ratios', [{}])
        if ratios_list:
            results['ratios'] = ratios_list[0]

        # Parse Income data
        results['annual_income_data'] = company_outlook_data.get('financialsAnnual', {}).get('income', [])
        results['annual_income_stats'] = self.calculate_income_stats(results['annual_income_data'])
        results['annual_income_stats']['symbol'] = symbol

        results['quarterly_income_data'] = company_outlook_data.get('financialsQuarter', {}).get('income', [])
        results['quarterly_income_stats'] = self.calculate_income_stats(results['quarterly_income_data'])
        results['quarterly_income_stats']['symbol'] = symbol

        # Parse balance sheet data
        results['annual_balance_sheet_data'] = company_outlook_data.get('financialsAnnual', {}).get('balance', [])
        results['annual_balance_sheet_stats'] = self.calculate_balance_sheet_stats(results['annual_balance_sheet_data'])
        results['annual_balance_sheet_stats']['symbol'] = symbol

        results['quarterly_balance_sheet_data'] = company_outlook_data.get('financialsQuarter', {}).get('balance', [])
        results['quarterly_balance_sheet_stats'] = self.calculate_balance_sheet_stats(results['quarterly_balance_sheet_data'])
        results['quarterly_balance_sheet_stats']['symbol'] = symbol

        # Parse cash flow data
        results['annual_cashflow_data'] = company_outlook_data.get('financialsAnnual', {}).get('cash', [])
        results['annual_cashflow_stats'] = self.calculate_cashflow_stats(results['annual_cashflow_data'])
        results['annual_cashflow_stats']['symbol'] = symbol

        results['quarterly_cashflow_data'] = company_outlook_data.get('financialsQuarter', {}).get('cash', [])
        results['quarterly_cashflow_stats'] = self.calculate_cashflow_stats(results['quarterly_cashflow_data'])
        results['quarterly_cashflow_stats']['symbol'] = symbol

        # Get rating
        rating_list = company_outlook_data.get('rating', [{}])
        if rating_list:
            results['rating'] = rating_list[0]

        return results
