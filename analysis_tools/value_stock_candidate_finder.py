import numpy as np
import pandas as pd
from datetime import datetime
from utils.log_utils import *
from utils.report_utils import *
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
from data_loaders.fmp_analyst_ratings_loader import FmpAnalystRatingsLoader
from data_loaders.fmp_price_target_loader import FmpPriceTargetLoader
from data_loaders.fmp_inst_own_data_loader import FmpInstOwnDataLoader
from data_loaders.fmp_analyst_estimates_loader import FmpAnalystEstimatesLoader
from data_loaders.fmp_company_outlook_loader import FmpCompanyOutlookLoader
from report_generators.excel_screener_report_generator import ExcelScreenerReportGenerator
import os

USE_INSTITUTIONAL_OWNERSHIP_API = True
CANDIDATES_DIR = "C:\\dev\\trading\\data\\value_stocks"

"""
Focus on value mid/large cap stocks with low P/E, low P/S, strong revenue growth and high price target
"""


# Functions to calculate score
def calculate_ratios_score(in_df: pd.DataFrame):
    df = in_df.copy()
    # Normalize columns relevant for identifying value stocks
    df = normalize_columns(df, ['priceToSalesRatioTTM', 'priceEarningsRatioTTM', 'grossProfitMarginTTM'])

    # Lower scores indicate better value (low P/E and P/S)
    in_df['ratios_score'] = round((
        (1 / (df['priceToSalesRatioTTM'] + 1)) * 0.2 +  # Invert P/S ratio to reward low values
        (1 / (df['priceEarningsRatioTTM'] + 1)) * 0.6 +  # Invert P/E ratio to reward low values
        df['grossProfitMarginTTM'] * 0.2  # Emphasis on profitability
    ), 2)
    return in_df


def calculate_quarterly_income_score(in_df: pd.DataFrame):
    df = in_df.copy()
    # Normalize columns for growth indicators
    df = normalize_columns(df, ['last_revenue', 'revenue_trend'])

    # Focus on growth in revenue with a slight penalty for increasing costs
    in_df['quarterly_income_score'] = round((
        df['revenue_trend'] * 0.6 +  # Strong emphasis on revenue growth
        df['last_revenue'] * 0.3  # Emphasis on recent revenue levels
    ), 2)
    return in_df


def calculate_annual_income_score(in_df: pd.DataFrame):
    df = in_df.copy()
    df = normalize_columns(df, ['last_revenue', 'revenue_trend', 'cost_expenses_trend'])
    in_df['annual_income_score'] = round((
        df['revenue_trend'] * 0.6 -
        df['cost_expenses_trend'] * 0.3  # Penalty for expenses trend
    ), 2)
    return in_df


def calculate_price_target_score(in_df: pd.DataFrame):
    df = in_df.copy()
    df = normalize_columns(df, ['avg_price_target_change_percent', 'num_price_target_analysts'])

    # Reward high price target growth and analyst support
    in_df['price_target_score'] = round((
        df['avg_price_target_change_percent'] * 0.7 +  # High weight on price target increase
        df['num_price_target_analysts'] * 0.3  # Emphasis on analyst coverage
    ), 2)
    return in_df


def calculate_final_score(ratios_df, quarterly_income_df, annual_income_df, price_target_df):
    scores_df = ratios_df[['symbol', 'ratios_score']].merge(
        quarterly_income_df[['symbol', 'quarterly_income_score']], on='symbol', how='outer'
    ).merge(
        annual_income_df[['symbol', 'annual_income_score']], on='symbol', how='outer'
    ).merge(
        price_target_df[['symbol', 'price_target_score']], on='symbol', how='outer'
    )

    # Replace NaNs with 0
    scores_df = scores_df.fillna(0)

    # Final score calculation prioritizing value and growth factors
    scores_df['final_score'] = round((
        scores_df['ratios_score'] * 0.4 +  # Value focus
        scores_df['quarterly_income_score'] * 0.4 +  # Growth emphasis
        scores_df['annual_income_score'] * 0.1 +  # Growth emphasis
        scores_df['price_target_score'] * 0.1  # Analyst price target importance
    ), 2)

    scores_df.sort_values(by='final_score', ascending=False, inplace=True)
    return scores_df


class ValueStockCandidateFinder:
    def __init__(self, fmp_api_key: str):
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)
        self.analyst_ratings_loader = FmpAnalystRatingsLoader(fmp_api_key)
        self.price_target_loader = FmpPriceTargetLoader(fmp_api_key)
        self.inst_own_loader = FmpInstOwnDataLoader(fmp_api_key)
        self.estimate_loader = FmpAnalystEstimatesLoader(fmp_api_key)
        self.company_outlook_loader = FmpCompanyOutlookLoader(fmp_api_key)
        self.report_generator = ExcelScreenerReportGenerator()

    def find_candidates(self):
        logi("Finding value stock candidates...")

        # Load stock screener results
        stock_list_df = self.fmp_data_loader.fetch_stock_screener_results(
            exchange_list="nyse,nasdaq,amex",
            price_more_than=5.0,
            market_cap_more_than=200000000,  # mid/large cap
            is_actively_trading=True,
            is_fund=False,
            is_etf=False,
            country="US",
            limit=1000
        )

        if stock_list_df is None or stock_list_df.empty:
            logw("No stocks returned from FMP")
            return

        # Sample a few symbols for demonstration
        symbol_list = stock_list_df['symbol'].unique()
        #symbol_list = symbol_list[:5]

        # Lists to store stats
        profile_list, ratios_list, news_list = [], [], []
        quarterly_income_stats_list, annual_income_stats_list = [], []
        quarterly_balance_sheet_stats_list, annual_balance_sheet_stats_list = [], []
        quarterly_cashflow_stats_list, annual_cashflow_stats_list = [], []
        price_target_list, inst_own_data_list = [], []

        for symbol in symbol_list:
            # skip stocks from other countries
            if "." in symbol:
                continue

            logd(f"Processing {symbol}...")

            # Fetch company outlook
            outlook_dict = self.company_outlook_loader.load(symbol)
            if outlook_dict is None:
                logw(f"No data for symbol {symbol}")
                continue

            # Populate outlook results
            profile_list.append(outlook_dict.get('profile', {}))
            ratios = outlook_dict.get('ratios', {})
            ratios['symbol'] = symbol
            ratios_list.append(outlook_dict.get('ratios', {}))
            news_list.append(outlook_dict.get('news_headlines', []))
            quarterly_income_stats_list.append(outlook_dict.get('quarterly_income_stats', {}))
            annual_income_stats_list.append(outlook_dict.get('annual_income_stats', {}))
            quarterly_balance_sheet_stats_list.append(outlook_dict.get('quarterly_balance_sheet_stats', {}))
            annual_balance_sheet_stats_list.append(outlook_dict.get('annual_balance_sheet_stats', {}))
            quarterly_cashflow_stats_list.append(outlook_dict.get('quarterly_cashflow_stats', {}))
            annual_cashflow_stats_list.append(outlook_dict.get('annual_cashflow_stats', {}))

            # Fetch price targets
            price_target_dict = self.price_target_loader.load(symbol)
            if price_target_dict:
                price_target_list.append(price_target_dict)

            # Fetch institutional ownership data
            if USE_INSTITUTIONAL_OWNERSHIP_API:
                inst_own_data = self.inst_own_loader.load_for_symbol(symbol)
                if inst_own_data and len(inst_own_data) > 0:
                    inst_own_data_list.append(inst_own_data)

        # Convert lists to DataFrames
        profile_column_list = ['symbol', 'company_name', 'description', 'website',
                               'mktCap', 'industry', 'sector', 'price', 'volAvg', 'beta']
        profile_df = pd.DataFrame(profile_list, columns=profile_column_list)

        news_column_list = ['symbol', 'news_headlines', 'urls']
        news_df = pd.DataFrame(news_list, columns=news_column_list)

        ratios_column_list = ['symbol', 'priceToSalesRatioTTM', 'priceEarningsRatioTTM', 'grossProfitMarginTTM',
                              'operatingProfitMarginTTM', 'netProfitMarginTTM', 'inventoryTurnoverTTM',
                              'receivablesTurnoverTTM', 'currentRatioTTM', 'quickRatioTTM',
                              'debtEquityRatioTTM', 'interestCoverageTTM']
        ratios_df = pd.DataFrame(ratios_list, columns=ratios_column_list)

        income_column_list = ['symbol', 'last_revenue', 'revenue_change', 'last_net_income', 'last_cost_expenses', 'revenue_trend',
                              'net_income_trend', 'cost_expenses_trend']
        quarterly_income_df = pd.DataFrame(quarterly_income_stats_list, columns=income_column_list)
        annual_income_df = pd.DataFrame(annual_income_stats_list, columns=income_column_list)

        balance_sheet_column_list = ['symbol', 'last_total_assets',
                                     'last_total_debt', 'last_cash_short_term_investments',
                                     'last_total_shareholders_equity', 'total_assets_trend',
                                     'cash_short_term_investments_trend', 'total_debt_trend',
                                     'total_shareholders_equity_trend']
        quarterly_balance_sheet_df = pd.DataFrame(quarterly_balance_sheet_stats_list, columns=balance_sheet_column_list)
        annual_balance_sheet_df = pd.DataFrame(annual_balance_sheet_stats_list, columns=balance_sheet_column_list)

        cashflow_column_list = ['symbol', 'last_operating_cashflow', 'last_capital_expenditure', 'cash_runway',
                                'last_free_cashflow', 'operating_cashflow_trend',
                                'capital_expenditure_trend', 'free_cashflow_trend',
                                'net_cash_for_investing_trend']
        quarterly_cashflow_df = pd.DataFrame(quarterly_cashflow_stats_list, columns=cashflow_column_list)
        annual_cashflow_df = pd.DataFrame(annual_cashflow_stats_list, columns=cashflow_column_list)

        price_target_column_list = ['symbol', 'avg_price_target_change_percent', 'price_target_coefficient_variation',
                                    'num_price_target_analysts']
        price_target_df = pd.DataFrame(price_target_list, columns=price_target_column_list)

        inst_own_column_list = ['symbol', 'investors_holding', 'investors_holding_change',
                                'total_invested', 'total_invested_change',
                                'investors_put_call_ratio', 'investors_put_call_ratio_change']
        inst_own_df = pd.DataFrame(inst_own_data_list, columns=inst_own_column_list)

        # Calculate section scores
        ratios_df = calculate_ratios_score(ratios_df)
        quarterly_income_df = calculate_quarterly_income_score(quarterly_income_df)
        annual_income_df = calculate_annual_income_score(annual_income_df)
        quarterly_balance_sheet_df = calculate_quarterly_balance_sheet_score(quarterly_balance_sheet_df)
        annual_balance_sheet_df = calculate_annual_balance_sheet_score(annual_balance_sheet_df)
        quarterly_cashflow_df = calculate_quarterly_cashflow_score(quarterly_cashflow_df)
        annual_cashflow_df = calculate_annual_cashflow_score(annual_cashflow_df)
        price_target_df = calculate_price_target_score(price_target_df)
        inst_own_df = pd.DataFrame()
        if USE_INSTITUTIONAL_OWNERSHIP_API:
            inst_own_df = calculate_inst_own_score(inst_own_df)

        # Calculate final score and sort each DataFrame by it
        scores_df = calculate_final_score(ratios_df, quarterly_income_df,
                                          annual_income_df, price_target_df)

        # Optional - Sort the sections in the same order as the final score
        profile_df = align_section_order(scores_df, profile_df)
        news_df = align_section_order(scores_df, news_df)
        ratios_df = align_section_order(scores_df, ratios_df)
        quarterly_income_df = align_section_order(scores_df, quarterly_income_df)
        annual_income_df = align_section_order(scores_df, annual_income_df)
        quarterly_balance_sheet_df = align_section_order(scores_df, quarterly_balance_sheet_df)
        annual_balance_sheet_df = align_section_order(scores_df, annual_balance_sheet_df)
        quarterly_cashflow_df = align_section_order(scores_df, quarterly_cashflow_df)
        annual_cashflow_df = align_section_order(scores_df, annual_cashflow_df)
        price_target_df = align_section_order(scores_df, price_target_df)
        if inst_own_df is not None and len(inst_own_df) > 0:
            inst_own_df = align_section_order(scores_df, inst_own_df)

        report_data = {
            'profile_data': profile_df,
            'scores_data': scores_df,
            'news_data': news_df,
            'ratios_data': ratios_df,
            'quarterly_income_data': quarterly_income_df,
            'annual_income_data': annual_income_df,
            'quarterly_balance_sheet_data': quarterly_balance_sheet_df,
            'annual_balance_sheet_data': annual_balance_sheet_df,
            'quarterly_cashflow_data': quarterly_cashflow_df,
            'annual_cashflow_data': annual_cashflow_df,
            'price_target_data': price_target_df,
        }

        if USE_INSTITUTIONAL_OWNERSHIP_API:
            report_data['inst_own_data'] = inst_own_df

        # Generate report
        file_name = f"value_stock_candidates_{datetime.today().strftime('%Y-%m-%d')}.xlsx"
        self.report_generator.generate_report(report_data, CANDIDATES_DIR, file_name)

        logi("Done with value stock analysis.")
