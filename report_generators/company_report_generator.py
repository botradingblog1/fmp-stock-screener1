import pandas as pd
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
from data_loaders.fmp_analyst_ratings_loader import FmpAnalystRatingsLoader
from data_loaders.fmp_price_target_loader import FmpPriceTargetLoader
from data_loaders.fmp_analyst_estimates_loader import FmpAnalystEstimatesLoader
from data_loaders.edgar_data_loader import EdgarDataLoader
from utils.log_utils import *
from utils.string_utils import camel_to_snake
from datetime import datetime, timedelta
from ai_clients.openai_client import OpenAiClient
from jinja2 import Environment, FileSystemLoader
from utils.log_utils import *
from datetime import datetime
from config import *
from utils.string_utils import *
import os

# Set up the Jinja2 environment and specify the templates directory
env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))


class CompanyReportGenerator:
    def __init__(self, fmp_api_key: str, openai_api_key: str):
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)
        self.fmp_analyst_ratings_loader = FmpAnalystRatingsLoader(fmp_api_key)
        self.fmp_price_target_loader = FmpPriceTargetLoader(fmp_api_key)
        self.fmp_estimates_loader = FmpAnalystEstimatesLoader(fmp_api_key)
        self.edgar_data_loader = EdgarDataLoader()
        self.openai_client = OpenAiClient(openai_api_key)
        """
        self.ratio_analyzer = RatiosAnalyzer()
        self.analyst_rating_analyzer = AnalystRatingAnalyzer()
        self.price_target_analyzer = PriceTargetAnalyzer()
        self.income_analyzer = QuarterlyIncomeAnalyzer()
        self.balance_sheet_analyzer = QuarterlyBalanceSheetAnalyzer()
        self.cash_flow_analyzer = QuarterlyCashFlowAnalyzer()
        self.inst_own_analyzer = InstitutionalOwnershipAnalyzer()
        self.estimate_analyzer = AnalystEstimateAnalyzer()
        """

    def analyze_management_update(self, symbol: str, management_update_text: str):
        # Limit input tokens to avoid errors
        management_update_text = limit_string_tokens(management_update_text, 8000)

        # Analyze management update using GenAI
        # Build prompt
        prompt = f"Analyze this management update in respect to potential for future stock price growth,"\
                 "and economic moat factors. Briefly discuss the potential of the main products and/or services." \
                 "Only return the analysis, no introductory text!" \
                 f"Content: {management_update_text}"
        role = "Financial analyst"
        analysis_text = self.openai_client.query(prompt, role, cache_data=False)
        return analysis_text

    def load_data(self, symbol: str):
        # Fetch company outlook
        company_outlook_data = self.fmp_data_loader.fetch_company_outlook(symbol)
        if not company_outlook_data:
            loge(f"No data returned for symbol {symbol}")
            return None

        # Parse sections
        profile_data = company_outlook_data.get('profile', {})
        news_data = company_outlook_data.get('stockNews', [])
        press_release_data = company_outlook_data.get('pressReleases', [])
        ratios_data = company_outlook_data.get('ratios', [{}])[0]  # Assuming the first item is relevant
        annual_income_data = company_outlook_data.get('financialsAnnual', {}).get('income', [])
        quarterly_income_data = company_outlook_data.get('financialsQuarter', {}).get('income', [])
        annual_balance_data = company_outlook_data.get('financialsAnnual', {}).get('balance', [])
        quarterly_balance_data = company_outlook_data.get('financialsQuarter', {}).get('balance', [])
        annual_cash_data = company_outlook_data.get('financialsAnnual', {}).get('cash', [])
        quarterly_cash_data = company_outlook_data.get('financialsQuarter', {}).get('cash', [])

        # Fetch analyst ratings
        grade_stats_df = self.fmp_analyst_ratings_loader.load(symbol)
        grade_stats_dict = {}
        if grade_stats_df is not None and len(grade_stats_df) > 0:
            grade_stats_dict = grade_stats_df.to_dict(orient='records')

        # Fetch price targets
        price_target_stats_dict = self.fmp_price_target_loader.load(symbol, lookback_days=90)

        # Fetch quarterly analyst estimates
        quarterly_estimates_df, quarterly_estimate_stats = self.fmp_estimates_loader.load(symbol, period="quarter")
        quarterly_estimate_details = {}
        if quarterly_estimates_df is not None and len(quarterly_estimates_df) > 0:
            quarterly_estimate_details = quarterly_estimates_df.to_dict(orient='records')

        # Fetch annual analyst estimates
        annual_estimates_df, annual_estimate_stats = self.fmp_estimates_loader.load(symbol, period="annual")
        annual_estimate_details = {}
        if annual_estimates_df is not None and len(annual_estimates_df) > 0:
            annual_estimate_details = annual_estimates_df.to_dict(orient='records')

        # Fetch institutional ownership
        inst_own_df = self.fmp_data_loader.fetch_institutional_ownership_changes(symbol, cache_data=True,
                                                                                 cache_dir=CACHE_DIR)
        institutional_ownership_data = {}
        if inst_own_df is not None and len(inst_own_df) > 0:
            institutional_ownership_df = inst_own_df.head(3)
            institutional_ownership_data = institutional_ownership_df.to_dict(orient='records')

        # Collect 10Q Management discussion from edgar
        latest_filing_form = self.edgar_data_loader.fetch_latest_form_html(symbol, form_name="10-Q")
        latest_filing_html = ''
        management_update_analysis_text = ''
        if latest_filing_form:
            latest_filing_html = latest_filing_form.html()

            # Extract management update
            management_update_text = self.edgar_data_loader.extract_item_data(latest_filing_form, item_name="Item 2")

            # Perform ChatGPT analysis
            management_update_analysis_text = self.analyze_management_update(symbol, management_update_text)

        # Create report content
        content = {
            'company_name': profile_data.get('companyName', 'Company'),
            'profile': profile_data,
            'news': news_data,
            'press_release_data': press_release_data,
            'management_update_analysis_text': management_update_analysis_text,
            'latest_filing_html': latest_filing_html,
            'ratios': ratios_data,
            'income_quarterly': quarterly_income_data,
            'income_annual': annual_income_data,
            'balance_sheet_quarterly': quarterly_balance_data,
            'balance_sheet_annual': annual_balance_data,
            'cash_flow_quarterly': quarterly_cash_data,
            'cash_flow_annual': annual_cash_data,
            'grade_stats_dict': grade_stats_dict,
            'price_target_stats': price_target_stats_dict,
            'quarterly_estimate_stats': quarterly_estimate_stats,
            'quarterly_estimate_details': quarterly_estimate_details,
            'annual_estimate_stats': annual_estimate_stats,
            'annual_estimate_details': annual_estimate_details,
            'institutional_ownership_data': institutional_ownership_data
        }

        return content

    def generate_report(self, symbol: str, reports_dir="reports"):
        logd(f"Generating report for {symbol}")
        try:
            # Fetch data
            content = self.load_data(symbol)
            if not content:
                loge(f"No content returned for symbol {symbol}")
                return

            # Render report
            report_file = "company_report_template.html"
            template = env.get_template(report_file)
            html_content = template.render(content)

            # Write the rendered HTML to a final report file
            self.store_report(symbol, reports_dir, html_content)
        except Exception as ex:
            loge(f"Failed to generate report for {symbol}: {str(ex)}")

    def store_report(self, symbol: str, reports_dir: str, html_content: str):
        os.makedirs(reports_dir, exist_ok=True)
        #today_str = datetime.today().strftime("%Y-%m-%d")
        report_file_name = f"{symbol}_report.html"
        reports_path = os.path.join(reports_dir, report_file_name)
        with open(reports_path, 'w', encoding='utf-8') as f:
            f.write(html_content)


