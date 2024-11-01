import pandas as pd
from datetime import datetime
from openpyxl.styles import Alignment
import os


class ExcelScreenerReportGenerator:

    def format_sheet_column_values(self, sheet):
        for col in sheet.columns:
            max_length = max(len(str(cell.value)) for cell in col)
            col_letter = col[0].column_letter
            sheet.column_dimensions[col_letter].width = max_length + 2

    def build_profile_sheet(self, writer: any, df: pd.DataFrame):
        df.to_excel(writer, sheet_name="Profile", index=False)
        sheet = writer.sheets["Profile"]
        self.format_sheet_column_values(sheet)
        sheet.freeze_panes = sheet["A2"]  # Freeze first row

        # Wrap and format company description
        sheet.column_dimensions["C"].width = 60
        is_header_row = True
        for cell in sheet["C"]:
            if is_header_row:
                is_header_row = False
                continue
            cell.alignment = Alignment(wrap_text=True)
            sheet.row_dimensions[cell.row].height = 120

        return sheet

    def build_news_sheet(self, writer: any, df: pd.DataFrame):
        df.to_excel(writer, sheet_name="News Headlines", index=False)
        sheet = writer.sheets["News Headlines"]
        self.format_sheet_column_values(sheet)
        sheet.freeze_panes = sheet["A2"]  # Freeze first row

        # Wrap and format news headlines
        sheet.column_dimensions["B"].width = 60
        is_header_row = True
        for cell in sheet["B"]:
            if is_header_row:
                is_header_row = False
                continue
            cell.alignment = Alignment(wrap_text=True)
            sheet.row_dimensions[cell.row].height = 60
        return sheet

    def build_generic_sheet(self, writer: any, title: str = "sheet", data_df: pd.DataFrame = None):
        data_df.to_excel(writer, sheet_name=title, index=False)
        sheet = writer.sheets[title]
        self.format_sheet_column_values(sheet)
        sheet.freeze_panes = sheet["A2"]  # Freeze first row
        return sheet

    def generate_report(self,
                        data: dict = {},
                        path: str = "reports",
                        file_name: str = "report.xlsx"):

        os.makedirs(path, exist_ok=True)
        full_path = os.path.join(path, file_name)
        with pd.ExcelWriter(full_path) as writer:
            # Build profile sheet
            if 'profile_data' in data:
                profile_data = data['profile_data']
                self.build_profile_sheet(writer, profile_data)

            # Build scores sheet
            if 'scores_data' in data:
                scores_data = data['scores_data']
                self.build_generic_sheet(writer, "Scores", scores_data)

            # Build news headlines sheet
            if 'news_data' in data:
                news_data = data['news_data']
                self.build_news_sheet(writer, news_data)

            # Build quarterly income sheet
            if 'quarterly_income_data' in data:
                df = data['quarterly_income_data']
                self.build_generic_sheet(writer, "Quarterly Income", df)

            # Build annual income sheet
            if 'annual_income_data' in data:
                df = data['annual_income_data']
                self.build_generic_sheet(writer, "Annual Income", df)

            # Build quarterly balance sheet tab
            if 'quarterly_balance_sheet_data' in data:
                df = data['quarterly_balance_sheet_data']
                self.build_generic_sheet(writer, "Quarterly Balance Sheet", df)

            # Build annual balance sheet tab
            if 'annual_balance_sheet_data' in data:
                df = data['annual_balance_sheet_data']
                self.build_generic_sheet(writer, "Annual Balance Sheet", df)

            # Build quarterly cashflow sheet
            if 'quarterly_cashflow_data' in data:
                df = data['quarterly_cashflow_data']
                self.build_generic_sheet(writer, "Quarterly Cashflow", df)

            # Build annual cashflow sheet
            if 'annual_cashflow_data' in data:
                df = data['annual_cashflow_data']
                self.build_generic_sheet(writer, "Annual Cashflow", df)

            # Build price target sheet
            if 'price_target_data' in data:
                df = data['price_target_data']
                self.build_generic_sheet(writer, "Price Target", df)

            # Build institutional ownership sheet
            if 'inst_own_data' in data:
                df = data['inst_own_data']
                self.build_generic_sheet(writer, "Institutional Ownership", df)

        print("Report generated at:", full_path)
