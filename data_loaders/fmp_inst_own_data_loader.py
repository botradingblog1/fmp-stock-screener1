import pandas as pd
from sklearn.linear_model import LinearRegression
import numpy as np
from config import *
from utils.log_utils import *
from botrading.data_loaders.fmp_data_loader import FmpDataLoader


INST_OWN_CACHE_DIR = "cache/inst_own_data"


class FmpInstOwnDataLoader:
    """
    Loads institutional ownership data
    """
    def __init__(self, fmp_api_key):
        self.fmp_data_loader = FmpDataLoader(api_key=fmp_api_key)

    def run(self, symbol_list):
        logi(f"Fetching institutional ownership data...")
        results = []

        # Fetch institutional ownership data
        inst_ownership_dict = self.fmp_data_loader.fetch_multiple_institutional_ownership_changes(symbol_list)
        for symbol, inst_own_df in inst_ownership_dict.items():
            if inst_own_df is None or len(inst_own_df) == 0:
                logi(f"No institutional ownership data for {symbol}")

            total_invested = inst_own_df['totalInvested'].iloc[0]
            total_invested_change = inst_own_df['totalInvestedChange'].iloc[0]
            investors_holding_change = inst_own_df['investorsHoldingChange'].iloc[0]
            investors_holding = inst_own_df['investorsHolding'].iloc[0]
            put_call_ratio = inst_own_df['putCallRatio'].iloc[0]
            put_call_ratio_change = inst_own_df['putCallRatioChange'].iloc[0]
            row = {
                'symbol': symbol,
                'investors_holding': investors_holding,
                'investors_holding_change': round(investors_holding_change, 2),
                'total_invested': round(total_invested, 0),
                'total_invested_change': round(total_invested_change, 2),
                'investors_put_call_ratio':  round(put_call_ratio, 2),
                'investors_put_call_ratio_change': round(put_call_ratio_change, 2)
            }
            results.append(row)

        # convert to dataframe
        inst_own_results_df = pd.DataFrame(results)

        logi(f"Done fetching institutional ownership data...")

        return inst_own_results_df

    def load_for_symbol(self, symbol):
        # Fetch institutional ownership data
        inst_own_df = self.fmp_data_loader.fetch_institutional_ownership_changes(symbol)
        if inst_own_df is None or len(inst_own_df) == 0:
            return {}

        total_invested = inst_own_df['totalInvested'].iloc[0]
        total_invested_change = inst_own_df['totalInvestedChange'].iloc[0]
        investors_holding_change = inst_own_df['investorsHoldingChange'].iloc[0]
        investors_holding = inst_own_df['investorsHolding'].iloc[0]
        put_call_ratio = inst_own_df['putCallRatio'].iloc[0]
        put_call_ratio_change = inst_own_df['putCallRatioChange'].iloc[0]

        # Create output row
        results = {
            'symbol': symbol,
            'investors_holding': investors_holding,
            'investors_holding_change': round(investors_holding_change, 2),
            'total_invested': round(total_invested, 0),
            'total_invested_change': round(total_invested_change, 2),
            'investors_put_call_ratio': round(put_call_ratio, 2),
            'investors_put_call_ratio_change': round(put_call_ratio_change, 2)
        }

        return results
