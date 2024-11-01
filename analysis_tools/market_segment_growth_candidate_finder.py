import numpy as np
import pandas as pd
import os
from sklearn.preprocessing import MinMaxScaler
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
from data_loaders.fmp_analyst_ratings_loader import FmpAnalystRatingsLoader
from data_loaders.fmp_price_target_loader import FmpPriceTargetLoader
from data_loaders.fmp_inst_own_data_loader import FmpInstOwnDataLoader
from data_loaders.fmp_analyst_estimates_loader import FmpAnalystEstimatesLoader
from utils.file_utils import *
from utils.log_utils import *
from utils.indicator_utils import *
from config import *
from datetime import datetime, timedelta

# Configuration
USE_INSTITUTIONAL_OWNERSHIP_API = True
CANDIDATES_DIR = "C:\\dev\\trading\\data\\market_segment_growth\\candidates"
CACHE_DIR = "cache"  # Added definition for CACHE_DIR

"""
    Definitions for the different market growth sector stocks
    Based on BCC Research report info here: https://www.bccresearch.com/report/justreleased
    Open each report detail page and look at the table below to find numbers for future market size,
    CAGR and associated companies.
    
        To add a new market segment:
    1. Find a new segment here:
       https://www.bccresearch.com/report/justreleased
    2. Get the numbers for segment name, future market size, CAGR and companies from the overview table
    3. For each company name, look up the stock symbl
    4. Create a new section for the market segment in the market_segment_info array
    5. Add the symbols and company names to the symbol_company_name_map for lookup if not already present
"""

market_segment_info = [
    {
        "name": "Private 5G Networks",
        "future_market_size": 22.2,
        "CAGR": 46.8,
        "symbol_list": ['CSCO', 'CIEN', 'JNPR', '6701.T', 'NOK', 'ORCL', 'SSNLF', 'ERIC', 'ZTCOF']
    },
    {
        "name": "Global Markets and Technologies for Nanofiltration",
        "future_market_size": 3.4,
        "CAGR": 17.6,
        "symbol_list": ['MMM', 'ALFVY', 'DCI', 'DD', 'NDEKY', 'NXFIL.AS', 'PH', 'TRYIY', 'VEOEY', 'XYL']
    },
    {
        "name": "AI in Clinical and Molecular Diagnostics Market",
        "future_market_size": 8.9,
        "CAGR": 26,
        "symbol_list": ['RHHBY', 'FUJIY', 'GEHC', 'ILMN', 'PHG', 'MDT', 'MSFT', 'SEMHF', 'TMO', 'BFLY', 'INTC', 'NVDA']
    },
    {
        "name": "RNA Sequencing",
        "future_market_size": 10.3,
        "CAGR": 19,
        "symbol_list": ['TXG', 'A', 'DHR', 'BIO', 'BGIGF', 'RHHBY', 'ILMN', 'ONT.L', 'QGEN', 'RVTY', 'LAB', 'TMO']
    },
    {
        "name": "Advanced Materials for 3D Printing",
        "future_market_size": 8.6,
        "CAGR": 18,
        "symbol_list": ['ARKAY', 'DM', 'EVK.DE', 'HENKY', 'MTLS', 'MKFG', 'PRLB', 'SSYS']
    },
    {
        "name": "Digital Twin Market",
        "future_market_size": 119,
        "CAGR": 45.7,
        "symbol_list": ['ABB', 'ALTR', 'ANSS', 'ADSK', 'BSY', 'DASTY', 'GE', 'HXGBY', 'IBM', 'MSFT', 'ORCL', 'ROK', 'SBGSY', 'SIEGY']
    },
    {
        "name": "Antibody-Drug Conjugates",
        "future_market_size": 49,
        "CAGR": 28,
        "symbol_list": ['ABBV', 'ADCT', 'DSNKY', 'RHHBY', 'GILD', 'PFE']
    },
    {
        "name": "Carbon Nanotubes",
        "future_market_size": 5.9,
        "CAGR": 20,
        "symbol_list": ['ARKAY', 'CBT', 'KUMHO.PK', 'LGCLF']
    },
    {
        "name": "Gene Editing Therapeutics",
        "future_market_size": 1,
        "CAGR": 147,
        "symbol_list": ['ALLO', 'BEAM', 'CRBU', 'CRSP', 'EDIT', 'NTLA', 'DTIL', 'SGMO', 'VRTX', 'VERV', 'VOR']
    },
    {
        "name": "Global Artificial Intelligence (AI) Market",
        "future_market_size": 1100,
        "CAGR": 39.7,
        "symbol_list": ['AMD', 'DELL', 'INTC', '2454.TW', 'MU', 'SSNLF', 'AVGO', 'CSCO', 'HPE', 'ERIC', 'IBM', 'IFNNY', 'MSFT', 'NVDA', 'ORCL', 'CRM', 'CHKP', 'FTNT', 'GOOGL', 'META', 'SAP', 'SIEGY']
    },
    {
        "name": "Artificial Intelligence (AI) in Life Sciences Market",
        "future_market_size": 33.5,
        "CAGR": 27.9,
        "symbol_list": ['AMD', 'DELL', 'INTC', '2454.TW', 'MU', 'SSNLF', 'AVGO', 'CSCO', 'HPE', 'ERIC', 'IBM', 'IFNNY', 'MSFT', 'NVDA', 'ORCL', 'CRM', 'CHKP', 'FTNT', 'GOOGL', 'META', 'SAP', 'SIEGY']
    },
    {
        "name": "Electric Vehicles and Fuel Cell Vehicles",
        "future_market_size": 1800,
        "CAGR": 18,
        "symbol_list": ['BMWYY', 'BYDDY', 'CQCQF', 'GNENY', 'GM', 'GWLLY', 'HYMTF', 'LI', 'MBGAF', 'SAICF', 'STLA', 'TSLA', 'TM', 'VWAGY', 'VOLAF', 'GELYY']
    },
    {
        "name": "Wearable Medical Devices",
        "future_market_size": 151.8,
        "CAGR": 27.5,
        "symbol_list": ['ABT', 'DXCM', 'GEHC', 'PHG', 'MDT', 'MASI', '3407.T']
    },
    {
        "name": "Global Virtual Power Plant Market",
        "future_market_size": 6.2,
        "CAGR": 21.5,
        "symbol_list": ['ABB', 'AGLNF', 'ENLAY', 'GE', 'SIEGY']
    }
]
# Map of symbol -> company name for lookup
symbol_company_name_map = {
    'ABB': 'ABB Ltd',
    'ABBV': 'AbbVie Inc.',
    'ABT': 'Abbott Laboratories',
    'ADCT': 'ADC Therapeutics SA',
    'ADSK': 'Autodesk, Inc.',
    'AGLNF': 'AGL Energy Limited',
    'ALFVY': 'Alfa Laval AB',
    'ALLO': 'Allogene Therapeutics, Inc.',
    'ALTR': 'Altair Engineering Inc.',
    'AMD': 'Advanced Micro Devices, Inc.',
    'AMZN': 'Amazon.com, Inc.',
    'ANSS': 'Ansys, Inc.',
    'ARKAY': 'Arkema S.A.',
    'AVGO': 'Broadcom Inc.',
    'BEAM': 'Beam Therapeutics Inc.',
    'BFLY': 'Butterfly Network, Inc.',
    'BGIGF': 'BGI Genomics Co., Ltd.',
    'BIO': 'Bio-Rad Laboratories, Inc.',
    'BMWYY': 'Bayerische Motoren Werke AG (BMW)',
    'BSY': 'Bentley Systems, Incorporated',
    'BYDDY': 'BYD Company Limited',
    'CBT': 'Cabot Corporation',
    'CHKP': 'Check Point Software Technologies Ltd.',
    'CIEN': 'Ciena Corporation',
    'CRM': 'Salesforce, Inc.',
    'CRBU': 'Caribou Biosciences, Inc.',
    'CRSP': 'CRISPR Therapeutics AG',
    'CSCO': 'Cisco Systems, Inc.',
    'CQCQF': 'Chongqing Changan Automobile Company Limited',
    'DASTY': 'Dassault Systèmes SE',
    'DCI': 'Donaldson Company, Inc.',
    'DD': 'DuPont de Nemours, Inc.',
    'DELL': 'Dell Technologies Inc.',
    'DHR': 'Danaher Corporation',
    'DSNKY': 'Daiichi Sankyo Company, Limited',
    'DTIL': 'Precision BioSciences, Inc.',
    'DXCM': 'DexCom, Inc.',
    'EDIT': 'Editas Medicine, Inc.',
    'ENLAY': 'Enel SpA',
    'ERIC': 'Telefonaktiebolaget LM Ericsson',
    'EVK': 'Evonik Industries AG',
    'FTNT': 'Fortinet, Inc.',
    'FUJIY': 'Fujifilm Holdings Corporation',
    'GE': 'General Electric Company',
    'GEHC': 'GE HealthCare Technologies Inc.',
    'GELYY': 'Geely Automobile Holdings Limited',
    'GILD': 'Gilead Sciences, Inc.',
    'GM': 'General Motors Company',
    'GOOGL': 'Alphabet Inc. (Google)',
    'GWLLY': 'Great Wall Motor Company Limited',
    'HENKY': 'Henkel AG & Co. KGaA',
    'HPE': 'Hewlett Packard Enterprise Company',
    'HYMTF': 'Hyundai Motor Company',
    'IBM': 'IBM Corporation',
    'IFNNY': 'Infineon Technologies AG',
    'ILMN': 'Illumina, Inc.',
    'INTC': 'Intel Corporation',
    'JNPR': 'Juniper Networks, Inc.',
    'LGCLF': 'LG Chem Ltd.',
    'LI': 'Li Auto Inc.',
    'MASI': 'Masimo Corporation',
    'MBGAF': 'Mercedes-Benz Group AG',
    'MDT': 'Medtronic plc',
    'META': 'Meta Platforms, Inc.',
    'MKFG': 'Markforged Holding Corporation',
    'MMM': '3M Company',
    'MSFT': 'Microsoft Corporation',
    'MTLS': 'Materialise NV',
    'MU': 'Micron Technology, Inc.',
    'NDEKY': 'Nitto Denko Corporation',
    'NOK': 'Nokia Corporation',
    'NTLA': 'Intellia Therapeutics, Inc.',
    'NVDA': 'NVIDIA Corporation',
    'NXFIL.AS': 'NX Filtration N.V.',
    'ONT.L': 'Oxford Nanopore Technologies plc',
    'ORCL': 'Oracle Corporation',
    'PFE': 'Pfizer Inc.',
    'PH': 'Parker-Hannifin Corporation',
    'PHG': 'Koninklijke Philips N.V.',
    'PRLB': 'Proto Labs, Inc.',
    'QGEN': 'Qiagen N.V.',
    'RHHBY': 'F. Hoffmann-La Roche Ltd',
    'ROK': 'Rockwell Automation, Inc.',
    'RVTY': 'Revvity, Inc.',
    'SAP': 'SAP SE',
    'SEMHF': 'Siemens Healthineers AG',
    'SGMO': 'Sangamo Therapeutics, Inc.',
    'SIEGY': 'Siemens AG',
    'SSNLF': 'Samsung Electronics Co., Ltd.',
    'STLA': 'Stellantis N.V.',
    'SSYS': 'Stratasys Ltd.',
    'TMO': 'Thermo Fisher Scientific Inc.',
    'TSLA': 'Tesla, Inc.',
    'TXG': '10x Genomics, Inc.',
    'VERV': 'Verve Therapeutics, Inc.',
    'VEOEY': 'Veolia Environnement S.A.',
    'VRTX': 'Vertex Pharmaceuticals Incorporated',
    'VOR': 'Vor Biopharma Inc.',
    'VWAGY': 'Volkswagen AG',
    'XYL': 'Xylem Inc.',
    'ZTCOF': 'ZTE Corporation',
    '6701.T': 'NEC Corporation',
    '2454.TW': 'MediaTek Inc.',
    'GNENY': 'GAC Group',
    'LAB': 'Standard BioTools Inc.',
    'CHKP': 'Check Point Software Technologies Ltd.',
    'SAICF': 'SAIC Motor Corporation Limited',
    'VOLAF': 'Volvo Car AB',
    # Removed duplicate and conflicting entries
}


class MarketSegmentGrowthCandidateFinder:
    def __init__(self, fmp_api_key: str):
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)
        self.analyst_ratings_loader = FmpAnalystRatingsLoader(fmp_api_key)
        self.price_target_loader = FmpPriceTargetLoader(fmp_api_key)
        self.inst_own_loader = FmpInstOwnDataLoader(fmp_api_key)
        self.estimate_loader = FmpAnalystEstimatesLoader(fmp_api_key)
        self.inst_own_loader = FmpInstOwnDataLoader(fmp_api_key)

    def find_candidates(self):
        logi("Finding market segment growth candidates...")

        # Set start and end dates
        start_date = datetime.today() - timedelta(days=400)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date = datetime.today()
        end_date_str = end_date.strftime("%Y-%m-%d")

        # Iterate through the growth market segments and gather data
        results = []

        for market_segment in market_segment_info:
            logi(f"Now processing market segment: {market_segment['name']}...")

            symbol_list = market_segment['symbol_list']
            # Iterate through the symbol list and fetch data
            for symbol in symbol_list:
                logi(f"Now processing symbol {symbol}...")

                # Look up company name
                company_name = symbol_company_name_map.get(symbol, "")

                # Initialize KPIs
                stats = {
                    "symbol": symbol,
                    "company_name": company_name,
                    "market_segment": market_segment['name'],
                    "market_segment_cagr": market_segment['CAGR'],
                    "future_market_size": market_segment['future_market_size'],
                    "price_to_sales": 0,
                    "price_to_earnings": 0,
                    "quick_ratio": 0,
                    "return_on_equity": 0,
                    "debt_to_equity": 0,
                    "free_cashflow_per_share": 0,
                    "operating_profit_margin": 0,
                    "current_ratio": 0,
                    "analyst_rating_score": 0,
                    "avg_price_target_change_percent": 0,
                    "price_target_coefficient_variation": 0,
                    "num_price_target_analysts": 0,
                    "avg_revenue_growth": 0,
                    "avg_net_income_growth": 0,
                    "investors_holding": 0,
                    "investors_put_call_ratio": 0
                }

                # Fetch financial ratios
                financial_ratios_df = self.fmp_data_loader.get_financial_ratios(symbol, period="annual")
                if financial_ratios_df is None or len(financial_ratios_df) == 0:
                    logi(f"No financial ratios available for {symbol}")
                    continue

                # Convert date to datetime format
                financial_ratios_df['date'] = pd.to_datetime(financial_ratios_df['date'], errors='coerce')
                # Sort to get latest dates
                financial_ratios_df.sort_values(by=['date'], ascending=False, inplace=True)

                # Get ratios
                stats['price_to_sales'] = financial_ratios_df['priceToSalesRatio'].iloc[0]
                stats['price_to_earnings'] = financial_ratios_df['priceEarningsRatio'].iloc[0]
                stats['quick_ratio'] = financial_ratios_df['quickRatio'].iloc[0]
                stats['return_on_equity'] = financial_ratios_df['returnOnEquity'].iloc[0]
                stats['debt_to_equity'] = financial_ratios_df['debtEquityRatio'].iloc[0]
                stats['free_cashflow_per_share'] = financial_ratios_df['freeCashFlowPerShare'].iloc[0]
                stats['operating_profit_margin'] = financial_ratios_df['operatingProfitMargin'].iloc[0]
                stats['current_ratio'] = financial_ratios_df['currentRatio'].iloc[0]

                # Fetch analyst ratings
                analyst_ratings_df = self.analyst_ratings_loader.fetch([symbol], num_lookback_days=60)
                if analyst_ratings_df is not None and not analyst_ratings_df.empty:
                    stats['analyst_rating_score'] = analyst_ratings_df['analyst_rating_score'].iloc[0]

                # Fetch revenue growth
                growth_df = self.fmp_data_loader.get_income_growth(symbol, period="annual")
                if growth_df is None or len(growth_df) == 0:
                    continue
                growth_df.replace([np.inf, -np.inf], 0, inplace=True)

                # Filter records for the last 3 years
                growth_start_date = datetime.today() - timedelta(days=365 * 3)
                growth_df['date'] = pd.to_datetime(growth_df['date'], errors='coerce')
                growth_df = growth_df[growth_df['date'] > growth_start_date]
                if len(growth_df) == 0:
                    continue

                # Calculate stats
                stats['avg_revenue_growth'] = growth_df['growthRevenue'].mean()
                stats['avg_net_income_growth'] = growth_df['growthNetIncome'].mean()

                # Load prices
                prices_df = self.fmp_data_loader.fetch_daily_prices_by_date(
                    symbol, start_date_str, end_date_str, cache_data=True, cache_dir=CACHE_DIR)
                if prices_df is None or prices_df.empty:
                    logi(f"No price data available for {symbol}")
                    continue
                prices_df.reset_index(inplace=True)

                # Fetch price targets
                prices_dict = {symbol: prices_df}
                price_target_df = self.price_target_loader.load([symbol], prices_dict, lookback_days=90)
                if price_target_df is not None and not price_target_df.empty:
                    stats['avg_price_target_change_percent'] = price_target_df['avg_price_target_change_percent'].iloc[0]
                    stats['price_target_coefficient_variation'] = price_target_df['price_target_coefficient_variation'].iloc[0]
                    stats['num_price_target_analysts'] = price_target_df['num_price_target_analysts'].iloc[0]

                # Fetch analyst estimates
                estimates_df, estimate_results = self.estimate_loader.load(symbol, "annual")
                if estimate_results:
                    stats['avg_estimated_revenue_change_percent'] = estimate_results['avg_revenue_change_percent']
                    stats['estimated_revenue_change_coefficient_variation'] = estimate_results['revenue_change_coefficient_variation']
                    stats['avg_num_analysts_estimates'] = estimate_results['avg_num_analysts']

                # Fetch institutional ownership data if enabled
                if USE_INSTITUTIONAL_OWNERSHIP_API:
                    inst_own_df = self.fmp_data_loader.fetch_institutional_ownership_changes(symbol,
                                                                                             include_current_quarter=True)
                    if inst_own_df is not None and not inst_own_df.empty:
                        stats['investors_holding'] = inst_own_df['investorsHolding'].iloc[0]
                        stats['investors_total_invested'] = inst_own_df['totalInvested'].iloc[0]
                        stats['investors_put_call_ratio'] = inst_own_df['putCallRatio'].iloc[0]

                results.append(stats)

        # Convert stats to dataframe
        results_df = pd.DataFrame(results)
        if len(results_df) == 0:
            logi("No results found")
            return

        # List of KPIs to be used in the weighted score
        kpi_columns = [
            'price_to_sales',
            'price_to_earnings',
            'quick_ratio',
            'return_on_equity',
            'debt_to_equity',
            'free_cashflow_per_share',
            'operating_profit_margin',
            'current_ratio',
            'analyst_rating_score',
            'avg_price_target_change_percent',
            'avg_revenue_growth',
            'avg_net_income_growth',
            'investors_holding',
        ]

        # KPIs where lower values are better -> need to be inverted
        inverse_kpis = [
            'price_to_sales',
            'price_to_earnings',
            'debt_to_equity',
        ]

        # Handle missing values by filling with mean values
        results_df[kpi_columns] = results_df[kpi_columns].fillna(results_df[kpi_columns].mean())

        # Invert KPIs where lower is better
        epsilon = 1e-6  # small constant to prevent division by zero
        for kpi in inverse_kpis:
            results_df[kpi] = 1 / (results_df[kpi] + epsilon)

        # Normalize the KPIs using Min-Max scaling
        scaler = MinMaxScaler()
        normalized_kpis = scaler.fit_transform(results_df[kpi_columns])
        normalized_kpis_df = pd.DataFrame(normalized_kpis, columns=kpi_columns)

        # Assign weights to each KPI
        weights = {
            'price_to_sales': 0.02,
            'price_to_earnings': 0.02,
            'quick_ratio': 0.03,
            'return_on_equity': 0.10,
            'debt_to_equity': 0.01,
            'free_cashflow_per_share': 0.10,
            'operating_profit_margin': 0.10,
            'current_ratio': 0.02,
            'analyst_rating_score': 0.10,
            'avg_price_target_change_percent': 0.05,
            'avg_revenue_growth': 0.20,
            'avg_net_income_growth': 0.15,
            'investors_holding': 0.20
        }

        # Apply weights to each normalized KPI
        for kpi in kpi_columns:
            normalized_kpis_df[kpi] = normalized_kpis_df[kpi] * weights[kpi]

        # Calculate the weighted score by summing the weighted KPIs
        results_df['weighted_score'] = normalized_kpis_df.sum(axis=1)

        # Round KPI values to 2 decimals
        results_df[kpi_columns] = results_df[kpi_columns].round(2)

        # Round the weighted score to 4 decimals for precision
        results_df['weighted_score'] = results_df['weighted_score'].round(2)

        # Sort by market segment and weighted score
        candidates_df = results_df.sort_values(by=['future_market_size', 'weighted_score'], ascending=[False, False])

        # Store the results to a CSV file
        os.makedirs(CANDIDATES_DIR, exist_ok=True)
        file_name = f"growth_market_sector_candidates_{datetime.today().strftime('%Y-%m-%d')}.csv"
        store_csv(CANDIDATES_DIR, file_name, candidates_df)

        logi("Done with growth market sector candidate analysis.")

