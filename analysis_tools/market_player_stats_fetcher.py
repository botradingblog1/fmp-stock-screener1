import numpy as np
import pandas as pd
from datetime import datetime
from utils.log_utils import *
from utils.report_utils import *
from botrading.data_loaders.fmp_data_loader import FmpDataLoader
from data_loaders.fmp_price_target_loader import FmpPriceTargetLoader
from data_loaders.fmp_inst_own_data_loader import FmpInstOwnDataLoader
from data_loaders.fmp_analyst_estimates_loader import FmpAnalystEstimatesLoader
from data_loaders.fmp_company_outlook_loader import FmpCompanyOutlookLoader
import os

"""
Fetches key stats of major market players
"""

USE_INSTITUTIONAL_OWNERSHIP_API = True
RESULTS_DIR = "results"
market_sector_list = [
    {
        "name": "Blockchain Technology",
        "current_market_size": 7,
        "future_market_size": 67,
        "cagr": 60,
        "portfolio_allocation": 10.0,
        "notes": "Driven by finance, supply chain, healthcare, and decentralized finance (DeFi), with strong enterprise adoption.",
        "market_leaders": ["COIN", "MSTR", "SQ", "RIOT", "MARA"]
    },
    {
        "name": "Private 5G Networks",
        "current_market_size": 4,
        "future_market_size": 22,
        "cagr": 46,
        "portfolio_allocation": 5.0,
        "notes": "Emerging market enabling high-speed, secure connectivity for industries like manufacturing, healthcare, and logistics.",
        "market_leaders": ["NOK", "ERIC", "CSCO", "HPE", "QCOM"]
    },
    {
        "name": "Digital Twin",
        "current_market_size": 15,
        "future_market_size": 119,
        "cagr": 45.7,
        "portfolio_allocation": 10.0,
        "notes": "Growing rapidly due to industrial IoT, simulation, and predictive maintenance applications.",
        "market_leaders": ["ANSS", "PTC", "MSFT", "GE", "SIEGY"]
    },
    {
        "name": "General AI",
        "current_market_size": 235,
        "future_market_size": 1100,
        "cagr": 37.9,
        "portfolio_allocation": 20.0,
        "notes": "Large and fast-growing market with applications across industries in automation, analytics, and autonomous systems.",
        "market_leaders": ["MSFT", "GOOGL", "NVDA", "IBM", "META"]
    },
    {
        "name": "Quantum Computing",
        "current_market_size": 0.5,
        "future_market_size": 4.2,
        "cagr": 35,
        "portfolio_allocation": 5.0,
        "notes": "High potential in cryptography, drug discovery, logistics, and optimization; early-stage technology.",
        "market_leaders": ["IBM", "GOOGL", "MSFT", "AMZN", "RGTI"]
    },
    {
        "name": "Antibody-Drug Conjugates",
        "current_market_size": 14.3,
        "future_market_size": 49,
        "cagr": 28,
        "portfolio_allocation": 0.0,
        "notes": "Focused on targeted cancer therapies, offering significant potential for growth.",
        "market_leaders": ["DSNKY", "RHHBY", "GILD", "PFE", "MRK"]
    },
    {
        "name": "Artificial Intelligence (AI) in Life Sciences Market",
        "current_market_size": 6.5,
        "future_market_size": 33.5,
        "cagr": 27.9,
        "portfolio_allocation": 0.0,
        "notes": "Small but expanding market, focused on drug discovery, genomics, diagnostics, and personalized medicine.",
        "market_leaders": ["NVDA", "GOOGL", "MSFT", "IBM", "ORCL"]
    },
    {
        "name": "Wearable Devices",
        "current_market_size": 45.2,
        "future_market_size": 187,
        "cagr": 27.5,
        "portfolio_allocation": 10.0,
        "notes": "Includes health monitoring and fitness devices, with growth in medical-grade applications for chronic disease management.",
        "market_leaders": ["AAPL", "GOOGL", "ABT", "DXCM", "GRMN"]
    },
    {
        "name": "Edge Computing",
        "current_market_size": 5,
        "future_market_size": 40,
        "cagr": 27.5,
        "portfolio_allocation": 0.0,
        "notes": "Essential for IoT, with applications in low-latency processing for autonomous vehicles, smart cities, and manufacturing.",
        "market_leaders": ["CSCO", "INTC", "AMZN", "MSFT", "HPE"]
    },
    {
        "name": "Augmented Reality (AR) & Virtual Reality (VR)",
        "current_market_size": 21.1,
        "future_market_size": 80,
        "cagr": 25,
        "portfolio_allocation": 5.0,
        "notes": "Growing in gaming, education, healthcare, and training, with metaverse potential.",
        "market_leaders": ["META", "AAPL", "GOOGL", "MSFT", "NVDA"]
    },
    {
        "name": "3D Printing (Additive Manufacturing)",
        "current_market_size": 16,
        "future_market_size": 48,
        "cagr": 20,
        "portfolio_allocation": 0.0,
        "notes": "Expanding in aerospace, healthcare, automotive, and construction, offering customization and efficiency.",
        "market_leaders": ["DDD", "SSYS", "PRLB", "HPQ", "DM"]
    },
    {
        "name": "Robotics",
        "current_market_size": 43.8,
        "future_market_size": 159,
        "cagr": 19.8,
        "portfolio_allocation": 5.0,
        "notes": "Expanding in industrial, service, and medical robotics, with strong demand in manufacturing and healthcare.",
        "market_leaders": ["ISRG", "ABB", "ROK", "FANUY", "TER"]
    },
    {
        "name": "Cloud Services",
        "current_market_size": 611.4,
        "future_market_size": 1806,
        "cagr": 18.49,
        "portfolio_allocation": 10.0,
        "notes": "Growth driven by digital transformation, enterprise adoption, and scalable storage/computing needs.",
        "market_leaders": ["AMZN", "MSFT", "GOOGL", "ORCL", "IBM", "ESTC", "CRWD"]
    },
    {
        "name": "Electric Vehicles",
        "current_market_size": 400,
        "future_market_size": 1800,
        "cagr": 18,
        "portfolio_allocation": 5.0,
        "notes": "Strong growth with government support and consumer demand for EVs, including battery electric and hybrid models.",
        "market_leaders": ["TSLA", "GM", "TM", "LI"]
    },
    {
        "name": "Renewable Energy",
        "current_market_size": 1200,
        "future_market_size": 2000,
        "cagr": 17.2,
        "portfolio_allocation": 10.0,
        "notes": "Fueled by climate goals and advances in solar, wind, and energy storage.",
        "market_leaders": ["NEE", "ENPH", "FSLR", "BEP", "SEDG"]
    },
    {
        "name": "Food Tech (Alternative Proteins & Lab-Grown Meat)",
        "current_market_size": 4.6,
        "future_market_size": 18,
        "cagr": 17,
        "portfolio_allocation": 0.0,
        "notes": "Driven by demand for sustainable and ethical food options, with advancements in plant-based and lab-grown proteins.",
        "market_leaders": ["BYND", "TSN", "BRFS", "INGR", "KHC"]
    },
    {
        "name": "Genomics & Gene Editing",
        "current_market_size": 5,
        "future_market_size": 19.3,
        "cagr": 15,
        "portfolio_allocation": 0.0,
        "notes": "Expanding with CRISPR and gene-editing technologies for agriculture, medicine, and disease treatment.",
        "market_leaders": ["CRSP", "EDIT", "ILMN", "NVTA", "NTLA"]
    },
    {
        "name": "Biometrics",
        "current_market_size": 16.2,
        "future_market_size": 36,
        "cagr": 15,
        "portfolio_allocation": 0.0,
        "notes": "Growth due to demand for secure authentication across mobile devices, financial services, and government.",
        "market_leaders": ["AAPL", "IDEXY", "NXPI", "V", "MA"]
    },
    {
        "name": "Telemedicine",
        "current_market_size": 72.4,
        "future_market_size": 185,
        "cagr": 15,
        "portfolio_allocation": 0.0,
        "notes": "Rising healthcare access and convenience, with growth in remote diagnostics and patient monitoring.",
        "market_leaders": ["TDOC", "AMWL", "CVS", "UNH", "HIMS"]
    },
 {
        "name": "Global Payment Processing",
        "current_market_size": 80,
        "future_market_size": 145,
        "cagr": 13.7,
        "portfolio_allocation": 0.0,
        "notes": "Driven by e-commerce and digital wallets, with rising demand in emerging markets.",
        "market_leaders": ["V", "MA", "PYPL", "SQ", "GPN"]
    },
    {
        "name": "Shared Autonomous Vehicles",
        "current_market_size": 500,
        "future_market_size": 2000,
        "cagr": 13.5,
        "portfolio_allocation": 0.0,
        "notes": "Self-driving taxis and delivery vehicles, fueled by advancements in autonomous tech and AI.",
        "market_leaders": ["TSLA", "GOOGL", "GM", "UBER", "LYFT"]
    },
    {
        "name": "Esports & Online Gaming",
        "current_market_size": 71.3,
        "future_market_size": 156,
        "cagr": 12,
        "portfolio_allocation": 0.0,
        "notes": "Growing with online gaming, esports, and digital streaming, supported by internet penetration and mobile access.",
        "market_leaders": ["ATVI", "TTWO", "EA", "NTDOY", "SE"]
    },
    {
        "name": "Health and Wellness Technology",
        "current_market_size": 46,
        "future_market_size": 92,
        "cagr": 12,
        "portfolio_allocation": 0.0,
        "notes": "Includes health apps, mental wellness, and tech-enabled fitness, with growing interest in personal health.",
        "market_leaders": ["AAPL", "PEAR", "HIMS", "PTON", "FIT"]
    },
    {
        "name": "Agricultural Technology (AgriTech)",
        "current_market_size": 13,
        "future_market_size": 28.9,
        "cagr": 12,
        "portfolio_allocation": 0.0,
        "notes": "Growth in smart farming, precision agriculture, and automation, addressing food security and climate challenges.",
        "market_leaders": ["DE", "AGCO", "RAVN", "Corteva (CTVA)", "CNHI"]
    },
    {
        "name": "Internet of Things - Worldwide",
        "current_market_size": 949,
        "future_market_size": 1560,
        "cagr": 10.49,
        "portfolio_allocation": 0.0,
        "notes": "Expanding in smart homes, industrial IoT, and connected devices.",
        "market_leaders": ["CSCO", "INTC", "GOOGL", "MSFT", "AMZN"]
    },
    {
        "name": "Semiconductors - Worldwide",
        "current_market_size": 607.4,
        "future_market_size": 980.8,
        "cagr": 10.06,
        "portfolio_allocation": 5.0,
        "notes": "Essential for electronics, IoT, AI, and EVs, with demand across industries despite cyclical nature.",
        "market_leaders": ["TSM", "NVDA", "AMD", "INTC", "ASML", "LRCX"]
    },
    {
        "name": "Cybersecurity",
        "current_market_size": 185.7,
        "future_market_size": 271.9,
        "cagr": 7.92,
        "portfolio_allocation": 0.0,
        "notes": "Steady growth due to rising cyber threats and demand for security in digital/IoT systems.",
        "market_leaders": ["CRWD", "PANW", "ZS", "OKTA", "NET"]
    }
]


class MarketLeaderStatsFetcher:
    def __init__(self, fmp_api_key: str):
        self.fmp_data_loader = FmpDataLoader(fmp_api_key)
        self.price_target_loader = FmpPriceTargetLoader(fmp_api_key)
        self.inst_own_loader = FmpInstOwnDataLoader(fmp_api_key)
        self.estimate_loader = FmpAnalystEstimatesLoader(fmp_api_key)
        self.company_outlook_loader = FmpCompanyOutlookLoader(fmp_api_key)

    def fetch_stats(self):

        # Initialize empty results list
        results = []

        # Iterate through the market sectors
        for market_sector in market_sector_list:
            market_name = market_sector.get('name', "")
            cagr = market_sector.get('cagr', "")
            portfolio_allocation = market_sector.get('portfolio_allocation', 0)
            market_leaders = market_sector.get('market_leaders', [])

            # Optionally, skip markets without portfolio allocation
            if portfolio_allocation == 0:
                continue

            for symbol in market_leaders:
                logd(f"Processing {symbol}...")

                # Fetch company outlook
                outlook_dict = self.company_outlook_loader.load(symbol)
                if outlook_dict is None:
                    logw(f"No data for symbol {symbol}")
                    continue

                # Initialize a dictionary for each symbol
                symbol_data = {
                    'symbol': symbol,
                    'market_name': market_name,
                    'cagr': cagr,
                    'portfolio_allocation': portfolio_allocation,
                    'company_name': outlook_dict.get('profile', {}).get('companyName', ''),
                    'description': outlook_dict.get('profile', {}).get('description', ''),
                    'website': outlook_dict.get('profile', {}).get('website', ''),
                    'mktCap': outlook_dict.get('profile', {}).get('mktCap', 0) / 1000000000,
                }

                # Add ratios data
                ratios = outlook_dict.get('ratios', {})
                symbol_data.update({
                    'price_to_sales': round(ratios.get('priceToSalesRatioTTM'), 2),
                    'price_to_earnings': round(ratios.get('priceEarningsRatioTTM'), 2),
                    'debt_to_equity': round(ratios.get('debtEquityRatioTTM'), 2),
                    'return_on_equity': round(ratios.get('returnOnEquityTTM'), 2)
                })

                # Add income data
                income_data = outlook_dict.get('annual_income_data', [])
                income_stats_df = pd.DataFrame(income_data)
                income_stats_df['date'] = pd.to_datetime(income_stats_df['date'], errors="coerce")
                income_stats_df.sort_values(by='date', ascending=True, inplace=True)

                # Get last annual revenue in billions
                symbol_data['last_revenue'] = round(income_stats_df['revenue'].iloc[-1] / 1000000000,
                                              2) if 'revenue' in income_stats_df else np.nan

                # Add price target data
                price_target_info = self.price_target_loader.load(symbol)
                if price_target_info:
                    symbol_data['avg_price_target_change_percent'] = price_target_info.get('avg_price_target_change_percent', None)
                    symbol_data['current_price'] = outlook_dict.get('profile', {}).get('price', 0.0)

                # Add institutional ownership data if enabled
                if USE_INSTITUTIONAL_OWNERSHIP_API:
                    inst_own_data = self.inst_own_loader.load_for_symbol(symbol)
                    if inst_own_data:
                        symbol_data.update({
                            'investors_holding': inst_own_data.get('investors_holding', None),
                            'total_invested': inst_own_data.get('total_invested', None) / 1000000000,
                            'investors_put_call_ratio': inst_own_data.get('investors_put_call_ratio', None)
                        })

                # Append symbol data to results
                results.append(symbol_data)

        # Create DataFrame from results list
        column_list = [
            'market_name', 'cagr', 'portfolio_allocation', 'symbol', 'company_name', 'description',
            'website', 'mktCap', 'last_revenue', 'price_to_sales', 'price_to_earnings', 'debt_to_equity',
            'return_on_equity', 'current_price', 'avg_price_target_change_percent'
        ]

        if USE_INSTITUTIONAL_OWNERSHIP_API:
            column_list.append('investors_holding')
            column_list.append('total_invested')
            column_list.append('investors_put_call_ratio')

        output_df = pd.DataFrame(results, columns=column_list)

        # Save the final DataFrame as CSV
        os.makedirs(RESULTS_DIR, exist_ok=True)
        output_df.to_csv(os.path.join(RESULTS_DIR, 'market_leader_stats.csv'), index=False)