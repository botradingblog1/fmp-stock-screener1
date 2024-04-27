from utils.log_utils import *
import requests
from utils.string_utils import *
import pandas as pd


class FmpClient:
    """
    Configure FMP client with api key
    """
    def __init__(self, fmp_api_key):
        self._api_key = fmp_api_key

    def fetch_tradable_list(self):
        try:
            url = f"https://financialmodelingprep.com/api/v3/available-traded/list?apikey={self._api_key}"
            response = requests.get(url)

            if response.status_code == 200:
                securities_data = response.json()
                if securities_data:
                    securities_df = pd.DataFrame(securities_data)

                    return securities_df
                return None
            else:
                return None
        except Exception as ex:
            print(ex)
            return None

    def fetch_analyst_ratings(self, symbol):
        try:
            url = f"https://financialmodelingprep.com/api/v3/grade/{symbol}?apikey={self._api_key}"
            response = requests.get(url)

            if response.status_code == 200:
                grades_data = response.json()
                if grades_data:
                    grades_df = pd.DataFrame(grades_data)
                    grades_df['date'] = pd.to_datetime(grades_df['date'], errors='coerce')
                    # Filter out invalid dates (NaT values after conversion)
                    grades_df = grades_df.dropna(subset=['date'])

                    return grades_df
                return None
            else:
                return None
        except Exception as ex:
            loge(ex)
            return None

    def get_company_outlook(self, symbol):
        try:
            url = f"https://financialmodelingprep.com/api/v4/company-outlook?symbol={symbol}&apikey={self._api_key}"
            response = requests.get(url)
            if response.status_code == 200:
                return {"status": 200, "data": response.json(), "message": ""}
            else:
                return {"status": response.status_code, "data": None, "message": "Failed in FMP company outlook"}
        except Exception as ex:
            loge(ex)
            return {"status": 400, "data": None, "message": "Failed in FMP company outlook"}

    def get_press_releases(self, symbol):
        try:
            url = f"https://financialmodelingprep.com/api/v3/press-releases/{symbol}?page=0&apikey={self._api_key}"
            response = requests.get(url)
            if response.status_code == 200:
                #  Modify press releases to capitalize the first word
                data = response.json()
                for item in data:
                    item['title'] = capitalize_first_word(item['title'])
                    item['text'] = capitalize_first_word(item['text'])

                return {"status": 200, "data": data, "message": ""}
            else:
                return {"status": response.status_code, "data": None, "message": "Failed in FMP press releases"}
        except Exception as ex:
            loge(ex)
            return {"status": 400, "data": None, "message": "Failed in FMP press releases"}

    def get_social_sentiment(self, symbol):
        try:
            url = f"https://financialmodelingprep.com/api/v4/historical/social-sentiment?symbol={symbol}&apikey={self._api_key}"
            response = requests.get(url)
            if response.status_code == 200:
                return {"status": 200, "data": response.json(), "message": ""}
            else:
                return {"status": response.status_code, "data": None, "message": "Failed in FMP social sentiment"}
        except Exception as ex:
            loge(ex)
            return {"status": 400, "data": None, "message": "Failed in FMP social sentiment"}


    def get_stock_grades(self, symbol):
        try:
            url = f"https://financialmodelingprep.com/api/v3/grade/{symbol}?limit=20&apikey={self._api_key}"
            response = requests.get(url)
            if response.status_code == 200:
                return {"status": 200, "data": response.json(), "message": ""}
            else:
                return {"status": response.status_code, "data": None, "message": "Failed in FMP stock grade"}
        except Exception as ex:
            loge(ex)
            return {"status": 400, "data": None, "message": "Failed in FMP stock grade"}

    def get_financial_ratios(self, symbol):
        try:
            url = f"https://financialmodelingprep.com/api/v3/ratios/{symbol}?limit=1&apikey={self._api_key}"
            response = requests.get(url)
            if response.status_code == 200:
                return {"status": 200, "data": response.json(), "message": ""}
            else:
                return {"status": response.status_code, "data": None, "message": "Failed in FMP financial ratios"}
        except Exception as ex:
            loge(ex)
            return {"status": 400, "data": None, "message": "Failed in FMP financial ratios"}

    def get_company_profile(self, symbol):
        try:
            url = f"https://financialmodelingprep.com/api/v3/profile//{symbol}?apikey={self._api_key}"
            response = requests.get(url)
            if response.status_code == 200:
                return {"status": 200, "data": response.json(), "message": ""}
            else:
                return {"status": response.status_code, "data": None, "message": "Failed in FMP company profile"}
        except Exception as ex:
            loge(ex)
            return {"status": 400, "data": None, "message": "Failed in FMP company profile"}

    def get_analyst_ratings(self, symbol):
        grades_df = pd.DataFrame()
        try:
            url = f"https://financialmodelingprep.com/api/v3/grade/{symbol}?apikey={self._api_key}"
            response = requests.get(url)

            if response.status_code == 200:
                grades_data = response.json()
                if grades_data:
                    strong_buy_count = 0
                    buy_count = 0
                    outperform_count = 0
                    strong_sell_count = 0
                    sell_count = 0
                    underperform_count = 0

                    # Process grades data
                    grades_df = pd.DataFrame(grades_data)
                    for item in grades_data:
                        if 'newGrade' in item and 'date' in item:
                            date = item['date']
                            new_grade = item['newGrade']
                            if new_grade == "Strong Buy":
                                strong_buy_count += 1
                            elif new_grade == "Buy" or new_grade == "Long-Term Buy" or new_grade == "Conviction Buy":
                                buy_count += 1
                            elif new_grade == "Outperform" or new_grade == "Perform":
                                outperform_count += 1
                            elif new_grade == "Strong Sell":
                                strong_sell_count += 1
                            elif new_grade == "Sell" or new_grade == "Long-Term Sell" or new_grade == "Conviction Sell":
                                sell_count += 1
                            elif new_grade == "Underperform":
                                underperform_count += 1

                        #  Save results in a dataframe
                        grade_row = pd.DataFrame({'date': [date],
                                                  'symbol': [symbol],
                                                  'strong_buy': [strong_buy_count],
                                                  'buy': [buy_count],
                                                  'outperform': [outperform_count],
                                                  'strong_sell': [strong_sell_count],
                                                  'sell': [sell_count],
                                                  'underperform': [underperform_count]})
                        grades_df = pd.concat([grades_df, grade_row], axis=0, ignore_index=True)
                    grades_df['date'] = pd.to_datetime(grades_df['date'])
                return {"status": 200, "data": grades_df, "message": ""}
            else:
                return {"status": response.status_code, "data": None, "message": "Failed in FMP analyst ratings"}
        except Exception as ex:
            loge(ex)
            return {"status": 400, "data": None, "message": "Failed in FMP analyst ratings"}


    def get_sec_filings_data(self, start_date_str, end_date_str):
        try:
            url = f"https://financialmodelingprep.com/api/v4/rss_feed?limit=100&from={start_date_str}&to={end_date_str}&isDone=true&apikey={self._api_key}"
            response = requests.get(url)
            if response.status_code == 200:
                #  Add unique id
                data = response.json()
                i = 0
                for item in data:
                    item['id'] = i
                    i += 1

                return {"status": 200, "data": data, "message": ""}
            else:
                return {"status": response.status_code, "data": None,
                        "message": "Failed in FMP SEC filings data"}
        except Exception as ex:
            loge(ex)
            return {"status": 400, "data": None, "message": "Failed in FMP SEC Filings data"}

    def get_stock_news(self, symbol, limit):
        try:
            url = f"https://financialmodelingprep.com/api/v3/stock_news?tickers={symbol}&limit={limit}&apikey={self._api_key}"
            response = requests.get(url)

            if response.status_code == 200:
                news_data = response.json()
                if news_data:
                    news_df = pd.DataFrame(news_data)
                    news_df['publishedDate'] = pd.to_datetime(news_df['publishedDate'], errors='coerce')
                    # Filter out invalid dates (NaT values after conversion)
                    news_df = news_df.dropna(subset=['publishedDate'])

                    return news_df
                return None
            else:
                return None
        except Exception as ex:
            loge(ex)
            return None

    def get_all_stocks_live_price(self):
        try:
            url = f"https://financialmodelingprep.com/api/v3/stock/real-time-price?apikey={self._api_key}"
            response = requests.get(url)
            if response.status_code == 200:
                #  Add unique id
                data = response.json()
                price_list = []
                if 'stockList' in data:
                    stock_list = data['stockList']
                    i = 0
                    for item in stock_list:
                        price_list.append(item)

                return {"status": 200, "data": price_list, "message": ""}
            else:
                return {"status": response.status_code, "data": None,
                        "message": "Failed in FMP get all stocks live price data"}
        except Exception as ex:
            loge(ex)
            return {"status": 400, "data": None, "message": "Failed in get all stocks live price data"}

