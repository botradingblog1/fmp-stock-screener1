import os
import pandas as pd
from enum import Enum


class MarketIndex(Enum):
    """
    List of common market indexes like NASDAQ 100, Dow Jones or S&P 500
    """
    NASDAQ_100 = 'NASDAQ_100'
    SNP_500 = 'SNP_500'
    DJI = 'DJI'
    RUSSELL_1000 = 'RUSSELL_1000'
    RUSSELL_2000 = 'RUSSELL_2000'
    UNKNOWN = 'UNKNOWN'


class MarketSymbolLoader:
    """
    MarketSymbolLoader provides the ability to load a list of stock symbols for different market indexes

    Attributes:
        None
    """
    def __init__(self):
        pass

    def fetch_nasdaq100_symbols(self, cache_file=False, cache_dir="cache", file_name="nasdaq100_symbols.csv"):
        """
        Fetches the list of NASDAQ 100 symbols.

        Parameters:
            cache_file (bool): Flag to indicate if the list should be cached
            cache_dir (str): Cache directory
            file_name (str): Cache file name

        Returns:
            DataFrame: dataframe with list of symbols and additional info.
        """
        wiki_url = 'https://en.wikipedia.org/wiki/Nasdaq-100'
        return self._fetch_symbols(wiki_url, 4, cache_file, cache_dir, file_name, 'Ticker')

    def fetch_dji_symbols(self, cache_file=False, cache_dir="cache", file_name="dji_symbols.csv"):
        """
        Fetches the list of Dow Jones Industrial Average (DJI) symbols.

        Parameters:
            cache_file (bool): Flag to indicate if the list should be cached
            cache_dir (str): Cache directory
            file_name (str): Cache file name

        Returns:
            DataFrame: dataframe with list of symbols and additional info.
        """
        wiki_url = 'https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average'
        return self._fetch_symbols(wiki_url, 1, cache_file, cache_dir, file_name, 'Symbol')

    def fetch_sp500_symbols(self, cache_file=False, cache_dir="cache", file_name="sp500_symbols.csv"):
        """
        Fetches the list of S&P 500 symbols.

        Parameters:
            cache_file (bool): Flag to indicate if the list should be cached
            cache_dir (str): Cache directory
            file_name (str): Cache file name

        Returns:
            DataFrame: dataframe with list of symbols and additional info.
        """
        wiki_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        return self._fetch_symbols(wiki_url, 0, cache_file, cache_dir, file_name, 'Symbol')

    def fetch_russell1000_symbols(self, cache_file=False, cache_dir="cache", file_name="russell1000_symbols.csv"):
        """
        Fetches the list of Russell 1000 symbols.

        Parameters:
            cache_file (bool): Flag to indicate if the list should be cached
            cache_dir (str): Cache directory
            file_name (str): Cache file name

        Returns:
            DataFrame: dataframe with list of symbols and additional info.
        """
        wiki_url = 'https://en.wikipedia.org/wiki/Russell_1000_Index'
        return self._fetch_symbols(wiki_url, 2, cache_file, cache_dir, file_name, 'Symbol')

    def fetch_russell2000_symbols(self, cache_file=False, cache_dir="cache", file_name="russell2000_symbols.csv"):
        """
        Fetches the list of Russell 2000 symbols.

        Parameters:
            cache_file (bool): Flag to indicate if the list should be cached
            cache_dir (str): Cache directory
            file_name (str): Cache file name

        Returns:
            DataFrame: dataframe with list of symbols and additional info.
        """
        wiki_url = 'https://en.wikipedia.org/wiki/Russell_2000_Index'
        return self._fetch_symbols(wiki_url, 2, cache_file, cache_dir, file_name, 'Symbol')

    def _fetch_symbols(self, url, table_index, cache_file, cache_dir, file_name, ticker_column):
        try:
            cache_path = os.path.join(cache_dir, file_name)
            # Create cache dir if needed
            if cache_file and not os.path.exists(cache_path):
                os.makedirs(cache_dir, exist_ok=True)

            # Try to load file from cache
            if os.path.exists(cache_path) and cache_file:
                symbols_df = pd.read_csv(cache_path)
                return symbols_df
            else:
                table = pd.read_html(url)
                symbols_df = table[table_index]
                symbols_df.rename(columns={ticker_column: "symbol"}, inplace=True)
                # Cache file
                if cache_file:
                    symbols_df.to_csv(cache_path, index=False)
                return symbols_df
        except Exception as e:
            print(f"Failed to fetch symbols from {url}, error: {str(e)}")
            return None

    def fetch_symbols(self, market_index: MarketIndex, cache_file=False, cache_dir="cache"):
        """
        Fetches the list of symbols for the specified market index.

        Parameters:
            market_index (MarketIndex): The market index to fetch symbols for.
            cache_file (bool): Flag to indicate if the list should be cached.
            cache_dir (str): Cache directory.

        Returns:
            DataFrame: dataframe with list of symbols and additional info.
        """
        if market_index == MarketIndex.NASDAQ_100:
            return self.fetch_nasdaq100_symbols(cache_file, cache_dir)
        elif market_index == MarketIndex.SNP_500:
            return self.fetch_sp500_symbols(cache_file, cache_dir)
        elif market_index == MarketIndex.DJI:
            return self.fetch_dji_symbols(cache_file, cache_dir)
        elif market_index == MarketIndex.RUSSELL_1000:
            return self.fetch_russell1000_symbols(cache_file, cache_dir)
        elif market_index == MarketIndex.RUSSELL_2000:
            return self.fetch_russell2000_symbols(cache_file, cache_dir)
        else:
            raise ValueError(f"Unsupported market index: {market_index}")