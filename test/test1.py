import pandas as pd
import pandas_ta as ta
import yfinance as yf
from datetime import datetime, timedelta
from datetime import date

stock = "aapl"
tday = datetime.today()
startDay = tday - timedelta(days=100)
# _df = pd.DataFrame() # Empty DataFrame
df = yf.download(stock,start=startDay,end=tday)
df.ta.supertrend(period=7, multiplier=3)
# OR if you want to automatically apply the results to the DataFrame
df.ta.supertrend(period=7, multiplier=3, append=True)

print('here')