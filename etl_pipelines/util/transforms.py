import pandas as pd
import numpy as np

calcs = {
    "calc_VWAP": calc_VWAP,
    "calc_daily_returns": calc_daily_returns
}

def calc_VWAP(high, low, close):
    typical_price = np.mean([high, low, close])
    return typical_price

# VWAP = Cumulative Typical Price x Volume/Cumulative Volume
#
# Where Typical Price = High price + Low price + Closing Price/3
#
# Cumulative = total since the trading session opened.


def calc_daily_returns(Close, Volume):
    total_returns = (Close * Volume)
    return total_returns


#IMA_df['VWAP'] = IMA_df.apply(lambda x: VWAP(x["High"], x["Low"], x["Close"]), axis=1)