import pandas as pd
import numpy as np



def VWAP(high, low, close):
    typical_price = np.mean([high, low, close])
    return typical_price



def daily_returns(Close, Volume):
    total_returns = (Close * Volume)
    return total_returns


#IMA_df['VWAP'] = IMA_df.apply(lambda x: VWAP(x["High"], x["Low"], x["Close"]), axis=1)