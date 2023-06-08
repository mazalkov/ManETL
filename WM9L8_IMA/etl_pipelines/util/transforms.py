import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt
import yfinance as yf

IMA_df = pd.read_csv("data/AAPL.csv")

IMA_df["daily_returns"] = (IMA_df["Close"].pct_change()) * 100


def VWAP(high, low, close):
    typical_price = np.mean([high, low, close])
    return typical_price


def daily_returns(Close, Volume):
    total_returns = Close * Volume
    return total_returns


# IMA_df['VWAP'] = IMA_df.apply(lambda x: VWAP(x["High"], x["Low"], x["Close"]), axis=1)


daily_volatility_apple = IMA_df["daily_returns"].std()
print("Daily volatility:")
print("Apple: ", "{:.2f}%".format(daily_volatility_apple))


monthly_volatility_apple = math.sqrt(21) * daily_volatility_apple
print("Monthly volatility:")
print("Apple: ", "{:.2f}%".format(monthly_volatility_apple))


annual_volatility_apple = math.sqrt(252) * daily_volatility_apple
print("Annual volatility:")
print("Apple: ", "{:.2f}%".format(annual_volatility_apple))
