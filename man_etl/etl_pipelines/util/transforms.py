import pandas as pd
import numpy as np
import math
import yfinance as yf
import arcticdb as Arctic

bucket = Arctic("s3://s3.eu-west-2.amazonaws.com:manstocks?region=eu-west-2&access=AKIAVHAD6ZB4RYHDPBWA&secret=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")
lib = bucket['etl_demo']
raw_symbols = [symbol for symbol in lib.list_symbols() if '_TRANSFORMED' not in symbol]
transformed_symbols = [df_name for df_name in lib.list_symbols() if '_TRANSFORMED' in df_name]
transformed_dfs_dict = dict(zip(raw_symbols, [lib.read(df_name).data for df_name in transformed_symbols]))
raw_dfs = [lib.read(symbol).data for symbol in raw_symbols] 


def calc_VWAP(high, low, close):
    typical_price = np.mean([high, low, close])
    return typical_price


# VWAP = Cumulative Typical Price x Volume/Cumulative Volume
# Where Typical Price = High price + Low price + Closing Price/3
# Cumulative = total since the trading session opened.


def calc_daily_returns(Close, Volume):
    total_returns = Close * Volume
    return total_returns


def calc_volatility(raw_dfs):        
    daily_volatility = raw_dfs['Daily Returns'].std()
    weekly_volatility = raw_dfs['Daily Returns'].std() * (252 ** 0.5)
    annual_volatility = raw_dfs['Daily Returns'].std() * (252 ** 0.5)

vol_df = pd.DataFrame(raw_dfs, index = ["Daily", "Weekly", "Annual"])


calcs = {"VWAP": (lambda x: calc_VWAP(x["High"], x["Low"], x["Close"])), "Daily Returns": (lambda x: calc_daily_returns(x["Close"], x["Volume"]))}

calc_vol = {"VOLATILITY":(lambda x: calc_volatility(x["Daily Returns"]))}