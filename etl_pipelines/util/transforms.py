import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt
import yfinance as yf

IMA_df = pd.read_csv('data/AAPL.csv')

IMA_df['daily_returns']=(IMA_df['Close'].pct_change())*100

def VWAP(high, low, close):
    typical_price = np.mean([high, low, close])
    return typical_price


def daily_returns(Close, Volume):
    total_returns = (Close * Volume)
    return total_returns


#IMA_df['VWAP'] = IMA_df.apply(lambda x: VWAP(x["High"], x["Low"], x["Close"]), axis=1)


def calculate_volatility(IMA_df):
    daily_volatility = IMA_df['Daily_Return'].std()
    monthly_volatility = math.sqrt(21) * daily_volatility
    annual_volatility = math.sqrt(252) * daily_volatility
    
    print('Daily volatility:')
    print('Apple: {:.2f}%'.format(daily_volatility))
    
    print('Monthly volatility:')
    print('Apple: {:.2f}%'.format(monthly_volatility))
    
    print('Annual volatility:')
    print('Apple: {:.2f}%'.format(annual_volatility))

# Assuming IMA_df contains the DataFrame with daily returns column 'daily_returns'
calculate_volatility(IMA_df)
