import streamlit as st
# import pyarrow_client
import yfinance as yf

# table = pyarrow_client.get_data()
# st.dataframe(table)

INTERVAL = "1h"
PERIOD = "1d"
TICKERS = ["AAPL", "GOOG", "TSLA"]
YF_DATA = dict()
for ticker in TICKERS:
    YF_DATA[ticker] = yf.Ticker(ticker)

st.title("Man ETL Dashboard")

if "chosen_ticker" not in st.session_state:
    st.session_state["chosen_ticker"] = TICKERS[0]

st.selectbox("Choose ticker:", key="chosen_ticker", options=TICKERS)

ticker = st.session_state["chosen_ticker"]
data = YF_DATA[ticker].history(period=PERIOD, interval=INTERVAL)
st.write(f"Volume data for {YF_DATA[ticker].info['longName']} for {PERIOD}")
st.line_chart(data["Volume"])

