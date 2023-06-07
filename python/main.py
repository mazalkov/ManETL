import streamlit as st
import pandas as pd


data = pd.read_csv("data/AAPL.csv")

st.dataframe(data.head(10))
