from arcticdb import Arctic
import streamlit as st
import pandas as pd
import click


data = pd.read_csv("data/AAPL.csv")

st.dataframe(data.head(10))

@click.command()
def etl():
    pass