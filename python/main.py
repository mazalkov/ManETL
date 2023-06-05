from arcticdb import Arctic
import streamlit as st
import pandas as pd
import click


data = pd.read_csv("data/AAPL.csv")

st.dataframe(data.head(10))

@click.command()
@click.option("--arg", default="Hello World", help="Test argument")
def etl(arg):
    print(f"The pipeline was run with {arg}")


if __name__ == "__main__":
    etl()
