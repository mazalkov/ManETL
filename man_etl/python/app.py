import streamlit as st
# import pyarrow_client
from arcticdb import Arctic

# table = pyarrow_client.get_data()
# st.dataframe(table)

## Load data from arctic

bucket = Arctic("s3://s3.eu-west-2.amazonaws.com:manstocks?region=eu-west-2&access=AKIAVHAD6ZB4RYHDPBWA&secret=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")
lib = bucket['etl_demo']
raw_symbols = [symbol for symbol in lib.list_symbols() if '_TRANSFORMED' not in symbol]
transformed_symbols = [df_name for df_name in lib.list_symbols() if '_TRANSFORMED' in df_name]
transformed_dfs_dict = dict(zip(raw_symbols, [lib.read(df_name).data for df_name in transformed_symbols]))

## Streamlit UI

st.title("Man ETL Dashboard")

if "chosen_symbol" not in st.session_state:
    st.session_state["chosen_symbol"] = raw_symbols[0]

st.selectbox("Choose symbol:", key="chosen_symbol", options=raw_symbols)


chosen_symbol = st.session_state["chosen_symbol"]
st.write(f"Volume data for {chosen_symbol}")
st.line_chart(transformed_dfs_dict[chosen_symbol]["Volume"])


