import streamlit as st

import pyarrow_client


table = pyarrow_client.get_data()

st.dataframe(table)
