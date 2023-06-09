import streamlit as st
from arcticdb import Arctic
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd

# import pyarrow_client

# table = pyarrow_client.get_data()
# st.dataframe(table)

### Load data from Arctic

bucket = Arctic(
    "s3://s3.eu-west-2.amazonaws.com:manstocks?region=eu-west-2&access=AKIAVHAD6ZB4RYHDPBWA&secret=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"
)
lib = bucket["etl_demo"]
raw_symbols = [symbol for symbol in lib.list_symbols() if "_TRANSFORMED" not in symbol]
transformed_symbols = [df_name for df_name in lib.list_symbols() if "_TRANSFORMED" in df_name]
transformed_dfs_dict = dict(zip(raw_symbols, [lib.read(df_name).data for df_name in transformed_symbols]))

FEATURES = ["Volume", "VWAP"]

### Streamlit UI ###

### SECTION 0 ###

st.title("Man ETL Dashboard")
st.write(
    """
Welcome! This dashboard allows you to customise the visualisation of features, both raw and derived,
from stock pricing data across a range of symbols.
"""
)

state_names = ["chosen_symbols", "chosen_feature", "chosen_norm", "chosen_single", "chosen_info"]
default_values = [raw_symbols[:3], FEATURES[0], True, raw_symbols[0], FEATURES[0]]

for i in range(len(state_names)):
    if state_names[i] not in st.session_state:
        st.session_state[state_names[i]] = default_values[i]

chosen_symbols = st.session_state[state_names[0]]  # Error if all options deselected!
chosen_feature = st.session_state[state_names[1]]
chosen_norm = st.session_state[state_names[2]]
chosen_single = st.session_state[state_names[3]]
chosen_info = st.session_state[state_names[4]]

## INFO ##

st.radio("Show feature information for:", key=state_names[4], options=FEATURES, horizontal=True)
if chosen_info == "Volume":
    st.write("Volume is a raw feature pulled directly from Yahoo Finance.")
elif chosen_info == "VWAP":
    st.write(
        """VWAP is the volume-weighted average price of stocks on a given day.
             It takes the typical price (i.e. the mean of the high, low, and close prices) at different intervals
             and normalises over the cumulative values for that day"""
    )
else:
    st.write("No info to display")

## SECTION 1 ##

st.header("Compare features across stocks")
col1a, col1b = st.columns(2)
with col1a:
    st.selectbox("Choose feature:", key=state_names[1], options=FEATURES)
with col1b:
    st.radio("Normalise?", key=state_names[2], options=[True, False])
st.multiselect("Chosen symbols", key=state_names[0], options=raw_symbols)

st.subheader(f"{'Normalised' if chosen_norm else ''} {chosen_feature} for {' '.join(str(x) for x in chosen_symbols)}")

fig = make_subplots(rows=1, cols=1)
for symbol in chosen_symbols:
    x_data = transformed_dfs_dict[symbol].index
    y_data = transformed_dfs_dict[symbol][chosen_feature]

    if chosen_norm == True:
        normalised_y_data = (y_data - y_data.min()) / (y_data.max() - y_data.min())
        fig.add_scatter(x=x_data, y=normalised_y_data, name=symbol, mode="lines")
    elif chosen_norm == False:
        fig.add_scatter(x=x_data, y=y_data, name=symbol, mode="lines")

fig.update_layout(xaxis_title="Date", yaxis_title=f"{'Normalised' if chosen_norm else ''} {chosen_feature}")

tab1, tab2 = st.tabs(["Default theme", "Plotly theme"])
with tab1:
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)
with tab2:
    st.plotly_chart(fig, theme=None, use_container_width=True)

# st.line_chart(transformed_dfs_dict[st.session_state['chosen_symbols'][0]][st.session_state['chosen_feature']])

## SECTION 2 ##

st.header(f"Overview of a single stock - {chosen_single}")
st.radio("Choose a symbol:", key="chosen_single", options=raw_symbols, horizontal=True)

fig = make_subplots(rows=1, cols=2, subplot_titles=FEATURES)
for i in range(len(FEATURES)):
    fig.add_trace(
        go.Scatter(
            x=transformed_dfs_dict[chosen_single].index,
            y=transformed_dfs_dict[chosen_single][FEATURES[i]],
            name=FEATURES[i],
        ),
        row=1,
        col=i + 1,
    )

fig.update_layout(height=400, width=1200, yaxis_title=f"{'Normalised' if chosen_norm else ''} {chosen_feature}")


tab1, tab2 = st.tabs(["Default theme", "Plotly theme"])
with tab1:
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)
with tab2:
    st.plotly_chart(fig, theme=None, use_container_width=True)
