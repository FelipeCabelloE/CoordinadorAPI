import duckdb
import pandas as pd
import streamlit as st


# Load the DuckDB file
def load_data(file_path):
    con = duckdb.connect(file_path)
    query = "SELECT fecha_hora, cmg_clp_kwh_, barra_info FROM api_data;"
    df = con.execute(query).fetch_df()
    con.close()
    return df


# File path to your DuckDB file (update with actual path)
duckdb_file_path = "./data.duckdb"

# Load the data
data = load_data(duckdb_file_path)

# Ensure correct data types
data["fecha_hora"] = pd.to_datetime(data["fecha_hora"])
data["barra_info"] = data["barra_info"].astype("category")

# Sidebar for filtering barra_info
barra_options = data["barra_info"].unique()
selected_barra = st.sidebar.selectbox("Select Barra Info", options=barra_options)

# Filter data based on selected "barra_info"
filtered_data = data[data["barra_info"] == selected_barra]


# Resample data to hourly granularity
numeric_columns = ["cmg_clp_kwh_"]
filtered_data = (
    filtered_data.set_index("fecha_hora")[numeric_columns]
    .resample("h")
    .mean()
    .reset_index()
)

# Main dashboard
st.title("CMG CLP/kWh Time Series Dashboard")

st.write(f"Displaying data for: **{selected_barra}**")

# Plot the time series
st.line_chart(filtered_data.set_index("fecha_hora")["cmg_clp_kwh_"])
