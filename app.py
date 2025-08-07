import streamlit as st
import pandas as pd
import mysql.connector
import os

# Page configuration
st.set_page_config(page_title="Credit Card Fraud Detection", layout="wide")

st.title("💳 Credit Card Fraud Detection Dashboard")

# MySQL connection
def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="frauddb"
    )

# Load data from MySQL
@st.cache_data(show_spinner=True)
def load_data():
    conn = get_connection()
    query = "SELECT * FROM fraud_data LIMIT 100000"  # Limit for performance
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Load the data
with st.spinner("Loading data from MySQL..."):
    df = load_data()
    st.success("Data loaded successfully!")

# Display filters
st.sidebar.header("🔍 Filter Options")
fraud_filter = st.sidebar.selectbox("Show", ["All", "Fraud", "Non-Fraud"])

if fraud_filter == "Fraud":
    df = df[df["is_fraud"] == 1]
elif fraud_filter == "Non-Fraud":
    df = df[df["is_fraud"] == 0]

# Show data stats
st.subheader("📊 Data Summary")
st.write(f"Total transactions displayed: `{len(df)}`")

col1, col2 = st.columns(2)
col1.metric("Fraud Count", int(df["is_fraud"].sum()))
col2.metric("Non-Fraud Count", int((df["is_fraud"] == 0).sum()))

# Display data
st.subheader("🧾 Data Preview")
st.dataframe(df.head(100))

# Optional: Show raw data toggle
if st.checkbox("Show full dataset"):
    st.dataframe(df)
