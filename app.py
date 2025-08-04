import streamlit as st
import pandas as pd
import joblib

# Page Configuration
st.set_page_config(page_title="Credit Card Fraud Detection", layout="wide")

# ---------- STYLING ----------
st.markdown("""
    <style>
    body {
        background-color: #E0F7FA; /* faint ocean blue */
    }
    * {
        font-family: Verdana;
    }
    h1, h2 {
        font-size: 30px !important;
        font-weight: bold;
    }
    .stTextInput > div > input, .stNumberInput input, .stSelectbox div, .stSlider div {
        border: 2px solid #FFDAB9 !important; /* papaya orange */
        background-color: white;
    }
    .stRadio label {
        font-size: 22px !important;
        font-weight: bold;
    }
    .stSidebar {
        background-color: #003366;
    }
    .stSidebar > div {
        color: white;
        font-size: 22px !important;
        font-weight: bold;
    }
    .stButton > button {
        font-size: 22px;
    }
    footer {visibility: hidden;}
    .footer {
        position: fixed;
        left: 0;
        bottom: 0;
        width: 100%;
        text-align: center;
        color: #888;
        font-size: 16px;
        padding: 10px;
    }
    </style>
""", unsafe_allow_html=True)

# ---------- Load model and features ----------
try:
    model = joblib.load("dashboards/streamlit_app/fraud_model.pkl")
    features = joblib.load("dashboards/streamlit_app/model_features.pkl")
except Exception as e:
    st.error(f"❌ Error loading model or features: {e}")
    st.stop()

# ---------- Load data function ----------
@st.cache_data
def load_data():
    try:
        df = pd.read_csv("dashboards/streamlit_app/processed_fraud_data_single.csv")
        return df
    except Exception as e:
        st.error(f"❌ Error loading data file: {e}")
        st.stop()

# Call load_data (optional, if you want to use the dataset somewhere)
data = load_data()

# ---------- Sidebar Menu ----------
menu = st.sidebar.radio("📋 Menu", ["Home", "About"])

# ---------- Home Section ----------
if menu == "Home":
    st.markdown("<h1>💳 Credit Card Fraud Detection</h1>", unsafe_allow_html=True)
    st.write("### Fill out transaction details:")

    with st.form("fraud_form"):
        col1, col2 = st.columns(2)

        with col1:
            amt = st.number_input("💰 Transaction Amount", min_value=0.0, format="%.2f")
            currency = st.selectbox("💱 Currency", ["USD", "INR", "EUR"])
            city_pop = st.slider("🏙️ City Population (scaled)", 0.0, 1.0, 0.1)
            trans_hour = st.slider("⏰ Transaction Hour (0-23)", 0, 23, 12)
            trans_dayofweek = st.slider("📆 Day of Week (1=Sun, 7=Sat)", 1, 7, 3)

        with col2:
            age = st.slider("👤 Age", 10, 100, 30)
            trans_month = st.slider("🗓️ Transaction Month (1-12)", 1, 12, 6)
            gender = st.radio("⚧️ Gender", ["Female", "Male"], horizontal=True)
            category_index = st.selectbox("🛍️ Category Index", [0.0, 1.0, 2.0, 3.0, 4.0])
            state_index = st.selectbox("📍 State Index", [float(i) for i in range(0, 50)])
            distance = st.slider("📏 Distance (miles)", 0.0, 1.0, 0.05)

        submit = st.form_submit_button("🚀 Predict Fraud")

    if submit:
        gender_index = 0.0 if gender == "Female" else 1.0
        input_data = pd.DataFrame([[
            amt, city_pop, age, trans_hour, trans_dayofweek, trans_month,
            gender_index, category_index, state_index, distance
        ]], columns=features)

        prediction = model.predict(input_data)[0]

        if prediction == 1:
            st.error("🚨 Fraud Detected!")
        else:
            st.success("✅ Transaction Looks Safe")

# ---------- About Section ----------
elif menu == "About":
    st.title("👩‍💻 About This App")
    st.markdown("""
    - **Credit Card Fraud Detection** using machine learning  
    - Built with `Streamlit`, `scikit-learn`, and `Pandas`  
    - UI designed for mobile and desktop with modern layout  
    - Predicts if a transaction is fraudulent based on input features  
    """)

# ---------- Footer ----------
st.markdown('<div class="footer">🛠️ This UI is developed by <b>MegharaniPol</b></div>', unsafe_allow_html=True)
