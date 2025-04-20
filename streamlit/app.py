# import streamlit as st
# import pandas as pd
# from datetime import datetime, timedelta
# import matplotlib.pyplot as plt
#from model import train_model, make_predictions, fetch_data_from_db
#import os
import streamlit as st
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import time
import os

st.markdown("""
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.3/css/all.min.css" rel="stylesheet">
    """, unsafe_allow_html=True)

icon_dict = {
    "broken clouds": '<i class="fas fa-cloud-sun"></i>',
    "light drizzle": '<i class="fas fa-cloud-rain"></i>',
    "mist": '<i class="fas fa-smog"></i>',
    "scattered clouds": '<i class="fas fa-cloud"></i>',
    "clear sky": '<i class="fas fa-sun"></i>',
    "few clouds": '<i class="fas fa-cloud-sun"></i>',                         
    "light intensity shower rain": '<i class="fas fa-cloud-showers-heavy"></i>',
    "light rain": '<i class="fas fa-cloud-rain"></i>',
    "moderate rain": '<i class="fas fa-cloud-showers-heavy"></i>',
    "lowering": '<i class="fas fa-cloud"></i>',
    "shower rain": '<i class="fas fa-cloud-showers-heavy"></i>',
    "medium drizzle": '<i class="fas fa-cloud-rain"></i>',
    "heavy drizzle": '<i class="fas fa-cloud-showers-heavy"></i>',
    "heavy rain": '<i class="fas fa-cloud-showers-heavy"></i>',
    "light snowfall": '<i class="fas fa-snowflake"></i>',
    "overcast clouds": '<i class="fas fa-cloud"></i>',
}

st.title("İstanbul Weather Prediction")
st.markdown("---")
col1 = st.columns([1])[0]
col2, col3 = st.columns([2, 6])

today = datetime.now().date()
dates = [today + timedelta(days=i + 1) for i in range(7)]

def get_file_timestamp(filepath):
    return os.path.getmtime(filepath)

@st.cache_data
def load_data(filepath):
    predictions_df = pd.read_csv(filepath)
    return predictions_df

filepath = "/opt/airflow/shared_data/predictions.csv"

last_modified_time = get_file_timestamp(filepath)

if 'last_modified_time' in st.session_state:
    if st.session_state.last_modified_time != last_modified_time:
        st.cache_data.clear()
        st.session_state.last_modified_time = last_modified_time
else:
    st.session_state.last_modified_time = last_modified_time

predictions_df = load_data(filepath)
predictions_df['date'] = pd.to_datetime(predictions_df['date'], errors='coerce')
print(predictions_df)

with col1:
    selected_date = st.date_input("Lütfen tarih seçiniz", today)
    selected_date = pd.to_datetime(selected_date, errors='coerce')
    selected_predictions = predictions_df[predictions_df["date"].dt.date == selected_date.date()]
    days_7_df = predictions_df.tail(7)
    days_7_df["date"] = pd.to_datetime(days_7_df["date"], errors="coerce")
    days_7_df["date"] = days_7_df["date"].dt.strftime('%d-%m-%Y')

    if st.button("Tahmini Göster"):
        icon_html = icon_dict[selected_predictions["desc"].values[0]]
        with col2:
            st.markdown(f"""
                    <div style="
                    background-color: #f0f0f0;
                    padding: 30px;
                    border-radius: 10px;
                    box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
                    text-align: left;
                    font-size: 18px;
                    width: 100%;  /* Kutu genişliği %100 */
                    max-width: 100%;  /* Maksimum genişlik */
                    ">
                    <div style="font-size: 24px; font-weight: bold; margin-bottom: 10px;">{selected_date.strftime('%d-%m-%Y')} için Tahmin Sonucu:</div>
                    <div style="font-size: 20px; margin-bottom: 5px;">Hava Durumu: {selected_predictions['desc'].iloc[0]} {icon_html}</div>
                    <div style="font-size: 20px; margin-bottom: 5px;">Sıcaklık: {selected_predictions['temp'].iloc[0]} °C</div>
                    <div style="font-size: 20px;">Nem: {selected_predictions['hum'].iloc[0]} %</div>
                    </div>
                """, unsafe_allow_html=True)

        with col3:
            days_7_df["date"] = pd.to_datetime(days_7_df["date"], errors="coerce")
            fig, ax = plt.subplots(1, 2, figsize=(12, 6), dpi=150)
            ax[0].plot(days_7_df["date"], days_7_df["temp"], marker="o", color="red", linestyle='-', alpha=0.8)
            ax[0].set_title("Temperature Forecast", fontsize=14)
            ax[0].set_ylabel("Temperature", fontsize=14)
            ax[0].tick_params(axis="x", labelsize=12, rotation=45)
            ax[0].tick_params(axis="y", labelsize=12)

            ax[1].plot(days_7_df["date"], days_7_df["hum"], marker="o", color="green", linestyle='-', alpha=0.8)
            ax[1].set_title("Humidity Forecast", fontsize=14)
            ax[1].set_ylabel("Humidity", fontsize=14)
            ax[1].tick_params(axis="x", labelsize=12, rotation=45)
            ax[1].tick_params(axis="y", labelsize=12)
                        
            fig.tight_layout(pad=3.0)
            plt.close(fig)
            st.pyplot(fig)

