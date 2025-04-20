import pandas as pd
import numpy as np
from airflow.providers.postgres.hooks.postgres import PostgresHook

def update_new_variables3():
    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    query = """ SELECT * FROM table_weather"""
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        query = """ SELECT * from table_weather"""
        weather_df_copy = pd.read_sql_query(query, conn)
        weather_df_copy["datetime"] = pd.to_datetime(weather_df_copy["datetime"])
        weather_df_copy["sunset"] = pd.to_datetime(weather_df_copy["sunset"], errors='coerce')
        weather_df_copy["sunrise"] = pd.to_datetime(weather_df_copy["sunrise"], errors='coerce')

        def get_hour_period(hour):
            if 6 <= hour < 12:
                    return "Morning"
            elif 12 <= hour < 18:
                    return "Afternoon"
            elif 18 <= hour < 24:
                    return "Evening"
            else:
                    return "Night"
            
        weather_df_copy["hour_period"] = weather_df_copy["datetime"].dt.hour.apply(get_hour_period)
        weather_df_copy["precipitation_situation"] = (weather_df_copy["precipitation"] > 0).astype(int)
        weather_df_copy["max_min_temp_diff"] = weather_df_copy["max_temperature"] - weather_df_copy["min_temperature"]
        weather_df_copy["temp_apparent_diff"] = weather_df_copy["temperature"] - weather_df_copy["apparent_temperature"]
        weather_df_copy["daily_avg_temp"] = (weather_df_copy["max_temperature"] + weather_df_copy["min_temperature"] + weather_df_copy["temperature"]) /3
        weather_df_copy["daylight_duration"] = (weather_df_copy["sunset"] - weather_df_copy["sunrise"]).dt.total_seconds() / 3600
      
        
        weather_df_copy["temperature_rolling_mean_3"] = weather_df_copy["temperature"].rolling(window=3).mean()
        weather_df_copy["temperature_pct_change"] = weather_df_copy["temperature"].pct_change()
        weather_df_copy["temperature_lag_1"] = weather_df_copy["temperature"].shift(1)
        weather_df_copy["humidity_rolling_mean_3"] = weather_df_copy["relative_humidity"].rolling(window=3).mean()
        weather_df_copy["humidity_pct_change"] = weather_df_copy["relative_humidity"].pct_change()
        weather_df_copy["humidity_lag_1"] = weather_df_copy["relative_humidity"].shift(1)
        weather_df_copy["apparent_rolling_mean_3"] = weather_df_copy["apparent_temperature"].rolling(window=3).mean()
        weather_df_copy["apparent_lag_1"] = weather_df_copy["apparent_temperature"].shift(1)
        weather_df_copy["precipitation_lag_1"] = weather_df_copy["precipitation"].shift(1)
        weather_df_copy["pressure_rolling_mean_3"] = weather_df_copy["pressure"].rolling(window=3).mean()
        weather_df_copy["pressure_pct_change"] = weather_df_copy["pressure"].pct_change()
        weather_df_copy["pressure_lag_1"] = weather_df_copy["pressure"].shift(1)
        weather_df_copy["wind_speed_rolling_mean_3"] = weather_df_copy["wind_speed"].rolling(window=3).mean()
        weather_df_copy["wind_speed_lag_1"] = weather_df_copy["wind_speed"].shift(1)
        weather_df_copy["wind_direction_sin"] = np.sin(np.deg2rad(weather_df_copy["wind_direction"]))
        weather_df_copy["wind_direction_cos"] = np.cos(np.deg2rad(weather_df_copy["wind_direction"]))
        weather_description = {
                803: "broken clouds",  #parçalı bulutlu
                51: "light drizzle",   #hafif çiseleyen yağmur
                701: "mist",           #sis
                741: "mist",
                300: "light drizzle",  #hafif çiseleyen yağmur
                802: "scattered clouds",  #dağınık bulutlar
                800: "clear sky",         #açık hava
                801: "few clouds",        #birkaç bulut
                520: "light intensity shower rain",    #hafif şiddetli sağanak yağış
                500: "light rain",        #hafif yağmur
                501: "moderate rain",     #orta şiddette yağmur
                0: "fair weather",   #açık hava
                3: "lowering",   #kapalı
                2: "broken clouds",   #parçalı bulutlu
                521: "shower rain",   #sağanak yağış
                1: "clear sky",       #açık hava
                53: "medium drizzle",  #orta derecede çiseleyen yağmur
                55: "heavy drizzle",   #şiddetli çiseleyen yağmur
                61: "light rain",      #hafif yağmur
                63: "medium rain",     #orta yağmur
                65: "heavy rain",      #şiddetli yağmur
                73: "light snowfall",  #hafif kar yağışı
                804: "overcast clouds",  #bulutlu
        }
        weather_df_copy["weather_description"] = weather_df_copy["weather_code"].map(weather_description)

        for index, row in weather_df_copy.iterrows():
            print("ilk for döngüsüne girildi")
            #print(f"veritabanında güncellenecek değer: {row['mean_humidity_temp']}")  # Bu değerin doğru olup olmadığını kontrol et
            cursor.execute("""
                UPDATE table_weather
                SET weather_description = %s,
                    hour_period = %s,
                    precipitation_situation = %s,
                    max_min_temp_diff = %s,
                    temp_apparent_diff = %s,
                    daily_avg_temp = %s,
                    daylight_duration = %s,
                    temperature_rolling_mean_3 = %s,
                    temperature_pct_change = %s,
                    temperature_lag_1 = %s,
                    humidity_rolling_mean_3 = %s,
                    humidity_pct_change = %s,
                    humidity_lag_1 = %s,
                    apparent_rolling_mean_3 = %s,
                    apparent_lag_1 = %s,
                    precipitation_lag_1 = %s,
                    pressure_rolling_mean_3 = %s,
                    pressure_pct_change = %s,
                    pressure_lag_1 = %s,
                    wind_speed_rolling_mean_3 = %s,
                    wind_speed_lag_1 = %s,
                    wind_direction_sin = %s,
                    wind_direction_cos = %s
                WHERE id = %s AND datetime = %s;
            """, (
                row["weather_description"],
                row['hour_period'],
                row['precipitation_situation'],
                row['max_min_temp_diff'],
                row['temp_apparent_diff'],
                row['daily_avg_temp'],
                row['daylight_duration'],
                row['temperature_rolling_mean_3'],
                row['temperature_pct_change'],
                row['temperature_lag_1'],
                row['humidity_rolling_mean_3'],
                row['humidity_pct_change'],
                row['humidity_lag_1'],
                row['apparent_rolling_mean_3'],
                row['apparent_lag_1'],
                row['precipitation_lag_1'],
                row['pressure_rolling_mean_3'],
                row['pressure_pct_change'],
                row['pressure_lag_1'],
                row['wind_speed_rolling_mean_3'],
                row['wind_speed_lag_1'],
                row["wind_direction_sin"],
                row["wind_direction_cos"],
                row['id'],
                row['datetime']
            ))     
        conn.commit()
        print("Veri ekleme işlemi başarılı bir şekilde gerçekleşti")
    except Exception as e:
        print(f"Güncelleme sırasında hata oluştu: {e}")
    
    cursor.close()
    conn.close()

def update_new_variables():
    hook = PostgresHook(postgres_conn_id='postgres_localhost')

    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        query = """ SELECT * FROM table_weather WHERE is_processed = FALSE """
        weather_df_copy = pd.read_sql_query(query, conn)
        weather_df_copy["datetime"] = pd.to_datetime(weather_df_copy["datetime"])
        weather_df_copy["sunset"] = pd.to_datetime(weather_df_copy["sunset"], errors='coerce')
        weather_df_copy["sunrise"] = pd.to_datetime(weather_df_copy["sunrise"], errors='coerce')

        def get_hour_period(hour):
            if 6 <= hour < 12:
                    return "Morning"
            elif 12 <= hour < 18:
                    return "Afternoon"
            elif 18 <= hour < 24:
                    return "Evening"
            else:
                    return "Night"
            
        weather_df_copy["hour_period"] = weather_df_copy["datetime"].dt.hour.apply(get_hour_period)
        weather_df_copy["precipitation_situation"] = (weather_df_copy["precipitation"] > 0).astype(int)
        weather_df_copy["max_min_temp_diff"] = weather_df_copy["max_temperature"] - weather_df_copy["min_temperature"]
        weather_df_copy["temp_apparent_diff"] = weather_df_copy["temperature"] - weather_df_copy["apparent_temperature"]
        weather_df_copy["daily_avg_temp"] = (weather_df_copy["max_temperature"] + weather_df_copy["min_temperature"] + weather_df_copy["temperature"]) /3
        weather_df_copy["daylight_duration"] = (weather_df_copy["sunset"] - weather_df_copy["sunrise"]).dt.total_seconds() / 3600
       
        
        weather_df_copy["temperature_rolling_mean_3"] = weather_df_copy["temperature"].rolling(window=3).mean()
        weather_df_copy["temperature_pct_change"] = weather_df_copy["temperature"].pct_change()
        weather_df_copy["temperature_lag_1"] = weather_df_copy["temperature"].shift(1)
        weather_df_copy["humidity_rolling_mean_3"] = weather_df_copy["relative_humidity"].rolling(window=3).mean()
        weather_df_copy["humidity_pct_change"] = weather_df_copy["relative_humidity"].pct_change()
        weather_df_copy["humidity_lag_1"] = weather_df_copy["relative_humidity"].shift(1)
        weather_df_copy["apparent_rolling_mean_3"] = weather_df_copy["apparent_temperature"].rolling(window=3).mean()
        weather_df_copy["apparent_lag_1"] = weather_df_copy["apparent_temperature"].shift(1)
        weather_df_copy["precipitation_lag_1"] = weather_df_copy["precipitation"].shift(1)
        weather_df_copy["pressure_rolling_mean_3"] = weather_df_copy["pressure"].rolling(window=3).mean()
        weather_df_copy["pressure_pct_change"] = weather_df_copy["pressure"].pct_change()
        weather_df_copy["pressure_lag_1"] = weather_df_copy["pressure"].shift(1)
        weather_df_copy["wind_speed_rolling_mean_3"] = weather_df_copy["wind_speed"].rolling(window=3).mean()
        weather_df_copy["wind_speed_lag_1"] = weather_df_copy["wind_speed"].shift(1)
        weather_df_copy["wind_direction_sin"] = np.sin(np.deg2rad(weather_df_copy["wind_direction"]))
        weather_df_copy["wind_direction_cos"] = np.cos(np.deg2rad(weather_df_copy["wind_direction"]))
        weather_description = {
                803: "broken clouds",  #parçalı bulutlu
                51: "light drizzle",   #hafif çiseleyen yağmur
                701: "mist",           #sis
                741: "mist",
                300: "light drizzle",  #hafif çiseleyen yağmur
                802: "scattered clouds",  #dağınık bulutlar
                800: "clear sky",         #açık hava
                801: "few clouds",        #birkaç bulut
                520: "light intensity shower rain",    #hafif şiddetli sağanak yağış
                500: "light rain",        #hafif yağmur
                501: "moderate rain",     #orta şiddette yağmur
                0: "fair weather",   #açık hava
                3: "lowering",   #kapalı
                2: "broken clouds",   #parçalı bulutlu
                521: "shower rain",   #sağanak yağış
                1: "clear sky",       #açık hava
                53: "medium drizzle",  #orta derecede çiseleyen yağmur
                55: "heavy drizzle",   #şiddetli çiseleyen yağmur
                61: "light rain",      #hafif yağmur
                63: "medium rain",     #orta yağmur
                65: "heavy rain",      #şiddetli yağmur
                73: "light snowfall",  #hafif kar yağışı
                804: "overcast clouds",  #bulutlu
        }
        weather_df_copy["weather_description"] = weather_df_copy["weather_code"].map(weather_description)

        for index, row in weather_df_copy.iterrows():
            
            cursor.execute("""
                UPDATE table_weather
                SET weather_description = %s,
                    hour_period = %s,
                    precipitation_situation = %s,
                    max_min_temp_diff = %s,
                    temp_apparent_diff = %s,
                    daily_avg_temp = %s,
                    daylight_duration = %s,
                    temperature_rolling_mean_3 = %s,
                    temperature_pct_change = %s,
                    temperature_lag_1 = %s,
                    humidity_rolling_mean_3 = %s,
                    humidity_pct_change = %s,
                    humidity_lag_1 = %s,
                    apparent_rolling_mean_3 = %s,
                    apparent_lag_1 = %s,
                    precipitation_lag_1 = %s,
                    pressure_rolling_mean_3 = %s,
                    pressure_pct_change = %s,
                    pressure_lag_1 = %s,
                    wind_speed_rolling_mean_3 = %s,
                    wind_speed_lag_1 = %s,
                    wind_direction_sin = %s,
                    wind_direction_cos = %s,
                    is_processed = TRUE
                WHERE id = %s AND datetime = %s;
            """, (
                row["weather_description"],
                row['hour_period'],
                row['precipitation_situation'],
                row['max_min_temp_diff'],
                row['temp_apparent_diff'],
                row['daily_avg_temp'],
                row['daylight_duration'],
                row['temperature_rolling_mean_3'],
                row['temperature_pct_change'],
                row['temperature_lag_1'],
                row['humidity_rolling_mean_3'],
                row['humidity_pct_change'],
                row['humidity_lag_1'],
                row['apparent_rolling_mean_3'],
                row['apparent_lag_1'],
                row['precipitation_lag_1'],
                row['pressure_rolling_mean_3'],
                row['pressure_pct_change'],
                row['pressure_lag_1'],
                row['wind_speed_rolling_mean_3'],
                row['wind_speed_lag_1'],
                row["wind_direction_sin"],
                row["wind_direction_cos"],
                row['id'],
                row['datetime']
            ))     
        conn.commit()
        print("Veri ekleme işlemi başarılı bir şekilde gerçekleşti")
        conn.rollback()
    except Exception as e:
        print(f"Güncelleme sırasında hata oluştu: {e}")
    
    cursor.close()
    conn.close()

def update_new_variables4():
    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    query_all = """ SELECT * FROM table_weather ORDER BY datetime"""

    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        query_all = """ SELECT * FROM table_weather ORDER BY datetime"""
        full_data = pd.read_sql_query(query_all, conn)
        
        full_data["datetime"] = pd.to_datetime(full_data["datetime"])
        full_data["sunset"] = pd.to_datetime(full_data["sunset"], errors='coerce')
        full_data["sunrise"] = pd.to_datetime(full_data["sunrise"], errors='coerce')
        
        weather_description = {
            803: "broken clouds",  #parçalı bulutlu
            51: "light drizzle",   #hafif çiseleyen yağmur
            701: "mist",           #sis
            741: "mist",
            300: "light drizzle",  #hafif çiseleyen yağmur
            802: "scattered clouds",  #dağınık bulutlar
            800: "clear sky",         #açık hava
            801: "few clouds",        #birkaç bulut
            520: "light intensity shower rain",    #hafif şiddetli sağanak yağış
            500: "light rain",        #hafif yağmur
            501: "moderate rain",     #orta şiddette yağmur
            0: "fair weather",   #açık hava
            3: "lowering",   #kapalı
            2: "broken clouds",   #parçalı bulutlu
            521: "shower rain",   #sağanak yağış
            1: "clear sky",       #açık hava
            53: "medium drizzle",  #orta derecede çiseleyen yağmur
            55: "heavy drizzle",   #şiddetli çiseleyen yağmur
            61: "light rain",      #hafif yağmur
            63: "medium rain",     #orta yağmur
            65: "heavy rain",      #şiddetli yağmur
            73: "light snowfall",  #hafif kar yağışı
            804: "overcast clouds",  #bulutlu
        }
        full_data["weather_description"] = full_data["weather_code"].fillna(weather_description)

        def get_hour_period(hour):
            if 6 <= hour < 12:
                    return "Morning"
            elif 12 <= hour < 18:
                    return "Afternoon"
            elif 18 <= hour < 24:
                    return "Evening"
            else:
                    return "Night"
            
        full_data["hour_period"] = full_data["datetime"].dt.hour.apply(get_hour_period)
        full_data["precipitation_situation"] = full_data["precipitation_situation"].fillna((full_data["precipitation"] > 0).astype(int))

        full_data["max_min_temp_diff"] = full_data["max_min_temp_diff"].fillna(full_data["max_temperature"] - full_data["min_temperature"])

        full_data["temp_apparent_diff"] = full_data["temp_apparent_diff"].fillna(full_data["apparent_temperature"] - full_data["temperature"])

        full_data["daily_avg_temp"] = full_data["daily_avg_temp"].fillna((full_data["max_temperature"] + full_data["min_temperature"] + full_data["temperature"]) /3)

        full_data["daylight_duration"] = full_data["daylight_duration"].fillna((full_data["sunset"] - full_data["sunrise"]).dt.total_seconds() / 3600)

        full_data["temperature_rolling_mean_3"] = full_data["temperature_rolling_mean_3"].fillna(full_data["temperature"].rolling(window=3, min_periods=1).mean())
        full_data["temperature_pct_change"] = full_data["temperature_pct_change"].fillna(full_data["temperature"].pct_change())
        full_data["temperature_lag_1"] = full_data["temperature_lag_1"].fillna(full_data["temperature"].shift(1))

        full_data["humidity_rolling_mean_3"] = full_data["humidity_rolling_mean_3"].fillna(full_data["relative_humidity"].rolling(window=3, min_periods=1).mean())
        full_data["humidity_pct_change"] = full_data["humidity_pct_change"].fillna(full_data["relative_humidity"].pct_change())
        full_data["humidity_lag_1"] = full_data["humidity_lag_1"].fillna(full_data["relative_humidity"].shift(1))

        full_data["apparent_rolling_mean_3"] = full_data["apparent_rolling_mean_3"].fillna(full_data["apparent_temperature"].rolling(window=3, min_periods=1).mean())
        full_data["apparent_lag_1"] = full_data["apparent_lag_1"].fillna(full_data["apparent_temperature"].shift(1))

        full_data["precipitation_lag_1"] = full_data["precipitation_lag_1"].fillna(full_data["precipitation"].shift(1))
        full_data["pressure_rolling_mean_3"] = full_data["pressure_rolling_mean_3"].fillna(full_data["pressure"].rolling(window=3, min_periods=1).mean())
        full_data["pressure_pct_change"] = full_data["pressure_pct_change"].fillna(full_data["pressure"].pct_change())
        full_data["pressure_lag_1"] = full_data["pressure_lag_1"].fillna(full_data["pressure"].shift(1))

        full_data["wind_speed_rolling_mean_3"] = full_data["wind_speed_rolling_mean_3"].fillna(full_data["wind_speed"].rolling(window=3, min_periods=1).mean())
        full_data["wind_speed_lag_1"] = full_data["wind_speed_lag_1"].fillna(full_data["wind_speed"].shift(1))

        full_data["wind_direction_sin"] = full_data["wind_direction_sin"].fillna(full_data["wind_direction_sin"].mean())
        full_data["wind_direction_cos"] = full_data["wind_direction_cos"].fillna(full_data["wind_direction_cos"].mean())

        latest_row = full_data.sort_values(by="datetime", ascending=False).iloc[0]
        latest_row = latest_row.apply(lambda x: int(x) if isinstance(x, np.int64) else x)

        cursor.execute("""
                UPDATE table_weather
                SET weather_description = %s,
                    hour_period = %s,
                    precipitation_situation = %s,
                    max_min_temp_diff = %s,
                    temp_apparent_diff = %s,
                    daily_avg_temp = %s,
                    daylight_duration = %s,
                    temperature_rolling_mean_3 = %s,
                    temperature_pct_change = %s,
                    temperature_lag_1 = %s,
                    humidity_rolling_mean_3 = %s,
                    humidity_pct_change = %s,
                    humidity_lag_1 = %s,
                    apparent_rolling_mean_3 = %s,
                    apparent_lag_1 = %s,
                    precipitation_lag_1 = %s,
                    pressure_rolling_mean_3 = %s,
                    pressure_pct_change = %s,
                    pressure_lag_1 = %s,
                    wind_speed_rolling_mean_3 = %s,
                    wind_speed_lag_1 = %s,
                    wind_direction_sin = %s,
                    wind_direction_cos = %s
                WHERE id = %s;
            """, (
                latest_row["weather_description"],
                latest_row['hour_period'],
                latest_row['precipitation_situation'],
                latest_row['max_min_temp_diff'],
                latest_row['temp_apparent_diff'],
                latest_row['daily_avg_temp'],
                latest_row['daylight_duration'],
                latest_row['temperature_rolling_mean_3'],
                latest_row['temperature_pct_change'],
                latest_row['temperature_lag_1'],
                latest_row['humidity_rolling_mean_3'],
                latest_row['humidity_pct_change'],
                latest_row['humidity_lag_1'],
                latest_row['apparent_rolling_mean_3'],
                latest_row['apparent_lag_1'],
                latest_row['precipitation_lag_1'],
                latest_row['pressure_rolling_mean_3'],
                latest_row['pressure_pct_change'],
                latest_row['pressure_lag_1'],
                latest_row['wind_speed_rolling_mean_3'],
                latest_row['wind_speed_lag_1'],
                latest_row["wind_direction_sin"],
                latest_row["wind_direction_cos"],
                latest_row['id']
            ))

        conn.commit()
        print("NULL değerler başarıyla dolduruldu")

    except Exception as e:
        print(f"Hata oluştu: {e}")
        conn.rollback()

    cursor.close()
    conn.close()
