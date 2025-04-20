def fetch_data_from_db():
    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    conn = hook.get_conn()
    query = """ SELECT * FROM table_weather"""
    weather_df = pd.read_sql_query(query, conn)
    conn.close()
    return weather_df

def preporecess_weather_data(weather_df):
    import pandas as pd
    import numpy as np
    def get_hour_period(hour):
        if 6 <= hour < 12:
            return "Morning"
        elif 12 <= hour < 18:
            return "Afternoon"
        elif 18 <= hour < 24:
            return "Evening"
        else:
            return "Night"
        
    weather_df['datetime'] = pd.to_datetime(weather_df['datetime'])
  
    weather_df["hour_period"] = weather_df["datetime"].dt.hour.apply(get_hour_period)
    weather_df["precipitation_situation"] = (weather_df["precipitation"] > 0).astype(int)
    weather_df["max_min_temp_diff"] = weather_df["max_temperature"] - weather_df["min_temperature"]
    weather_df["temp_apparent_diff"] = weather_df["temperature"] - weather_df["apparent_temperature"]
    weather_df["daily_avg_temp"] = (weather_df["max_temperature"] + weather_df["min_temperature"] + weather_df["temperature"]) /3
    weather_df["daylight_duration"] = (weather_df["sunset"] - weather_df["sunrise"]).dt.total_seconds() / 3600
    grouped = weather_df.groupby("hour_period").agg(
            max_humidity=("relative_humidity", "max"),
            min_temperature=("temperature", "min")
    ).reset_index()
            
    grouped["mean_humidity_temp"] = (grouped["max_humidity"] + grouped["min_temperature"]) / 2

    weather_df = weather_df.merge(
            grouped[["hour_period", "mean_humidity_temp"]],
            on="hour_period",
            how="left"
    )
            
    weather_df["temperature_rolling_mean_3"] = weather_df["temperature"].rolling(window=3).mean()
    weather_df["temperature_pct_change"] = weather_df["temperature"].pct_change()
    weather_df["temperature_lag_1"] = weather_df["temperature"].shift(1)
    weather_df["humidity_rolling_mean_3"] = weather_df["relative_humidity"].rolling(window=3).mean()
    weather_df["humidity_pct_change"] = weather_df["relative_humidity"].pct_change()
    weather_df["humidity_lag_1"] = weather_df["relative_humidity"].shift(1)
    weather_df["apparent_rolling_mean_3"] = weather_df["apparent_temperature"].rolling(window=3).mean()
    weather_df["apparent_lag_1"] = weather_df["apparent_temperature"].shift(1)
    weather_df["precipitation_lag_1"] = weather_df["precipitation"].shift(1)
    weather_df["pressure_rolling_mean_3"] = weather_df["pressure"].rolling(window=3).mean()
    weather_df["pressure_pct_change"] = weather_df["pressure"].pct_change()
    weather_df["pressure_lag_1"] = weather_df["pressure"].shift(1)
    weather_df["wind_speed_rolling_mean_3"] = weather_df["wind_speed"].rolling(window=3).mean()
    weather_df["wind_speed_lag_1"] = weather_df["wind_speed"].shift(1)
    weather_df["wind_direction_sin"] = np.sin(np.deg2rad(weather_df["wind_direction"]))
    weather_df["wind_direction_cos"] = np.cos(np.deg2rad(weather_df["wind_direction"]))
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
        0: "clear sky",   #açık hava
        3: "lowering",   #kapalı
        2: "broken clouds",   #parçalı bulutlu
        521: "shower rain",   #sağanak yağış
        1: "clear sky",       #açık hava
        53: "medium drizzle",  #orta derecede çiseleyen yağmur
        55: "heavy drizzle",   #şiddetli çiseleyen yağmur
        61: "light rain",      #hafif yağmur
        63: "moderate rain",     #orta yağmur
        65: "heavy rain",      #şiddetli yağmur
        73: "light snowfall",  #hafif kar yağışı
        804: "overcast clouds",  #bulutlu
    }
    weather_df["weather_description"] = weather_df["weather_code"].map(weather_description)

    weather_df.fillna(method='bfill', inplace=True)

    selected_columns = ["id", "data_type", "weather_code", "max_temperature", "min_temperature", 
                    "mean_apparent_temp", "precipitation_sum", "snowfall_sum", "sunrise", "sunset",
                    "max_wind_speed", "dominant_wind_dir", "description"]
    weather_df = weather_df.drop(selected_columns, axis=1)

    from sklearn.preprocessing import OneHotEncoder

    ohe = OneHotEncoder(sparse=False, drop='first')
    encoded_columns = ohe.fit_transform(weather_df[["hour_period"]])
    encoded_columns_names = ohe.get_feature_names_out(["hour_period"])
    encoded_df = pd.DataFrame(encoded_columns, columns=encoded_columns_names)
    encoded_df.index = weather_df.index
    weather_df = pd.concat([weather_df, encoded_df], axis=1).drop(columns=["hour_period"])

    weather_df['year'] = weather_df['datetime'].dt.year
    weather_df['month'] = weather_df['datetime'].dt.month
    weather_df['day'] = weather_df['datetime'].dt.day
    weather_df['weekday'] = weather_df['datetime'].dt.weekday
    weather_df['hour'] = weather_df['datetime'].dt.hour

    weather_df['sin_hour'] = np.sin(2 * np.pi * weather_df['hour'] / 24)
    weather_df['cos_hour'] = np.cos(2 * np.pi * weather_df['hour'] / 24)

    weather_df['sin_month'] = np.sin(2 * np.pi * weather_df['month'] / 12)
    weather_df['cos_month'] = np.cos(2 * np.pi * weather_df['month'] / 12)

    weather_df = weather_df.drop("datetime", axis=1)
    return weather_df

def create_dataset(X, y, window_size=10):
    import numpy as np
    import pandas as pd
    if len(X) != len(y):
        raise ValueError(f"X ve y dizilerinin uzunlukları eşit olmalıdır. X length: {len(X)}, y length: {len(y)}")
    else:
        print(f"X ve y dizilerinin boyları eşit: {len(X)} = {len(y)}")
    
    if isinstance(y, pd.Series):
        y = y.reset_index(drop=True)

    X_data, y_data = [], []
    for i in range(len(X) - window_size):
        X_data.append(X[i:i + window_size])
        if i + window_size < len(y):
           y_data.append(y[i + window_size])
        else:
            break
    print(f"create_dataset fonksiyonunda X shape: {X.shape}, y shape: {y.shape}")
    return np.array(X_data), np.array(y_data)

def fit_lstm_model(X_train, y_train, vocab_size, window_size):
    from tensorflow.keras.models import Sequential
    from tensorflow.keras.layers import Dense, Dropout,GRU
    from tensorflow.keras.callbacks import EarlyStopping
    from tensorflow.keras.layers import BatchNormalization
    from tensorflow.keras.optimizers import Adam

    model = Sequential()
    model.add(GRU(64, input_shape = (window_size, X_train.shape[2]), return_sequences=True))
    model.add(BatchNormalization())
    model.add(Dropout(0.4))

    model.add(GRU(32, activation="relu", return_sequences=True))
    model.add(BatchNormalization())
    model.add(Dropout(0.3))
    model.add(GRU(16, activation="relu"))
    model.add(BatchNormalization())
    model.add(Dropout(0.3))
    
    model.add(Dense(16))
    model.add(Dropout(0.2))

    model.add(Dense(vocab_size, activation="softmax" if vocab_size > 1 else "linear"))

    optimizer = Adam(learning_rate=0.0003, clipnorm=1.0)
    model.compile(loss= "sparse_categorical_crossentropy" if vocab_size > 1 else "mse", optimizer=optimizer, 
                  metrics=["accuracy"] if vocab_size > 1 else ["mse"])
    early_stopping = EarlyStopping(monitor="val_loss", patience=15, restore_best_weights=True)
    model.fit(X_train, y_train, epochs = 120, batch_size=64, verbose=1, callbacks=[early_stopping])
    return model

def train_model(target):
    
    from sklearn.preprocessing import StandardScaler, LabelEncoder, MinMaxScaler
    import numpy as np

    weather_df = fetch_data_from_db()
    weather_df = preporecess_weather_data(weather_df)

    if target != "weather_description":
        label_encoder = LabelEncoder()
        weather_df["weather_description"] = label_encoder.fit_transform(weather_df["weather_description"])

    X = weather_df.drop([target], axis=1)
    y = weather_df[target]
    feature_names = X.columns
    sc = MinMaxScaler()
    X_scaled = sc.fit_transform(X)
    
    if target == "weather_description":
        label_encoder = LabelEncoder()
        y_encoded = label_encoder.fit_transform(y)
        vocab_size = len(label_encoder.classes_)
    else:
        y_encoded = y
        label_encoder = None
        vocab_size = 1

    train_size = int(len(X_scaled) * 0.7)
    test_size = len(X_scaled) - train_size

    X_train, X_test = X_scaled[:train_size], X_scaled[train_size:]
    y_train, y_test = y_encoded[:train_size], y_encoded[train_size:]

    window_size = 10
    X_train_seq, y_train_seq = create_dataset(X_train, y_train, window_size)
    X_test_seq, y_test_seq = create_dataset(X_test, y_test, window_size)

    X_train_seq = np.reshape(X_train_seq, (X_train_seq.shape[0], X_train_seq.shape[1], X_train_seq.shape[2]))
    X_test_seq = np.reshape(X_test_seq, (X_test_seq.shape[0], X_test_seq.shape[1], X_test_seq.shape[2]))

    model = fit_lstm_model(X_train_seq, y_train_seq, vocab_size, window_size)

    return model, label_encoder, sc, weather_df, feature_names

def make_predictions(model, label_encoder, sc, recent_weather_data, feature_names, window_size):
    import numpy as np
    import pandas as pd
    from datetime import datetime, timedelta
    recent_weather_data = recent_weather_data[feature_names]
    scaled_data = sc.transform(recent_weather_data)
    X_recent_seq, _ = create_dataset(scaled_data, np.zeros(len(scaled_data)), window_size)
    X_recent_seq = np.reshape(X_recent_seq, (X_recent_seq.shape[0], X_recent_seq.shape[1], X_recent_seq.shape[2]))

    predictions = model.predict(X_recent_seq)
    predicted_class = np.argmax(predictions, axis=1)
    if label_encoder:
        predicted_labels = label_encoder.inverse_transform(predicted_class)
    else:
        predicted_labels = predicted_class

    print("Predicted labels:", predicted_labels)

    prediction_dates = [datetime.now() + timedelta(days=i +1) for i in range(7)]
    predictions_df = pd.DataFrame({
        'date': prediction_dates[:7],
        'predicted_label': predicted_labels[:7]
    })
    print(f"Predictions DataFrame türü: {type(predictions_df)}")
    print(f"Predictions DataFrame içeriği:\n{predictions_df.head()}")
    return predictions_df

def streamlit_app():
    import pandas as pd
    from datetime import datetime, timedelta
    import matplotlib.pyplot as plt

    #@st.cache_resource
    def load_model():
        model_desc, label_encoder_desc, scaler_desc, recent_weather_data_desc, feature_names_desc = train_model("weather_description")
        model_temp, label_encoder_temp, scaler_temp, recent_weather_data_temp, feature_names_temp = train_model("temperature")
        model_hum, label_encoder_hum, scaler_hum, recent_weather_data_hum, feature_names_hum = train_model("relative_humidity")
        return {
            "weather_description": (model_desc, label_encoder_desc, scaler_desc, recent_weather_data_desc, feature_names_desc),
            "temperature": (model_temp, label_encoder_temp, scaler_temp, recent_weather_data_temp, feature_names_temp),
            "relative_humidity": (model_hum, label_encoder_hum, scaler_hum, recent_weather_data_hum, feature_names_hum)
        }

    models = load_model()

    def get_predictions(model, label_encoder, scaler, recent_weather_data, feature_names, selected_date):
        window_size = 10
        predictions_df = make_predictions(model, label_encoder, scaler, recent_weather_data, feature_names, window_size)
        print("make_predictions çıktısı:", predictions_df)
        print(f"get_predictions - predictions_df türü: {type(predictions_df)}")

        if isinstance(predictions_df, pd.DataFrame):
            # 'date' sütununun doğru formatta olduğundan emin olalım
            if "date" in predictions_df.columns:
                predictions_df["date"] = pd.to_datetime(predictions_df["date"], errors="coerce")
                
                # 'date' sütunundaki hatalı değerleri kontrol et
                if predictions_df["date"].isnull().all():
                    return "Tüm 'date' değerleri hatalı veya dönüştürülemiyor!"
        else:
            return "'date' sütunu bulunamadı!"
        
        # selected_date = pd.to_datetime(selected_date)

        # print(f"selected_date tipi: {type(selected_date)}")

        # selected_predictions = predictions_df[predictions_df["date"].dt.date == selected_date.date()]

        # print(f"selected_predictions:\n{selected_predictions}")

        # if not selected_predictions.empty:
        #     predicted_label = selected_predictions.iloc[0]
        #     return predicted_label, predictions_df
        # else:
        #     return "Tahmin yapılabilir tarih aralığı dışında."
        return predictions_df

    model_desc, label_encoder_desc, scaler_desc, recent_weather_data_desc, feature_names_desc = models["weather_description"]
    model_temp, label_encoder_temp, scaler_temp, recent_weather_data_temp, feature_names_temp = models["temperature"]
    model_hum, label_encoder_hum, scaler_hum, recent_weather_data_hum, feature_names_hum = models["relative_humidity"]

    today = datetime.now().date()
    selected_date = [today + timedelta(days=i) for i in range(5)]

    predictions_df_desc_5 = get_predictions(model_desc, label_encoder_desc, scaler_desc, recent_weather_data_desc, feature_names_desc, selected_date)
    predictions_df_temp_5 = get_predictions(model_temp, label_encoder_temp, scaler_temp, recent_weather_data_temp, feature_names_temp, selected_date)
    predictions_df_hum_5 = get_predictions(model_hum, label_encoder_hum, scaler_hum, recent_weather_data_hum, feature_names_hum, selected_date)

    predictions_df_desc_5 = predictions_df_desc_5.rename(columns={'predicted_label': 'desc'})
    predictions_df_temp_5 = predictions_df_temp_5.rename(columns={'predicted_label': 'temp'})
    predictions_df_hum_5 = predictions_df_hum_5.rename(columns={'predicted_label': 'hum'})

    predictions_df_desc_5["date"] = predictions_df_desc_5["date"].dt.date
    predictions_df_temp_5["date"] = predictions_df_temp_5["date"].dt.date
    predictions_df_hum_5["date"] = predictions_df_hum_5["date"].dt.date

    merged_df = predictions_df_desc_5.merge(predictions_df_temp_5, on='date', how='left')
    merged_df = merged_df.merge(predictions_df_hum_5, on = 'date', how= 'left')
    print(merged_df)

    merged_df.to_csv("/opt/airflow/shared_data/predictions.csv", index=False)

    
