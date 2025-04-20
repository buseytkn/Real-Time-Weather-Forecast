import requests
import psycopg2
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook

conn = psycopg2.connect(
    dbname = "dbweather",
    user = "postgres",
    password = "password",
    host = "localhost",
    port = "5432"
)
cursor = conn.cursor()

#İstanbu koordinatları
enlem = 41.0082
boylam = 28.9784

#OPEN METEO APİ

end_date = datetime.strptime("2024-10-31", "%Y-%m-%d") 
start_date = datetime.strptime("2024-07-01", "%Y-%m-%d")

meteo_url = (
    f"https://archive-api.open-meteo.com/v1/archive?latitude={enlem}&longitude={boylam}"
    f"&start_date={start_date.strftime('%Y-%m-%d')}&end_date={end_date.strftime('%Y-%m-%d')}"
    "&hourly=temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,rain,snowfall,surface_pressure,"
    "windspeed_10m,winddirection_10m,weathercode"
    "&daily=weathercode,temperature_2m_max,temperature_2m_min,apparent_temperature_max,apparent_temperature_min,"
    "precipitation_sum,snowfall_sum,windspeed_10m_max,winddirection_10m_dominant,sunrise,sunset"
    "&timezone=Europe/Istanbul"
)

response = requests.get(meteo_url)
meteo_data = response.json()

# SAATLİK VERİLERİN ÇEKİLMESİ
hourly_data = meteo_data.get("hourly", {})
for i, datetime_str in enumerate(hourly_data["time"]):
    raw_time = datetime_str
    if len(raw_time) == 10: 
        raw_time += "T00:00"
    formatted_time = datetime.strptime(raw_time, "%Y-%m-%dT%H:%M").strftime("%Y-%m-%d %H:%M:%S")
    try:
        cursor.execute(
        """
        INSERT INTO table_weather ( "datetime", data_type, temperature, relative_humidity, apparent_temperature, precipitation, snowfall,
        pressure, wind_speed, wind_direction, weather_code
        ) VALUES (%s, 'hourly', %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            formatted_time,
            hourly_data["temperature_2m"][i],
            hourly_data["relative_humidity_2m"][i],
            hourly_data["apparent_temperature"][i],
            hourly_data["precipitation"][i],
            hourly_data["snowfall"][i],
            hourly_data["surface_pressure"][i],
            hourly_data["windspeed_10m"][i],
            hourly_data["winddirection_10m"][i],
            hourly_data["weathercode"][i],
        ),
      )
    except Exception as e:
        print(f'{formatted_time} için ekleme hatası {e}')
        conn.rollback()
    
    

    # GÜNLÜK VERİLERİN KAYDEDİLMESİ
    daily_data = meteo_data.get("daily", {})
    for i, datetime_str in enumerate(daily_data["time"]):
        raw_time = datetime_str
        if len(raw_time) == 10:  # "%Y-%m-%d" formatı
           raw_time += "T00:00"
        formatted_time = datetime.strptime(raw_time, "%Y-%m-%dT%H:%M").strftime("%Y-%m-%d %H:%M:%S")
        sunrise = daily_data["sunrise"][i].replace('T', ' ') + "+00:00"  # UTC'yi ekleyerek formatla
        sunset = daily_data["sunset"][i].replace('T', ' ') + "+00:00"
        try:
              cursor.execute(
            """
            INSERT INTO table_weather ( "datetime", data_type, weather_code, max_temperature, min_temperature, mean_apparent_temp,
            precipitation_sum, snowfall_sum, max_wind_speed, dominant_wind_dir, sunrise, sunset
            ) VALUES (%s, 'daily', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                formatted_time,
                daily_data["weathercode"][i],
                daily_data["temperature_2m_max"][i],
                daily_data["temperature_2m_min"][i],
                daily_data["apparent_temperature_max"][i],
                daily_data["precipitation_sum"][i],
                daily_data["snowfall_sum"][i],
                daily_data["windspeed_10m_max"][i],
                daily_data["winddirection_10m_dominant"][i],
                sunrise,
                sunset,
            ),
          )
        except Exception as e:
            print(f'{formatted_time} için ekleme hatası {e}')
            conn.rollback()
      

conn.commit()
cursor.close()
conn.close() 


conn = psycopg2.connect(
    dbname = "dbweather",
    user = "postgres",
    password = "125690by",
    host = "localhost",
    port = "5432"
    )

def fetch_hourly_weather_data():
    enlem = 41.0082
    boylam = 28.9784
    end_date = datetime.today()
    start_date = end_date
    postgres_hook = PostgresHook(postgres_conn_id='postgres_localhost')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
  
    meteo_url = (
    f"https://archive-api.open-meteo.com/v1/archive?latitude={enlem}&longitude={boylam}"
    f"&start_date={start_date.date()}&end_date={end_date.date()}"
    "&hourly=temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,rain,snowfall,surface_pressure,"
    "windspeed_10m,winddirection_10m,weathercode"
    "&daily=weathercode,temperature_2m_max,temperature_2m_min,apparent_temperature_max,apparent_temperature_min,"
    "precipitation_sum,snowfall_sum,windspeed_10m_max,winddirection_10m_dominant,sunrise,sunset"
    "&timezone=Europe/Istanbul"
    )
    
    response = requests.get(meteo_url)
    meteo_data = response.json()
    cursor.execute("SET search_path TO public;")

    # SAATLİK VERİLERİN ÇEKİLMESİ
    hourly_data = meteo_data.get("hourly", {})
    for i, datetime_str in enumerate(hourly_data["time"]):
       raw_time = datetime_str
       formatted_time = None  
       try:
          if len(raw_time) == 10: 
            raw_time += "T00:00"
          formatted_time = datetime.strptime(raw_time, "%Y-%m-%dT%H:%M").strftime("%Y-%m-%d %H:%M:%S")
        
          cursor.execute(
              """
              INSERT INTO "table_weather" ( "datetime", data_type, temperature, relative_humidity, apparent_temperature, precipitation, snowfall,
              pressure, wind_speed, wind_direction, weather_code
              ) VALUES (%s, 'hourly', %s, %s, %s, %s, %s, %s, %s, %s, %s)
              """,
              (
                formatted_time,
                hourly_data["temperature_2m"][i],
                hourly_data["relative_humidity_2m"][i],
                hourly_data["apparent_temperature"][i],
                hourly_data["precipitation"][i],
                hourly_data["snowfall"][i],
                hourly_data["surface_pressure"][i],
                hourly_data["windspeed_10m"][i],
                hourly_data["winddirection_10m"][i],
                hourly_data["weathercode"][i],
              ),
          )
          conn.commit()
          print("Saatlik veriler başarıyla kaydedildi")
       except Exception as e:
            # Hata mesajı
            if formatted_time is None:
               print(f'Zaman formatı hatalı, ekleme yapılamadı: {e}')
            else:
               print(f'{formatted_time} için ekleme hatası {e}')
            conn.rollback()

    cursor.close()
    conn.close()

def fetch_daily_weather_data():
    enlem = 41.0082
    boylam = 28.9784
    end_date = datetime.today()
    start_date = end_date
    postgres_hook = PostgresHook(postgres_conn_id='postgres_localhost2')
    conn = postgres_hook.get_conn()
    #conn.reset()
    cursor = conn.cursor()
    cursor.execute("SET search_path TO public;")
  
    meteo_url = (
    f"https://archive-api.open-meteo.com/v1/archive?latitude={enlem}&longitude={boylam}"
    f"&start_date={start_date.date()}&end_date={end_date.date()}"
    "&hourly=temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,rain,snowfall,surface_pressure,"
    "windspeed_10m,winddirection_10m,weathercode"
    "&daily=weathercode,temperature_2m_max,temperature_2m_min,apparent_temperature_max,apparent_temperature_min,"
    "precipitation_sum,snowfall_sum,windspeed_10m_max,winddirection_10m_dominant,sunrise,sunset"
    "&timezone=Europe/Istanbul"
    )
    
    response = requests.get(meteo_url)
    meteo_data = response.json()
    daily_data = meteo_data.get("daily", {})
    for i, datetime_str in enumerate(daily_data["time"]):
        raw_time = datetime_str
        if len(raw_time) == 10:  # "%Y-%m-%d" formatı
           raw_time += "T00:00"
        formatted_time = datetime.strptime(raw_time, "%Y-%m-%dT%H:%M").strftime("%Y-%m-%d %H:%M:%S")
        sunrise = daily_data["sunrise"][i].replace('T', ' ') + "+00:00"  # UTC'yi ekleyerek formatla
        sunset = daily_data["sunset"][i].replace('T', ' ') + "+00:00"
        try:
            cursor.execute(
            """
            INSERT INTO "table_weather" ( "datetime", data_type, weather_code, max_temperature, min_temperature, mean_apparent_temp,
            precipitation_sum, snowfall_sum, max_wind_speed, dominant_wind_dir, sunrise, sunset
            ) VALUES (%s, 'daily', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                formatted_time,
                daily_data["weathercode"][i],
                daily_data["temperature_2m_max"][i],
                daily_data["temperature_2m_min"][i],
                daily_data["apparent_temperature_max"][i],
                daily_data["precipitation_sum"][i],
                daily_data["snowfall_sum"][i],
                daily_data["windspeed_10m_max"][i],
                daily_data["winddirection_10m_dominant"][i],
                sunrise,
                sunset,
            ),
          )
            conn.commit()
            print("Günlük veriler başarıyla kaydedildi")
        except Exception as e:
            print(f'{formatted_time} için ekleme hatası {e}')
            conn.rollback()
        
    cursor.close()
    conn.close()

def fetch_weather_data():

    OPENWEATHERMAP_API_KEY = 'api_key'

    WEATHERAPI_KEY = 'api_key'

    enlem = 41.0082
    boylam = 28.9784
    postgres_hook = PostgresHook(postgres_conn_id='postgres_localhost')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    openweathermap_url = (
        f"http://api.openweathermap.org/data/2.5/weather?lat={enlem}&lon={boylam}&units=metric&appid={OPENWEATHERMAP_API_KEY}"
    )

    weatherapi_url = (
        f"http://api.weatherapi.com/v1/current.json?key={WEATHERAPI_KEY}&q={enlem},{boylam}"
    )

    openweathermap_response = requests.get(openweathermap_url)
    openweathermap_data = openweathermap_response.json()

    weatherapi_response = requests.get(weatherapi_url)
    weatherapi_data = weatherapi_response.json()

    cursor.execute("SET search_path TO public;")

    raw_time = datetime.utcfromtimestamp(openweathermap_data["dt"]) #.strftime("%Y-%m-%d %H:%M:%S")
    sunrise = datetime.utcfromtimestamp(openweathermap_data["sys"]["sunrise"]) #.strftime("%Y-%m-%d %H:%M:%S")
    sunset = datetime.utcfromtimestamp(openweathermap_data["sys"]["sunset"]) #.strftime("%Y-%m-%d %H:%M:%S")

    rain = weatherapi_data.get("current", {}).get("precip_mm", None)  # Yağış
    snow = weatherapi_data.get("current", {}).get("snow_mm", 0)  # Kar

    snow = snow if snow is not None else 0

    cursor.execute(
        """
        SELECT COUNT(*) FROM table_weather WHERE datetime = %s
        """, (raw_time,),
    )
    result = cursor.fetchone()

    if result[0] == 0:

        try:
            cursor.execute(
              """
              INSERT INTO "table_weather" ( "datetime", data_type, temperature, relative_humidity, apparent_temperature,
              precipitation, snowfall, weather_code, pressure, wind_speed, wind_direction, max_temperature, min_temperature, sunrise, sunset, description)
              VALUES (%s, 'current', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
              """,
              (
                raw_time,
                openweathermap_data["main"]["temp"],  # Sıcaklık
                openweathermap_data["main"]["humidity"],  # Nem
                openweathermap_data["main"]["feels_like"],  # Hissedilen sıcaklık
                rain,  # Yağış
                snow,  # Kar
                openweathermap_data["weather"][0]["id"],  # Hava durumu kodu
                openweathermap_data["main"]["pressure"],  # Basınç
                openweathermap_data["wind"]["speed"],  # Rüzgar hızı
                openweathermap_data["wind"]["deg"],  # Rüzgar yönü
                openweathermap_data["main"]["temp_max"],  # Maksimum sıcaklık
                openweathermap_data["main"]["temp_min"],  # Minimum sıcaklık
                sunrise,  # Gün doğumu
                sunset,  # Gün batımı
                openweathermap_data["weather"][0]["description"],
              ),
            )
            conn.commit()
            print(f"Güncel veri başarıyla kaydedildi: {raw_time}")
        except Exception as e:
           print(f"{raw_time} için hata oluştu: {e}")
           conn.rollback()
    else:
        print(f"{raw_time} için veri mevcut eklenmedi")
    
    cursor.close()
    conn.close()


fetch_weather_data()
