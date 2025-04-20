# Real Time İstanbul Weather Forecast
Bu proje, İstanbul için hava durumu tahminlerini gerçek zamanlı olarak gerçekleştiren bir makine öğrenmesi uygulamasıdır. Projede GRU (Gated Recurrent Unit) modeli kullanılarak sıcaklık, nem ve hava durumu açıklaması (weather_description) tahmin edilmiştir. Tüm süreç Docker konteynerları içinde çalışacak şekilde yapılandırılmış, veri akışı ve zamanlanmış görevler ise Apache Airflow ile yönetilmiştir. Sonuçlar görsel olarak Streamlit üzerinden kullanıcıya sunulmaktadır.

# Kullanılan Teknolojiler

- Python – Tüm modelleme ve iş akışı kodları için

- GRU (Gated Recurrent Unit) – Zaman serisi tahmini için derin öğrenme modeli

- Docker – Proje ortamını izole bir şekilde çalıştırmak için konteynerleştirme aracı

- Apache Airflow – Veri toplama, işleme ve model çalıştırma süreçlerini zamanlamak ve yönetmek için

- Streamlit – Kullanıcı dostu bir arayüz ile tahmin sonuçlarını görselleştirmek için

- PostgreSQL – Verilerin saklandığı ilişkisel veritabanı

- Open-Meteo API – Geçmiş hava durumu verilerini çekmek için

- OpenWeatherMap API – Gerçek zamanlı (güncel) hava durumu verilerini almak için
