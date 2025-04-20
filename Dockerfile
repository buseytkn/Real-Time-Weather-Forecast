FROM apache/airflow:2.3.2

USER airflow
RUN pip install streamlit

COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

