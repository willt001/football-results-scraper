FROM apache/airflow:2.10.2
COPY requirements.txt .
RUN python -m pip install -r requirements.txt