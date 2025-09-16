FROM apache/airflow:3.0.6
COPY airflow_requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /airflow_requirements.txt