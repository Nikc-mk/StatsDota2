FROM apache/airflow:2.9.0-python3.12
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
COPY --chown=airflow:root dags/* /opt/airflow/dags