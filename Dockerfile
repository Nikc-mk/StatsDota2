FROM apache/airflow:2.9.0-python3.12
COPY requirements.txt /
COPY --chown=airflow:root dag_1.py /opt/airflow/dags
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt