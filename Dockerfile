FROM apache/airflow:2.10.3

COPY requirements.txt /opt/airflow/

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt