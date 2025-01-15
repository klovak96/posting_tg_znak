FROM apache/airflow:2.6.0

COPY requirements.txt /opt/airflow/

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt