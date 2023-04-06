FROM apache/airflow:2.5.2-python3.10


USER airflow

COPY requirements.txt .
RUN pip install apache-airflow[azure]
RUN pip install -r requirements.txt