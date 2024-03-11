FROM apache/airflow:2.8.0

ADD requirements.txt .
RUN pip install -r requirements.txt
