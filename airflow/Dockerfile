FROM apache/airflow:2.4.2

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq

# COPY requirements.txt .

USER $AIRFLOW_UID

# RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir 'apache-airflow[amazon]'


WORKDIR $AIRFLOW_HOME