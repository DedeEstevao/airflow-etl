# ===============================
# Airflow base image
# ===============================
ARG AIRFLOW_VERSION=2.9.0
ARG PYTHON_VERSION=3.10

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}


# Copia requirements
COPY requirements.txt /requirements.txt


# Instala dependências
RUN pip install --no-cache-dir \
    -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.10.txt"

# PYTHONPATH para importar etl/, sql/, etc
ENV PYTHONPATH=/opt/airflow
