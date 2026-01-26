# ===============================
# Airflow base image
# ===============================
ARG AIRFLOW_VERSION=2.9.0
ARG PYTHON_VERSION=3.10

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# ===============================
# Switch to root to install deps
# ===============================

USER root

# Copia requirements
COPY requirements.txt /requirements.txt

# Ajusta permissões
RUN chown airflow:root /requirements.txt

# Volta para o usuário airflow
USER airflow

# Instala dependências corretamente
RUN pip install --no-cache-dir \
    -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.10.txt" \
