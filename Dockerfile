FROM apache/airflow:2.11.1-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# Instala dependências do projeto
COPY pyproject.toml /opt/airflow/pyproject.toml
RUN pip install --no-cache-dir \
    polars>=1.0.0 \
    httpx>=0.28.1 \
    python-dotenv>=1.2.1 \
    requests>=2.32.5 \
    rich>=14.0.0

# Copia código-fonte (será sobrescrito pelo volume em dev)
COPY src/ /opt/airflow/src/
COPY core/ /opt/airflow/core/
COPY main.py /opt/airflow/main.py

WORKDIR /opt/airflow
