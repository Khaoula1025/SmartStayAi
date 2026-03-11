FROM apache/airflow:2.9.1-python3.11

# System dependencies for Prophet + psycopg2
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libpq-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

# Prophet first (largest dep — cached as its own layer)
RUN pip install --no-cache-dir prophet==1.1.5

# SmartStay runtime dependencies
RUN pip install --no-cache-dir \
    pandas==2.2.1 \
    numpy==1.26.4 \
    scikit-learn>=1.4.0 \
    joblib \
    psycopg2-binary \
    openpyxl \
    xlrd \
    python-dotenv \
    cryptography