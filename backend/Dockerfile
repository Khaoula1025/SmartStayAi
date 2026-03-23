# backend/Dockerfile
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libpq-dev curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip install uv

# Copy dependency files
COPY pyproject.toml uv.lock* ./

# Create dummy README so hatchling doesn't complain
RUN touch README.md

# Install only dependencies, not the project itself
RUN uv sync --frozen --no-dev --no-install-project

# Copy backend app source
COPY backend/ ./

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
