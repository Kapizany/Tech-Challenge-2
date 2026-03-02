FROM python:3.11-slim

# Evita pyc e melhora logs em container
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependências do sistema (certificados + timezone opcional)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
  && rm -rf /var/lib/apt/lists/*

# Instala deps Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código
COPY extract_b3_to_s3.py .



CMD ["python", "extract_b3_to_s3.py"]