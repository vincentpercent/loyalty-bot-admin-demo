FROM python:3.11-slim

WORKDIR /app

# Сначала копируем только requirements.txt
COPY ./bot/requirements.txt ./requirements.txt

# Устанавливаем зависимости (этот слой закешируется)
RUN pip install --no-cache-dir -r requirements.txt \
    fastapi \
    "uvicorn[standard]" \
    python-multipart \
    jinja2

# Теперь копируем код
COPY ./bot ./bot
COPY ./admin ./admin
COPY ./bot/config.py ./config.py

ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "admin.main:app", "--host", "0.0.0.0", "--port", "8000"]

