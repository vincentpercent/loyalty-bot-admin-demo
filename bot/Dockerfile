FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Устанавливаем Python-зависимости
COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Создаём непривилегированного пользователя
RUN addgroup --system app && adduser --system --ingroup app app

# Копируем исходники бота
COPY . .

RUN chown -R app:app /app
USER app

CMD ["python", "app.py"]

