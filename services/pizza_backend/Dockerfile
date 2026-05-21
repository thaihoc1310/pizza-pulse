FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pizza_backend ./pizza_backend

EXPOSE 8080

CMD ["uvicorn", "pizza_backend.app:app", "--host", "0.0.0.0", "--port", "8080"]
