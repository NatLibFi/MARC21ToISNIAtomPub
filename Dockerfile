FROM python:3.10-slim-bullseye
WORKDIR /app
COPY . .

RUN pip install -r requirements.txt && \
    useradd -m appuser && \
    chown appuser:appuser -R /app

USER appuser
