FROM python:3.10-slim-bullseye
WORKDIR /isni
COPY . .

RUN useradd -m isniuser && \
    chown isniuser:isniuser -R /isni

USER isniuser

RUN pip install -r requirements.txt

CMD python3 -m unittest