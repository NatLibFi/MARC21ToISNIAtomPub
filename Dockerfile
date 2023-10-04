FROM python:3.10-slim-bullseye
WORKDIR /isni
COPY . .

RUN useradd -m isniuser && \
    chown isniuser:isniuser -R /isni

USER isniuser

RUN python3 -m venv isni_venv && \
    . ./isni_venv/bin/activate && \
    pip install -U pip && \
    pip install -r requirements.txt && \
    . ./isni_venv/bin/activate

CMD python3 -m unittest
