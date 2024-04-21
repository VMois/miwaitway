FROM python:3.12.3-slim-bookworm

WORKDIR /app

ADD requirements.txt .
RUN pip install -r requirements.txt

ADD ingest_vehicle_positions.py .
ENTRYPOINT [ "python", "ingest_vehicle_positions.py" ]
