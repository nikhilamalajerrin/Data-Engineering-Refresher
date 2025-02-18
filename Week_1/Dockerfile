FROM python:3.13

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app

#COPY pipeline.py pipeline.py
COPY ingest_data.py injest_data.py

ENTRYPOINT ["python","injest_data.py"]
