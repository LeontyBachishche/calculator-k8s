FROM python:3.10-slim
WORKDIR /app
RUN pip install pika psycopg2-binary redis
COPY worker.py .
CMD ["python", "worker.py"]
