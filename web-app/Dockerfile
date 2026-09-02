FROM python:3.10-slim
WORKDIR /app
RUN pip install flask redis psycopg2-binary pika prometheus_client
COPY app.py .
CMD ["python", "app.py"]
