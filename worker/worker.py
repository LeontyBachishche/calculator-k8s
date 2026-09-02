import pika
import json
import psycopg2
from redis import Redis
import os

REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')

DB_NAME = os.getenv('DB_NAME', 'calc_db')
DB_USER = os.getenv('DB_USER', 'admin')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'admin123')

RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'admin')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', 'admin')
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)

redis = Redis(host=REDIS_HOST, port=6379, password=REDIS_PASSWORD, decode_responses=True)

def db_save(username, exp, res):
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()
    cur.execute('INSERT INTO history (username, expression, result) VALUES (%s, %s, %s);', (username, exp, str(res)))
    conn.commit()
    cur.close()
    conn.close()

def callback(ch, method, properties, body):
    data = json.loads(body)
    try:
        result = eval(data['expression'], {"__builtins__": None}, {})
        redis.set(data['expression'], str(result), ex=3600)
        username = data.get('username', 'guest')
        db_save(username, data['expression'], result)
        print(f"User [{username}] Calculated: {data['expression']} = {result}")
    except Exception as e:
        print(f"Error: {e}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)

connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials))
channel = connection.channel()
channel.queue_declare(queue='calc_tasks')
channel.basic_consume(queue='calc_tasks', on_message_callback=callback)
print('Worker started. Waiting for tasks...')
channel.start_consuming()
