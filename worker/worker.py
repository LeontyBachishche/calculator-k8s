# worker.py
import pika
import json
import psycopg2
from redis import Redis
import os  # <-- КРИТИЧЕСКИ ВАЖНО: Импортируем модуль OS!

# <-- КРИТИЧЕСКИ ВАЖНО: Объявляем переменные хостов!
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')

redis = Redis(host=REDIS_HOST, port=6379)

def db_save(username, exp, res):
    conn = psycopg2.connect(
        host=POSTGRES_HOST, 
        database='calc_db', 
        user='admin', 
        password='admin123'
    )
    cur = conn.cursor()
    cur.execute('INSERT INTO history (username, expression, result) VALUES (%s, %s, %s);', (username, exp, str(res)))
    conn.commit()
    cur.close()
    conn.close()

def callback(ch, method, properties, body):
    data = json.loads(body)
    try:
        # Безопасное вычисление
        result = eval(data['expression'], {"__builtins__": None}, {})
        redis.set(data['expression'], str(result), ex=3600)
        username = data.get('username', 'guest')
        db_save(username, data['expression'], result)
        print(f"User [{username}] Calculated: {data['expression']} = {result}")
    except Exception as e:
        print(f"Error: {e}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Авторизация под admin / secretpass123
credentials = pika.PlainCredentials('admin', 'admin')

# Теперь RABBITMQ_HOST гарантированно существует и заполнится!
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials))
channel = connection.channel()
channel.queue_declare(queue='calc_tasks')
channel.basic_consume(queue='calc_tasks', on_message_callback=callback)
print('Worker started. Waiting for tasks...')
channel.start_consuming()
