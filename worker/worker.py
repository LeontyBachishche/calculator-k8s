import pika
import json
import psycopg2
from redis import Redis
import os
import time

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

def init_db():
    """Ожидает запуск PostgreSQL и автоматически создает таблицу history, если её нет"""
    print("⏳ Ожидание готовности PostgreSQL для воркера...")
    while True:
        try:
            conn = psycopg2.connect(host=POSTGRES_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS history (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(255) NOT NULL,
                    expression VARCHAR(255) NOT NULL,
                    result VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
            cur.close()
            conn.close()
            print("✅ База данных успешно проверена. Таблица 'history' готова к работе.")
            break  # Выходим из цикла, когда таблица успешно создана
        except psycopg2.OperationalError:
            print("❗ PostgreSQL еще не принимает соединения. Повторная попытка через 3 секунды...")
            time.sleep(3)
        except Exception as e:
            print(f"❌ Непредвиденная ошибка инициализации таблицы history: {e}")
            time.sleep(5)

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
        print(f"Error evaluating/saving: {e}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():

    init_db() 
    

    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    
    # Бесконечный цикл для ожидания запуска RabbitMQ в K8s
    while True:
        try:
            print('Connecting to RabbitMQ...')
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.queue_declare(queue='calc_tasks')
            channel.basic_consume(queue='calc_tasks', on_message_callback=callback)
            print('Worker started. Waiting for tasks...')
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            print("❗ RabbitMQ еще не готов. Повторная попытка через 5 секунд...")
            time.sleep(5)
        except KeyboardInterrupt:
            print("Worker stopped.")
            break

if __name__ == '__main__':
    main()
