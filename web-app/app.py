from flask import Flask, request, jsonify, render_template_string, Response
from redis import Redis
import psycopg2
import pika
import json
import os
from prometheus_client import Counter, generate_latest

app = Flask(__name__)

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

REQUEST_COUNT = Counter('calc_requests_total', 'Total app requests', ['method', 'endpoint'])

HTML_INTERFACE = """
<!DOCTYPE html>
<html>
<head>
    <title>Микросервисный Калькулятор</title>
    <style>
        body { font-family: Arial, sans-serif; background: #f4f4f9; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }
        .calc-container { background: white; padding: 30px; border-radius: 10px; box-shadow: 0 4px 15px rgba(0,0,0,0.1); width: 350px; }
        h2 { text-align: center; color: #333; margin-top: 0; }
        input, button { width: 100%; padding: 12px; margin: 8px 0; border-radius: 5px; border: 1px solid #ccc; box-sizing: border-box; font-size: 16px; }
        button { background: #007bff; color: white; border: none; cursor: pointer; font-weight: bold; }
        button:hover { background: #0056b3; }
        .result-box { margin-top: 15px; padding: 15px; border-radius: 5px; background: #e9ecef; font-weight: bold; text-align: center; min-height: 20px; }
        .cache-badge { color: green; font-size: 12px; display: block; margin-top: 5px; }
    </style>
</head>
<body>
    <div class="calc-container">
        <h2>Калькулятор K8s</h2>
        <input type="text" id="username" placeholder="Ваше имя (для истории)" value="dmitry">
        <input type="text" id="expression" placeholder="Пример (например: 2+2*3 или 100/4)">
        <button onclick="sendCalculate()">Посчитать</button>
        <div class="result-box" id="result-output">Результат появится здесь...</div>
    </div>

    <script>
        async function sendCalculate() {
            const user = document.getElementById('username').value;
            const expr = document.getElementById('expression').value;
            const output = document.getElementById('result-output');

            output.innerHTML = "⏱ Отправка в очередь...";

            try {
                await fetch('/register', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ username: user })
                });

                const response = await fetch('/calculate', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ username: user, expression: expr })
                });
                const data = await response.json();

                if (data.source === 'cache') {
                    output.innerHTML = `🎉 Ответ: ${data.result} <span class="cache-badge">(Взято из кэша Redis)</span>`;
                } else {
                    output.innerHTML = "⏳ Задача в RabbitMQ. Подождите 2 сек и нажмите кнопку снова!";
                }
            } catch (err) {
                output.innerHTML = "❌ Ошибка соединения с сервером";
            }
        }
    </script>
</body>
</html>
"""

def get_db_connection():
    return psycopg2.connect(host=POSTGRES_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)

@app.route('/')
def index():
    return render_template_string(HTML_INTERFACE)

@app.route('/metrics')
def metrics():
    return Response(generate_latest(), mimetype='text/plain')

@app.route('/register', methods=['POST'])
def register():
    REQUEST_COUNT.labels(method='POST', endpoint='/register').inc()
    data = request.json
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('INSERT INTO users (username) VALUES (%s) ON CONFLICT DO NOTHING;', (data['username'],))
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"status": "registered"})

@app.route('/calculate', methods=['POST'])
def calculate():
    REQUEST_COUNT.labels(method='POST', endpoint='/calculate').inc()
    data = request.json
    cached_res = redis.get(data['expression'])
    if cached_res:
        return jsonify({"result": cached_res, "source": "cache"})

    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue='calc_tasks')
    channel.basic_publish(exchange='', routing_key='calc_tasks', body=json.dumps(data))
    connection.close()
    return jsonify({"status": "task queued", "source": "queue"})

def init_db():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("✅ База данных успешно инициализирована (таблица users готова).")
    except Exception as e:
        print(f"❌ Не удалось инициализировать БД: {e}")

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000)
