from flask import Flask, jsonify, request
from confluent_kafka import Producer, Consumer, KafkaException
import threading
import json
import logging
import os

app = Flask(__name__)

# Настройка логгирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация Kafka
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC_USER = os.getenv("KAFKA_TOPIC_USER", "user_events")
KAFKA_TOPIC_PAYMENT = os.getenv("KAFKA_TOPIC_PAYMENT", "payment_events")
KAFKA_TOPIC_MOVIE = os.getenv("KAFKA_TOPIC_MOVIE", "movie_events")

# Конфигурация Producer
producer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
}

# Создание Producer
producer = Producer(producer_conf)

# Функция для отправки сообщения в Kafka
def send_event(topic, event_data):
    try:
        producer.produce(topic, json.dumps(event_data).encode('utf-8'))
        producer.flush()
        logger.info(f"Event sent to topic {topic}: {event_data}")
    except Exception as e:
        logger.error(f"Failed to send event to topic {topic}: {e}")

# Функция для потребления сообщений из Kafka
def consume_events(topic):
    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': f'{topic}_consumer_group',
        'auto.offset.reset': 'earliest',
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            event_data = json.loads(msg.value().decode('utf-8'))
            logger.info(f"Consumed event from topic {topic}: {event_data}")
    except Exception as e:
        logger.error(f"Error in consumer for topic {topic}: {e}")
    finally:
        consumer.close()

# Запуск Consumer в отдельных потоках
threading.Thread(target=consume_events, args=(KAFKA_TOPIC_USER,), daemon=True).start()
threading.Thread(target=consume_events, args=(KAFKA_TOPIC_PAYMENT,), daemon=True).start()
threading.Thread(target=consume_events, args=(KAFKA_TOPIC_MOVIE,), daemon=True).start()

# API для проверки работоспособности
@app.get('/api/events/health')
def get_events_service_health():
    return jsonify({"status": True})

# API для создания событий
@app.route('/api/events/user', methods=['POST'])
def create_user_event():
    event_data = request.json
    send_event(KAFKA_TOPIC_USER, event_data)
    return jsonify({"status": "success", "message": "User event created"}), 201

@app.route('/api/events/payment', methods=['POST'])
def create_payment_event():
    event_data = request.json
    send_event(KAFKA_TOPIC_PAYMENT, event_data)
    return jsonify({"status": "success", "message": "Payment event created"}), 201

@app.route('/api/events/movie', methods=['POST'])
def create_movie_event():
    event_data = request.json
    send_event(KAFKA_TOPIC_MOVIE, event_data)
    return jsonify({"status": "success", "message": "Movie event created"}), 201

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8082, debug=True)
