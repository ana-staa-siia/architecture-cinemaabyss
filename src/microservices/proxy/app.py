from flask import Flask, Response, request
import requests
import os
import random
import logging

app = Flask(__name__)

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Конфигурация из переменных окружения
MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")
GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "false").lower() == "true"
MOVIES_MIGRATION_PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", 0))

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def proxy(path):
    # Определяем целевой URL
    target_url = get_target_url(path)
    logger.info(f"redirect to : {target_url}")
    full_url = f"{target_url}/{path}"

    # Копируем заголовки из исходного запроса
    headers = {key: value for key, value in request.headers if key != 'Host'}

    # Передаём метод, данные и заголовки
    response = requests.request(
        method=request.method,
        url=full_url,
        headers=headers,
        data=request.get_data(),
        params=request.args,
        json=request.json if request.is_json else None,
        stream=False  # Важно: отключаем stream mode
    )

    # Формируем правильные заголовки для ответа
    excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
    response_headers = [
        (name, value)
        for name, value in response.raw.headers.items()
        if name.lower() not in excluded_headers
    ]

    # Возвращаем ответ от целевого сервиса
    return response.content, response.status_code, response_headers

def get_target_url(path):
    # Логика маршрутизации для /movies
    if path.startswith("api/movies"):
        if not GRADUAL_MIGRATION:
            return MONOLITH_URL
        random_value = random.randint(0, 99)
        return MOVIES_SERVICE_URL if random_value < MOVIES_MIGRATION_PERCENT else MONOLITH_URL
    # Все остальные запросы идут на монолит
    return MONOLITH_URL

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)