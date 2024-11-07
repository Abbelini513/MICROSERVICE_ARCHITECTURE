import pika
import numpy as np
import json
from sklearn.datasets import load_diabetes
import time
from datetime import datetime

# Загружаем датасет о диабете (X — признаки, y — метки)
X, y = load_diabetes(return_X_y=True)

# Бесконечный цикл для отправки сообщений
while True:
    try:
        # Создаем подключение к RabbitMQ на хосте 'rabbitmq'
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()

        # Объявляем очереди y_true и features, если их нет
        channel.queue_declare(queue='y_true')
        channel.queue_declare(queue='features')

        # Формируем случайный индекс строки для выбора данных
        random_row = np.random.randint(0, X.shape[0])

        # Генерируем уникальный идентификатор на основе текущего времени (timestamp)
        message_id = datetime.timestamp(datetime.now())

        # Создаем сообщения с идентификатором
        message_y_true = {
            'id': message_id,
            'body': float(y[random_row])  # Преобразуем метку в float для JSON
        }
        message_features = {
            'id': message_id,
            'body': X[random_row].tolist()  # Преобразуем вектор признаков в list для JSON
        }

        # Публикуем сообщения в очереди y_true и features
        channel.basic_publish(exchange='', routing_key='y_true', body=json.dumps(message_y_true))
        print(f"Сообщение с правильным ответом отправлено в очередь: {message_y_true}")

        channel.basic_publish(exchange='', routing_key='features', body=json.dumps(message_features))
        print(f"Сообщение с вектором признаков отправлено в очередь: {message_features}")

        # Закрываем подключение
        connection.close()

        # Задержка перед следующей итерацией
        time.sleep(10)

    except pika.exceptions.AMQPConnectionError:
        print("Не удалось подключиться к очереди. Повтор через 5 секунд...")
        time.sleep(5)
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        time.sleep(5)