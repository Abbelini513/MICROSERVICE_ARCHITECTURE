import pika
import pickle
import numpy as np
import json
import time

# Загрузка сериализованной модели из файла
with open('myfile.pkl', 'rb') as pkl_file:
    regressor = pickle.load(pkl_file)

try:
    # Устанавливаем соединение с RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    # Объявляем очереди features и y_pred
    channel.queue_declare(queue='features')
    channel.queue_declare(queue='y_pred')

    # Определяем callback-функцию для обработки входящих сообщений
    def callback(ch, method, properties, body):
        try:
            # Парсим входящее сообщение
            message = json.loads(body)
            message_id = message['id']
            features = message['body']
            
            print(f'Получен вектор признаков для предсказания (ID: {message_id}): {features}')
            
            # Делаем предсказание
            pred = regressor.predict(np.array(features).reshape(1, -1))
            
            # Формируем сообщение с предсказанием
            prediction_message = {
                'id': message_id,
                'body': float(pred[0])  # Преобразуем в float для корректной сериализации
            }
            
            # Отправляем предсказание в очередь y_pred
            channel.basic_publish(
                exchange='',
                routing_key='y_pred',
                body=json.dumps(prediction_message)
            )
            print(f'Предсказание {pred[0]} отправлено в очередь y_pred (ID: {message_id})')
            
        except Exception as e:
            print(f'Ошибка при обработке сообщения: {e}')

    # Подписка на очередь features и назначение callback-функции
    channel.basic_consume(
        queue='features',
        on_message_callback=callback,
        auto_ack=True
    )
    
    print('...Ожидание сообщений, для выхода нажмите CTRL+C')
    channel.start_consuming()

except KeyboardInterrupt:
    print("\nПрограмма остановлена пользователем")
except Exception as e:
    print(f'Не удалось подключиться к очереди: {e}')
    time.sleep(5)
finally:
    if 'connection' in locals() and connection.is_open:
        connection.close()