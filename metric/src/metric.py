import pika
import pandas as pd
import json
from pathlib import Path

# Создание папки logs, если она не существует
log_dir = Path("/usr/src/app/logs")
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / "metric_log.csv"

# Инициализация файла metric_log.csv с заголовками
if not log_file.exists():
    with open(log_file, "w") as f:
        f.write("id,y_true,y_pred,absolute_error\n")

# Подключение к RabbitMQ
try:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
    channel = connection.channel()

    # Объявление очередей
    channel.queue_declare(queue="y_true")
    channel.queue_declare(queue="y_pred")

    # Хранилище для временного хранения данных
    data_store = {}

    def calculate_and_log_error():
        """Вычисление абсолютной ошибки и запись в лог-файл."""
        with open(log_file, "a") as f:
            for message_id, values in list(data_store.items()):
                if "y_true" in values and "y_pred" in values:
                    y_true = values["y_true"]
                    y_pred = values["y_pred"]
                    abs_error = abs(y_true - y_pred)
                    # Запись в CSV
                    f.write(f"{message_id},{y_true},{y_pred},{abs_error}\n")
                    f.flush()  # Принудительная запись в файл
                    print(f"Записаны метрики для ID {message_id}: y_true={y_true}, y_pred={y_pred}, absolute_error={abs_error}")
                    # Удаление обработанных данных
                    del data_store[message_id]

    def callback_y_true(ch, method, properties, body):
        """Обработка сообщений из очереди y_true."""
        message = json.loads(body)
        message_id = message["id"]
        y_true = message["body"]
        data_store.setdefault(message_id, {})["y_true"] = y_true
        calculate_and_log_error()

    def callback_y_pred(ch, method, properties, body):
        """Обработка сообщений из очереди y_pred."""
        message = json.loads(body)
        message_id = message["id"]
        y_pred = message["body"]
        data_store.setdefault(message_id, {})["y_pred"] = y_pred
        calculate_and_log_error()

    # Подписка на очереди
    channel.basic_consume(queue="y_true", on_message_callback=callback_y_true, auto_ack=True)
    channel.basic_consume(queue="y_pred", on_message_callback=callback_y_pred, auto_ack=True)

    print(" [*] Ожидание сообщений для вычисления метрик. Нажмите CTRL+C для выхода.")
    channel.start_consuming()

except KeyboardInterrupt:
    print("\nПрограмма остановлена пользователем")
except Exception as e:
    print(f"Ошибка при подключении к RabbitMQ: {e}")
finally:
    if 'connection' in locals() and connection.is_open:
        connection.close()