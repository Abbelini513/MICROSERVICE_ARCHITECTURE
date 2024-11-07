import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import time
from datetime import datetime

# Пути к файлу metric_log.csv и к директории logs
log_file = Path("/usr/src/app/logs/metric_log.csv")
plot_path = Path("/usr/src/app/logs/error_distribution.png")

def plot_error_distribution():
    """Функция для построения и сохранения гистограммы распределения ошибок."""
    if log_file.exists() and log_file.stat().st_size > 0:
        data = pd.read_csv(log_file)
        
        # Настройка графика
        plt.figure(figsize=(10, 6))
        sns.histplot(data["absolute_error"], kde=True, color="orange", bins=15)
        plt.xlabel("Absolute Error")
        plt.ylabel("Count")
        plt.title("Distribution of Absolute Errors in Model Predictions")
        
        # Добавляем статистические линии для среднего и медианы
        mean_error = data["absolute_error"].mean()
        median_error = data["absolute_error"].median()
        plt.axvline(mean_error, color='red', linestyle='--', linewidth=1.5, label=f'Mean: {mean_error:.2f}')
        plt.axvline(median_error, color='blue', linestyle='--', linewidth=1.5, label=f'Median: {median_error:.2f}')
        plt.legend()
        
        # Добавляем временную метку на график
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        plt.figtext(0.15, 0.01, f"Generated: {timestamp}", ha="left", fontsize=8, alpha=0.6)
        
        # Сохранение графика
        plt.savefig(plot_path, dpi=200, bbox_inches='tight')
        plt.close()
        print(f"График успешно обновлен: {plot_path} в {timestamp}")
    else:
        print("Файл логов не найден или пуст, ожидание...")

print(" [*] Ожидание обновления для построения графика. Нажмите CTRL+C для выхода.")
while True:
    plot_error_distribution()
    time.sleep(10)