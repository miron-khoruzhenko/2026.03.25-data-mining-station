# Используем официальный легкий образ Python
FROM python:3.11-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем список зависимостей и устанавливаем их
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Устанавливаем системные зависимости браузера для Playwright (это критический шаг)
RUN playwright install --with-deps chromium

# Копируем весь остальной код проекта
COPY . .

# Открываем порт для Streamlit
EXPOSE 8501

# Команда для запуска нашего дашборда
CMD ["python", "-m", "streamlit", "run", "src/ui/app.py", "--server.port=8501", "--server.address=0.0.0.0"]