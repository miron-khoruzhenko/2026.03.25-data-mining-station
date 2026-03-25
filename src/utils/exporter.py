import sys
import os
import json
import pandas as pd
from datetime import datetime
from pathlib import Path

# Подключение модулей из корня проекта
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from src.db.db_manager import DBManager

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

class ExcelExporter:
    def __init__(self):
        self.db = DBManager()
        self.exports_dir = PROJECT_ROOT / "data" / "exports"
        self.exports_dir.mkdir(parents=True, exist_ok=True)

    def export_done_items(self, filename_prefix="export"):
        """
        Выгружает все успешно собранные данные (status='done') в Excel.
        """
        print("\n[*] Запуск экспорта данных в Excel...")
        
        with self.db.get_connection() as conn:
            # Получаем все завершенные задачи
            rows = conn.execute("SELECT extracted_data FROM scraper_items WHERE status = 'done'").fetchall()
        
        if not rows:
            print("[-] Нет данных для экспорта (нет записей со статусом 'done').")
            return None

        data_list = []
        for row in rows:
            raw_json = row['extracted_data']
            if raw_json:
                try:
                    parsed_data = json.loads(raw_json)
                    data_list.append(parsed_data)
                except json.JSONDecodeError:
                    print(f"[!] Ошибка парсинга JSON для строки: {raw_json}")
                    continue

        if not data_list:
            print("[-] Данные пусты после парсинга.")
            return None

        # Создаем DataFrame (таблицу) из списка словарей
        df = pd.DataFrame(data_list)
        
        # 1. Базовые колонки, которые мы хотим видеть первыми
        base_columns = [
            'company_name', 'company_phone', 'company_email', 
            'company_website', 'company_fax'
        ]
        
        # 2. Системные колонки, которые лучше держать в конце
        system_columns = ['data_url', 'source_url']
        
        # 3. Определяем динамические (кастомные) колонки
        all_columns = list(df.columns)
        custom_columns = [col for col in all_columns if col not in base_columns and col not in system_columns]
        
        # Формируем итоговый порядок колонок
        final_columns = []
        
        # Добавляем базовые (только те, что реально есть в данных)
        for col in base_columns:
            if col in all_columns:
                final_columns.append(col)
        
        # Вставляем кастомные посередине
        final_columns.extend(custom_columns)
        
        # Вставляем системные в конец
        for col in system_columns:
            if col in all_columns:
                final_columns.append(col)

        # Переставляем колонки в DataFrame
        df = df[final_columns]
        
        # Формируем имя файла с датой и временем
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"{filename_prefix}_{timestamp}.xlsx"
        filepath = self.exports_dir / filename
        
        # Сохраняем в Excel
        try:
            df.to_excel(filepath, index=False, engine='openpyxl')
            print(f"[+] Экспорт успешно завершен! Сохранено строк: {len(df)}")
            print(f"[+] Файл сохранен по пути: {filepath}")
            return filepath
        except Exception as e:
            print(f"[!] Ошибка при сохранении Excel файла: {e}")
            return None


# ==========================================
# БЛОК ИЗОЛИРОВАННОГО ТЕСТИРОВАНИЯ
# ==========================================
if __name__ == '__main__':
    exporter = ExcelExporter()
    exporter.export_done_items("test_export")