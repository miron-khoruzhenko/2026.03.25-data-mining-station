import sqlite3
import json
from pathlib import Path
from contextlib import contextmanager

# Вычисляем абсолютный путь до корня проекта (на 3 уровня вверх от db_manager.py)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

class DBManager:
    def __init__(self, db_path: str = None):
        if db_path is None:
            # Теперь база всегда будет создаваться в корневой data/databases/
            self.db_path = PROJECT_ROOT / "data" / "databases" / "mining_state.db"
        else:
            self.db_path = Path(db_path)
            
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        
    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def _init_db(self):
        """Создает таблицы, если они не существуют."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Таблица Режима 1: Очередь категорий и сохранение шаблонов пагинации
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS crawler_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    category_url TEXT UNIQUE NOT NULL,
                    url_template TEXT,
                    current_page INTEGER DEFAULT 1,
                    max_pages INTEGER,
                    status TEXT DEFAULT 'pending'
                )
            ''')
            
            # Таблица Режима 2: Очередь ссылок на карточки деталей
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scraper_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data_url TEXT UNIQUE NOT NULL,
                    source_url TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    extracted_data TEXT
                )
            ''')

            # Таблица кэширования сгенерированных AI селекторов по доменам
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ai_configs (
                    domain TEXT PRIMARY KEY,
                    selectors_json TEXT NOT NULL
                )
            ''')
            conn.commit()

    # --- Методы для Режима 1 (Crawler) ---

    def add_category_task(self, category_url: str):
        """Добавляет новую категорию для сбора ссылок."""
        with self.get_connection() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO crawler_tasks (category_url) VALUES (?)",
                (category_url,)
            )
            conn.commit()

    def get_pending_category(self):
        with self.get_connection() as conn:
            return conn.execute(
                "SELECT * FROM crawler_tasks WHERE status = 'pending' LIMIT 1"
            ).fetchone()

    def update_category_progress(self, task_id: int, current_page: int, url_template: str = None, max_pages: int = None, status: str = 'pending'):
        """Обновляет прогресс краулера. Если скрипт упадет, он продолжит с current_page."""
        with self.get_connection() as conn:
            query = "UPDATE crawler_tasks SET current_page = ?, status = ?"
            params = [current_page, status]
            
            if url_template:
                query += ", url_template = ?"
                params.append(url_template)
            if max_pages:
                query += ", max_pages = ?"
                params.append(max_pages)
                
            query += " WHERE id = ?"
            params.append(task_id)
            
            conn.execute(query, params)
            conn.commit()

    # --- Методы для Режима 2 (Scraper) ---

    def add_scraper_items(self, source_url: str, data_urls: list[str]):
        """Массовое добавление собранных ссылок. Дубликаты игнорируются."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(
                "INSERT OR IGNORE INTO scraper_items (data_url, source_url) VALUES (?, ?)",
                [(url, source_url) for url in data_urls]
            )
            conn.commit()

    def get_pending_item(self):
        """Берет одну ссылку для парсинга и сразу ставит статус 'processing' для защиты от параллельных гонок."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN TRANSACTION")
            
            item = cursor.execute(
                "SELECT * FROM scraper_items WHERE status = 'pending' LIMIT 1"
            ).fetchone()
            
            if item:
                cursor.execute(
                    "UPDATE scraper_items SET status = 'processing' WHERE id = ?", 
                    (item['id'],)
                )
            cursor.execute("COMMIT")
            return item

    def update_item_status(self, item_id: int, status: str, extracted_data: dict = None):
        """Обновляет статус после парсинга и сохраняет JSON с данными."""
        with self.get_connection() as conn:
            data_str = json.dumps(extracted_data, ensure_ascii=False) if extracted_data else None
            conn.execute(
                "UPDATE scraper_items SET status = ?, extracted_data = ? WHERE id = ?",
                (status, data_str, item_id)
            )
            conn.commit()

    def reset_processing_items(self):
        """Сбрасывает зависшие задачи (со статусом 'processing') обратно в 'pending' при перезапуске скрипта."""
        with self.get_connection() as conn:
            conn.execute("UPDATE scraper_items SET status = 'pending' WHERE status = 'processing'")
            conn.commit()
            
# --- Методы для управления БД (Удаление / Очистка) ---

    def delete_scraper_items(self, item_ids: list):
        """Удаляет выбранные строки по их ID."""
        if not item_ids:
            return
        with self.get_connection() as conn:
            # Создаем строку с нужным количеством знаков вопроса для IN (?, ?, ?)
            placeholders = ','.join('?' * len(item_ids))
            conn.execute(f"DELETE FROM scraper_items WHERE id IN ({placeholders})", item_ids)
            conn.commit()

    def clear_all_queues(self):
        """Полностью очищает очереди (Режим 1 и 2). Кэш AI (ai_configs) при этом сохраняется."""
        with self.get_connection() as conn:
            conn.execute("DELETE FROM scraper_items")
            conn.execute("DELETE FROM crawler_tasks")
            conn.commit()
            
    def delete_ai_config(self, domain: str):
        """Удаляет закэшированные селекторы для конкретного домена."""
        with self.get_connection() as conn:
            conn.execute("DELETE FROM ai_configs WHERE domain = ?", (domain,))
            conn.commit()
            
    def update_ai_config(self, domain: str, selectors: dict):
        """Обновляет JSON селекторов для указанного домена после ручного редактирования."""
        with self.get_connection() as conn:
            conn.execute(
                "UPDATE ai_configs SET selectors_json = ? WHERE domain = ?",
                (json.dumps(selectors, ensure_ascii=False), domain)
            )
            conn.commit()
            
    def force_checkpoint(self):
        """Сбрасывает данные из временного WAL-файла в основной .db для безопасного скачивания."""
        with self.get_connection() as conn:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            
    def requeue_items(self, item_ids: list):
        """Возвращает выбранные по ID строки обратно в очередь (статус pending)."""
        if not item_ids: return
        with self.get_connection() as conn:
            placeholders = ','.join('?' * len(item_ids))
            conn.execute(f"UPDATE scraper_items SET status = 'pending' WHERE id IN ({placeholders})", item_ids)
            conn.commit()

    def requeue_all_empty_and_errors(self):
        """Массово находит все пустые и ошибочные строки и возвращает их в очередь."""
        with self.get_connection() as conn:
            # 1. Возвращаем строки с явными ошибками
            conn.execute("UPDATE scraper_items SET status = 'pending' WHERE status IN ('empty', 'error', 'timeout_empty')")
            
            # 2. Ищем "псевдо-успешные" строки (где есть только системные ссылки, а спарсенные данные null)
            done_items = conn.execute("SELECT id, extracted_data FROM scraper_items WHERE status = 'done' AND extracted_data IS NOT NULL").fetchall()
            
            ids_to_requeue = []
            for item in done_items:
                try:
                    data = json.loads(item['extracted_data'])
                    
                    # Удаляем системные поля из проверки
                    data.pop('data_url', None)
                    data.pop('source_url', None)
                    
                    # Если все остальные ключи пустые (None или "") — это "пустышка"
                    if all(v is None or str(v).strip() == "" or str(v).strip() == "-" for v in data.values()):
                        ids_to_requeue.append(item['id'])
                except:
                    pass
            
            # 3. Массово переводим найденные пустышки обратно в pending
            if ids_to_requeue:
                # Разбиваем на батчи (SQLite имеет лимит в 999 переменных на запрос)
                batch_size = 900
                for i in range(0, len(ids_to_requeue), batch_size):
                    batch = ids_to_requeue[i:i + batch_size]
                    placeholders = ','.join('?' * len(batch))
                    conn.execute(f"UPDATE scraper_items SET status = 'pending' WHERE id IN ({placeholders})", batch)
            
            conn.commit()