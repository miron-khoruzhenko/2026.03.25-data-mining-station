import sqlite3, json
from pathlib import Path
from contextlib import contextmanager

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

class DBManager:
    def __init__(self, db_path=None):
        self.db_path = Path(db_path) if db_path else PROJECT_ROOT / "data" / "databases" / "mining_state.db"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        
    @contextmanager
    def get_connection(self):
        # 1. timeout=30.0 заставляет потоки ждать до 30 секунд, если база занята, вместо краша
        # 2. check_same_thread=False разрешает использование коннекта из разных потоков asyncio
        conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try: 
            # 3. Включаем режим WAL для максимальной параллельности (чтение не блокирует запись)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            yield conn
        finally: 
            conn.close()

    def _init_db(self):
        with self.get_connection() as c:
            c.execute("CREATE TABLE IF NOT EXISTS crawler_tasks (id INTEGER PRIMARY KEY AUTOINCREMENT, category_url TEXT UNIQUE NOT NULL, url_template TEXT, current_page INTEGER DEFAULT 1, max_pages INTEGER, status TEXT DEFAULT 'pending')")
            c.execute("CREATE TABLE IF NOT EXISTS scraper_items (id INTEGER PRIMARY KEY AUTOINCREMENT, data_url TEXT UNIQUE NOT NULL, source_url TEXT NOT NULL, status TEXT DEFAULT 'pending', extracted_data TEXT)")
            c.execute("CREATE TABLE IF NOT EXISTS ai_configs (domain TEXT PRIMARY KEY, selectors_json TEXT NOT NULL)")
            c.commit()

    def add_category_task(self, category_url):
        with self.get_connection() as c: c.execute("INSERT OR IGNORE INTO crawler_tasks (category_url) VALUES (?)", (category_url,)); c.commit()

    def get_pending_category(self):
        with self.get_connection() as c: return c.execute("SELECT * FROM crawler_tasks WHERE status = 'pending' LIMIT 1").fetchone()

    def update_category_progress(self, task_id, current_page, url_template=None, max_pages=None, status='pending'):
        with self.get_connection() as c:
            q, p = "UPDATE crawler_tasks SET current_page = ?, status = ?", [current_page, status]
            if url_template: q += ", url_template = ?"; p.append(url_template)
            if max_pages: q += ", max_pages = ?"; p.append(max_pages)
            c.execute(q + " WHERE id = ?", p + [task_id]); c.commit()

    def add_scraper_items(self, source_url, data_urls):
        with self.get_connection() as c:
            c.executemany("INSERT OR IGNORE INTO scraper_items (data_url, source_url) VALUES (?, ?)", [(u, source_url) for u in data_urls])
            c.commit()

    def get_pending_item(self):
        with self.get_connection() as c:
            # 4. Используем BEGIN IMMEDIATE. Это предотвращает дедлоки, сразу запрещая другим потокам начинать транзакции записи
            c.execute("BEGIN IMMEDIATE")
            item = c.execute("SELECT * FROM scraper_items WHERE status = 'pending' LIMIT 1").fetchone()
            if item: c.execute("UPDATE scraper_items SET status = 'processing' WHERE id = ?", (item['id'],))
            c.execute("COMMIT")
            return item

    def update_item_status(self, item_id, status, extracted_data=None):
        with self.get_connection() as c:
            # Добавлена небольшая оптимизация записи JSON
            c.execute("UPDATE scraper_items SET status = ?, extracted_data = ? WHERE id = ?", (status, json.dumps(extracted_data, ensure_ascii=False) if extracted_data else None, item_id))
            c.commit()

    def reset_processing_items(self):
        with self.get_connection() as c: c.execute("UPDATE scraper_items SET status = 'pending' WHERE status = 'processing'"); c.commit()
            
    def delete_scraper_items(self, item_ids):
        if not item_ids: return
        with self.get_connection() as c:
            c.execute(f"DELETE FROM scraper_items WHERE id IN ({','.join('?'*len(item_ids))})", item_ids); c.commit()

    def clear_all_queues(self):
        with self.get_connection() as c: c.execute("DELETE FROM scraper_items"); c.execute("DELETE FROM crawler_tasks"); c.commit()
            
    def delete_ai_config(self, domain):
        with self.get_connection() as c: c.execute("DELETE FROM ai_configs WHERE domain = ?", (domain,)); c.commit()
            
    def update_ai_config(self, domain, selectors):
        with self.get_connection() as c: c.execute("INSERT OR REPLACE INTO ai_configs (domain, selectors_json) VALUES (?, ?)", (domain, json.dumps(selectors, ensure_ascii=False))); c.commit()
            
    def force_checkpoint(self):
        with self.get_connection() as c: c.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            
    def requeue_items(self, item_ids):
        if not item_ids: return
        with self.get_connection() as c: c.execute(f"UPDATE scraper_items SET status = 'pending' WHERE id IN ({','.join('?'*len(item_ids))})", item_ids); c.commit()

    def requeue_all_empty_and_errors(self):
        with self.get_connection() as c:
            c.execute("UPDATE scraper_items SET status = 'pending' WHERE status IN ('empty', 'error', 'timeout_empty')")
            done_items = c.execute("SELECT id, extracted_data FROM scraper_items WHERE status = 'done' AND extracted_data IS NOT NULL").fetchall()
            ids = [i['id'] for i in done_items if (d := json.loads(i['extracted_data'])) and [d.pop('data_url', None), d.pop('source_url', None)] and all(v is None or str(v).strip() in ("", "-") for v in d.values())]
            for i in range(0, len(ids), 900): c.execute(f"UPDATE scraper_items SET status = 'pending' WHERE id IN ({','.join('?'*len(ids[i:i+900]))})", ids[i:i+900])
            c.commit()