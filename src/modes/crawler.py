import sys, os, time, asyncio, random, json
from urllib.parse import urlparse, urljoin
try: from streamlit.runtime.scriptrunner import StopException
except ImportError: pass

# Позволяет запускать файл напрямую из любой директории
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.db.db_manager import DBManager
from src.core.browser import BrowserManager
from src.ai.extractor import AIExtractor

class CategoryCrawler:
    def __init__(self):
        self.db = DBManager()
        self.browser = BrowserManager()
        self.ai = AIExtractor()
        self.start_time = 0

    def _log(self, msg, ui_callback=None):
        print(msg)
        if ui_callback: ui_callback(msg)

    def run(self, max_pages_to_test=None, headless=True, ui_callback=None):
        """Синхронная обертка для вызова из интерфейса/демона."""
        try: asyncio.run(self._async_run(max_pages_to_test, headless, ui_callback))
        except StopException:
            self._log("\n[🛑] Процесс прерван.", ui_callback); raise
        except Exception as e: self._log(f"\n[!] Ошибка: {e}", ui_callback)

    async def _async_run(self, max_pages, headless, ui_callback):
        task = await asyncio.to_thread(self.db.get_pending_category)
        if not task:
            self._log("[-] Нет категорий в очереди (crawler_tasks).", ui_callback)
            return

        self._log(f"[*] Запуск краулера для категории: {task['category_url']}", ui_callback)
        await self.browser.start(headless=headless)
        self.start_time = time.time()

        try:
            await self._process_category(task, max_pages, ui_callback)
        finally:
            self._log("[~] Закрытие браузера...", ui_callback)
            await self.browser.close()

    async def _get_cached_config(self, domain_key):
        """Извлекает настройки пагинации из общего кэша AI."""
        def fetch():
            with self.db.get_connection() as conn:
                row = conn.execute("SELECT selectors_json FROM ai_configs WHERE domain = ?", (domain_key,)).fetchone()
                return json.loads(row['selectors_json']) if row else None
        return await asyncio.to_thread(fetch)

    async def _process_category(self, task, max_pages, ui_callback):
        task_id = task['id']
        base_url = task['category_url']
        current_page = task['current_page']
        url_template = task['url_template']
        
        # Специальный ключ для кэша, чтобы не смешивать селекторы списков и карточек
        domain_key = "crawler_" + urlparse(base_url).netloc 

        page = await self.browser.new_page()

        # --- ФАЕРВОЛ: Блокируем скачивание тяжелого мусора ---
        async def block_aggressively(route):
            if route.request.resource_type in ["image", "media", "font"]: await route.abort()
            else: await route.continue_()
        await page.route("**/*", block_aggressively)
        # -----------------------------------------------------

        # --- ШАГ 1: ПОЛУЧЕНИЕ НАСТРОЕК (КЭШ ИЛИ AI) ---
        ai_config = await self._get_cached_config(domain_key)
        
        if not ai_config or not url_template:
            self._log("[~] Настройки пагинации не найдены. Запрашиваю у AI...", ui_callback)
            try:
                await page.goto(base_url, timeout=60000, wait_until="domcontentloaded")
                await self.browser.check_captcha_and_pause(page)
                
                clean_html = await self.browser.get_clean_html(page)
                ai_config = await asyncio.to_thread(self.ai.get_pagination_config, clean_html, base_url)
                
                url_template = ai_config.get('url_template')
                item_selector = ai_config.get('item_selector')
                
                if not item_selector:
                    self._log("[!] AI не смог найти селектор карточек. Пропуск.", ui_callback)
                    await asyncio.to_thread(self.db.update_category_progress, task_id, current_page, status='error')
                    return
                    
                self._log(f"[+] AI вернул шаблон: {url_template}", ui_callback)
                self._log(f"[+] AI вернул селектор карточек: {item_selector}", ui_callback)
                
                # Сохраняем в кэш БД
                await asyncio.to_thread(self.db.update_ai_config, domain_key, ai_config)
                await asyncio.to_thread(self.db.update_category_progress, task_id, current_page, url_template=url_template)
            except Exception as e:
                self._log(f"[!] Ошибка AI инициализации: {e}", ui_callback)
                return
        else:
            item_selector = ai_config.get('item_selector')
            self._log("[~] Настройки пагинации успешно загружены из кэша.", ui_callback)

        # --- ШАГ 2: ЦИКЛ ПАГИНАЦИИ ---
        consecutive_empty_pages = 0
        start_page = current_page
        
        while True:
            pages_processed = current_page - start_page
            if max_pages and pages_processed >= max_pages:
                self._log(f"[-] Достигнут лимит страниц ({max_pages}).", ui_callback)
                break

            target_url = base_url if current_page == 1 else (url_template.replace('{page}', str(current_page)) if url_template else None)
            
            if not target_url:
                self._log("[-] Пагинации нет (шаблон null). Завершаем.", ui_callback)
                break

            self._log(f"\n[>] Скрапинг страницы {current_page}: {target_url}", ui_callback)
            
            try:
                response = await page.goto(target_url, timeout=60000, wait_until="domcontentloaded")
                
                # ОБХОД ЗАЩИТЫ (WAF)
                if response and response.status in [429, 403, 503]:
                    self._log(f"[🛑] Защита сайта: HTTP {response.status}. Охлаждаем поток на 60 сек...", ui_callback)
                    await asyncio.sleep(60)
                    continue
                    
                await self.browser.check_captcha_and_pause(page)
            except Exception as e:
                self._log(f"[!] Ошибка загрузки страницы: {e}", ui_callback)
                await asyncio.sleep(5)
                continue

            # --- SMART POLLING (Ожидание стабильности списка) ---
            found_elements = []
            for _ in range(20): # Ждем до 5 секунд (20 тактов по 250мс)
                elements = await page.locator(item_selector).all()
                if elements:
                    found_elements = elements
                    break
                await page.wait_for_timeout(250)
            
            if not found_elements:
                self._log("[-] На странице не найдено ссылок. Возможно, это конец списка.", ui_callback)
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= 2:
                    self._log("[+] Пагинация завершена (2 пустые страницы подряд).", ui_callback)
                    await asyncio.to_thread(self.db.update_category_progress, task_id, current_page, status='done')
                    break
                current_page += 1
                continue
                
            consecutive_empty_pages = 0
            
            # Извлекаем href
            extracted_urls = []
            for el in found_elements:
                href = await el.get_attribute('href')
                if href:
                    extracted_urls.append(urljoin(base_url, href))

            # Сохраняем в БД (теперь метод возвращает статистику дубликатов)
            added, skipped = await asyncio.to_thread(self.db.add_scraper_items, target_url, extracted_urls)
            self._log(f"[+] Собрано: {len(extracted_urls)} (Новых: {added}, Пропущено дубликатов: {skipped})", ui_callback)
            
            # Сохраняем прогресс текущей страницы
            current_page += 1
            await asyncio.to_thread(self.db.update_category_progress, task_id, current_page, url_template=url_template)
            
            # JITTER (Имитация человека, короткая пауза)
            await asyncio.sleep(random.uniform(0.2, 0.5))


# ==========================================
# БЛОК ИЗОЛИРОВАННОГО ТЕСТИРОВАНИЯ
# ==========================================
if __name__ == '__main__':
    print("[-] Запуск тестирования модуля Crawler...")
    db = DBManager()
    test_url = "https://quotes.toscrape.com/tag/life/"
    
    print(f"[*] Добавляю тестовый URL в базу: {test_url}")
    db.add_category_task(test_url)
    
    crawler = CategoryCrawler()
    crawler.run(max_pages_to_test=2, headless=False)