import sys, os, json, time, asyncio, random
from urllib.parse import urlparse
try: from streamlit.runtime.scriptrunner import StopException
except ImportError: pass
from src.modes.adapters import ADAPTERS_REGISTRY

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.db.db_manager import DBManager
from src.core.browser import BrowserManager
from src.ai.extractor import AIExtractor

class DataScraper:
    def __init__(self):
        self.db = DBManager()
        self.browser = BrowserManager()
        self.ai = AIExtractor()
        self.db.reset_processing_items()
        self.items_processed, self.max_items, self.start_time = 0, 0, 0

    def _log(self, msg, ui_callback=None, stats=None):
        print(msg)
        if ui_callback: ui_callback(msg, stats)

    def run(self, custom_fields=None, max_items_to_test=None, use_only_custom=False, headless=True, ui_callback=None):
        try: asyncio.run(self._async_run(custom_fields, max_items_to_test, use_only_custom, headless, ui_callback))
        except StopException:
            self._log("\n[🛑] Процесс прерван.", ui_callback); raise
        except Exception as e: self._log(f"\n[!] Ошибка: {e}", ui_callback)

    async def _async_run(self, custom_fields, max_items, use_only_custom, headless, ui_callback):
        self._log("[*] Запуск экстрактора (Многопоточный Режим).", ui_callback)
        await self.browser.start(headless=headless)
        self.start_time, self.max_items = time.time(), max_items
        
        # Запускаем пул из 5 независимых воркеров (вкладок)
        # await asyncio.gather(*[self._worker(i, custom_fields, use_only_custom, ui_callback) for i in range(5)])
        await asyncio.gather(*[self._worker(i, custom_fields, use_only_custom, ui_callback) for i in range(3)])
        
        self._log("[~] Закрытие браузера...", ui_callback)
        await self.browser.close()

    async def _worker(self, w_id, custom_fields, use_only_custom, ui_callback):
        page = await self.browser.new_page()
        while True:
            # --- 0. ПРОВЕРКА ЛИМИТОВ И ОЧЕРЕДИ ---
            if self.max_items and self.items_processed >= self.max_items: break
            
            item = await asyncio.to_thread(self.db.get_pending_item)
            if not item: break

            i_id, target = item['id'], item['data_url']
            domain = urlparse(target).netloc
            self._log(f"[W-{w_id}] Сбор: {target}", ui_callback)

            # --- 1. ПРОВЕРКА ПЛАГИНА (АДАПТЕРА) НА ПЕРЕХВАТ API ---
            adapter = None
            if domain in ADAPTERS_REGISTRY:
                adapter = ADAPTERS_REGISTRY[domain]()
                # Вешаем слушатель сетевого трафика на эту вкладку
                page.on("response", adapter.intercept_response)

            # --- 2. ЗАГРУЗКА СТРАНИЦЫ И ОБХОД ЗАЩИТЫ ---
            try:
                response = await page.goto(target, timeout=60000, wait_until="domcontentloaded")
                
                if response and response.status in [429, 403, 503]:
                    self._log(f"[W-{w_id}][🛑] Защита сайта: HTTP {response.status}. Охлаждаем поток на 60 сек...", ui_callback)
                    await asyncio.to_thread(self.db.update_item_status, i_id, 'pending')
                    await asyncio.sleep(60)
                    if adapter: page.remove_listener("response", adapter.intercept_response)
                    continue
                    
                await self.browser.check_captcha_and_pause(page)
            except Exception as e:
                self._log(f"[W-{w_id}][!] Ошибка: {e}", ui_callback)
                await asyncio.to_thread(self.db.update_item_status, i_id, 'error')
                if adapter: page.remove_listener("response", adapter.intercept_response)
                continue

            ext_data = {}
            is_empty = True

            # --- 3. ПОПЫТКА ИЗВЛЕЧЕНИЯ ДАННЫХ ЧЕРЕЗ API (МГНОВЕННО) ---
            if adapter:
                # Умный микро-опрос: проверяем адаптер каждые 150мс (макс 15 итераций = ~2.2 сек)
                for _ in range(15):
                    ext_data = adapter.extract_data()
                    if ext_data and any(ext_data.values()):
                        is_empty = False
                        self._log(f"[W-{w_id}][⚡] Данные перехвачены через API плагин!", ui_callback)
                        break # Выходим мгновенно, как только данные получены
                    await page.wait_for_timeout(150)
                
                # Снимаем слушатель, чтобы не засорять память
                page.remove_listener("response", adapter.intercept_response)

            # --- 4. ФОЛЛБЕК НА SMART POLLING 3.0 (ЕСЛИ ПЛАГИН НЕ ПОМОГ) ---
            if is_empty:
                selectors = await self._get_selectors(domain, custom_fields, use_only_custom, page)
                if not selectors: 
                    await asyncio.to_thread(self.db.update_item_status, i_id, 'error')
                    continue

                prev_filled = -1
                stable_ticks = 0
                
                for _ in range(30):
                    ext_data = await self._extract(page, selectors)
                    filled_count = sum(1 for v in ext_data.values() if v and str(v).strip() not in ("", "-"))
                    
                    if filled_count == len(selectors): break 
                    if filled_count > 0 and filled_count == prev_filled:
                        stable_ticks += 1
                        if stable_ticks >= 6: break
                    else: stable_ticks = 0
                        
                    prev_filled = filled_count
                    await page.wait_for_timeout(250)

                is_empty = prev_filled == 0

            # --- 5. ФИНАЛЬНОЕ СОХРАНЕНИЕ В БАЗУ ---
            if is_empty:
                self._log(f"[W-{w_id}][-] Пусто. Скип.", ui_callback)
                await asyncio.to_thread(self.db.update_item_status, i_id, 'empty')
            else:
                ext_data.update({'data_url': target, 'source_url': item['source_url']})
                await asyncio.to_thread(self.db.update_item_status, i_id, 'done', ext_data)
                
            # --- 6. ОБНОВЛЕНИЕ ТАЙМЕРОВ И JITTER ---
            self.items_processed += 1
            el = int(time.time() - self.start_time)
            avg = el / self.items_processed if self.items_processed > 0 else 0
            
            # Правильное безопасное обращение к БД через менеджер контекста
            def _get_pending_count():
                with self.db.get_connection() as conn:
                    return conn.execute("SELECT COUNT(*) FROM scraper_items WHERE status = 'pending'").fetchone()[0]
                    
            pending_count = await asyncio.to_thread(_get_pending_count)
            
            remaining_items = min(pending_count, self.max_items - self.items_processed) if self.max_items else pending_count
            self._log(f"[W-{w_id}][+] Готово", ui_callback, {"elapsed": el, "eta": int(avg * remaining_items)})
            
            # Случайная пауза перед следующей ссылкой для имитации человека
            # await asyncio.sleep(random.uniform(2.0, 4.5))
            # Минимальная пауза (0.2 - 0.5 сек) вместо долгих 4 секунд
            await asyncio.sleep(random.uniform(0.2, 0.5))

    # --- ИСПРАВЛЕННАЯ ЛОГИКА РАБОТЫ С КЭШЕМ ---
    def _get_cached_selectors(self, domain):
        """Безопасное извлечение селекторов из БД."""
        with self.db.get_connection() as conn:
            row = conn.execute("SELECT selectors_json FROM ai_configs WHERE domain = ?", (domain,)).fetchone()
            return json.loads(row['selectors_json']) if row else None

    async def _get_selectors(self, domain, c_fields, only_custom, page):
        selectors = await asyncio.to_thread(self._get_cached_selectors, domain)
        if selectors: return selectors
        
        selectors = await asyncio.to_thread(self.ai.get_data_selectors, await self.browser.get_clean_html(page), c_fields, only_custom)
        if selectors: await asyncio.to_thread(self.db.update_ai_config, domain, selectors)
        return selectors

    async def _extract(self, page, selectors):
        res = {}
        for f, sel in selectors.items():
            if not sel: res[f] = None; continue
            try:
                loc = page.locator(sel).first
                if await loc.count() > 0:
                    if any(kw in f.lower() for kw in ['website', 'email', 'url', 'link']) and await loc.evaluate("el => el.tagName.toLowerCase() === 'a'"):
                        href = await loc.get_attribute('href')
                        res[f] = href.replace('mailto:', '').strip() if href and href.startswith('mailto:') else (href.strip() if href else (await loc.inner_text()).strip())
                    else: res[f] = (await loc.inner_text()).strip()
                else: res[f] = None
            except: res[f] = None
        return res