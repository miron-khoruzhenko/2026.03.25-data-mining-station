import sys
import os
import json
import time
from urllib.parse import urlparse

# Подключение модулей из корня проекта
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.db.db_manager import DBManager
from src.core.browser import BrowserManager
from src.ai.extractor import AIExtractor

class DataScraper:
    def __init__(self):
        self.db = DBManager()
        self.browser = BrowserManager()
        self.ai = AIExtractor()
        
        # Сброс зависших задач при рестарте (защита от крашей)
        self.db.reset_processing_items()

    def _log(self, msg: str, ui_callback=None):
        print(msg)
        if ui_callback:
            ui_callback(msg)

    def run(self, custom_fields: list = None, max_items_to_test: int = None, use_only_custom: bool = False, headless: bool = True, ui_callback=None):
        self._log("[*] Запуск экстрактора (Режим 2).", ui_callback)
        self.browser.start(headless=headless)
        items_processed = 0

        try:
            while True:
                if max_items_to_test and items_processed >= max_items_to_test:
                    self._log(f"[-] Достигнут лимит тестирования ({max_items_to_test} шт.). Завершение.", ui_callback)
                    break

                item = self.db.get_pending_item()
                if not item:
                    self._log("[-] Нет ссылок в очереди (status = 'pending'). Завершение работы.", ui_callback)
                    break

                item_id = item['id']
                target_url = item['data_url']
                domain = urlparse(target_url).netloc
                
                self._log(f"\n[>] Обработка [{items_processed + 1}]: {target_url}", ui_callback)
                
                try:
                    self.browser.page.goto(target_url, timeout=60000)
                    self.browser.check_captcha_and_pause()
                    # Возвращаем надежную базовую паузу
                    self.browser.page.wait_for_timeout(2000) 
                except Exception as e:
                    self._log(f"[!] Ошибка загрузки страницы карточки: {e}", ui_callback)
                    self.db.update_item_status(item_id, status='error')
                    continue

                selectors = self._get_selectors_for_domain(domain, custom_fields, use_only_custom, ui_callback)
                
                if not selectors:
                    self._log(f"[!] Не удалось получить селекторы для домена {domain}.", ui_callback)
                    self.db.update_item_status(item_id, status='error')
                    continue

                # 1. Первичное извлечение данных
                extracted_data = self._extract_data_from_page(selectors)
                
                # Локальная функция для проверки: пусты ли все собранные поля?
                def is_data_empty(data):
                    return all(v is None or str(v).strip() == "" for v in data.values())

                # 2. Если данные пустые, делаем Retry (повторную попытку)
                if is_data_empty(extracted_data):
                    self._log("[~] Данные не найдены (возможно, не успел прогрузиться JS). Делаю повторную попытку...", ui_callback)
                    
                    try:
                        self.browser.page.reload()
                        self.browser.page.wait_for_timeout(4000) # Ждем чуть дольше на втором шансе
                        extracted_data = self._extract_data_from_page(selectors)
                    except Exception:
                        pass # Если релоад упал, просто пойдем дальше

                    # 3. Если данные ВСЁ ЕЩЕ пустые, помечаем строку как empty и пропускаем
                    if is_data_empty(extracted_data):
                        self._log("[-] Страница пустая (селекторы ничего не нашли). Пропускаю.", ui_callback)
                        # Статус 'empty' гарантирует, что Экспортер проигнорирует эту строку
                        self.db.update_item_status(item_id, status='empty')
                        items_processed += 1
                        continue

                # Если мы дошли сюда, значит данные есть. Добавляем системные поля.
                extracted_data['data_url'] = target_url
                extracted_data['source_url'] = item['source_url']

                self._log(f"[+] Данные собраны: {json.dumps(extracted_data, ensure_ascii=False)}", ui_callback)
                
                # Сохраняем как успешно собранные
                self.db.update_item_status(item_id, status='done', extracted_data=extracted_data)
                items_processed += 1
                
                time.sleep(1)

        finally:
            self.browser.close()
            
    def _get_selectors_for_domain(self, domain: str, custom_fields: list, use_only_custom: bool, ui_callback=None) -> dict:
        """
        Ищет селекторы в базе. Если их нет, генерирует через AI и кэширует.
        """
        with self.db.get_connection() as conn:
            row = conn.execute("SELECT selectors_json FROM ai_configs WHERE domain = ?", (domain,)).fetchone()
            
            if row:
                print("[~] Селекторы найдены в кэше БД.")
                return json.loads(row['selectors_json'])
            
            print("[~] Селекторы для домена не найдены. Запрашиваю анализ у AI...")
            clean_html = self.browser.get_clean_html()
            
            # Запрос к Gemini
            selectors = self.ai.get_data_selectors(clean_html, custom_fields=custom_fields, use_only_custom=use_only_custom)
            
            # Сохраняем в кэш
            conn.execute(
                "INSERT INTO ai_configs (domain, selectors_json) VALUES (?, ?)",
                (domain, json.dumps(selectors))
            )
            conn.commit()
            
            print("[+] Новые селекторы сгенерированы и сохранены.")
            return selectors

    def _extract_data_from_page(self, selectors: dict) -> dict:
        """
        Применяет локаторы к текущей странице через Playwright.
        """
        result = {}
        for field_name, selector in selectors.items():
            if not selector: 
                result[field_name] = None
                continue
                
            try:
                locator = self.browser.page.locator(selector).first
                if locator.count() > 0:
                    # Универсальная проверка: ищем ключевые слова в названии любого кастомного поля
                    is_link_field = any(kw in field_name.lower() for kw in ['website', 'email', 'url', 'link'])
                    
                    # Проверяем, является ли найденный элемент тегом <a>
                    is_a_tag = locator.evaluate("el => el.tagName.toLowerCase() === 'a'")
                    
                    if is_link_field and is_a_tag:
                        href = locator.get_attribute('href')
                        if href and href.startswith('mailto:'):
                            result[field_name] = href.replace('mailto:', '').strip()
                        else:
                            # Берем href, но если он пустой (бывает и такое), берем внутренний текст
                            result[field_name] = href.strip() if href else locator.inner_text().strip()
                    else:
                        result[field_name] = locator.inner_text().strip()
                else:
                    result[field_name] = None
            except Exception as e:
                # print(f"[!] Ошибка извлечения {field_name}: {e}") # Можно раскомментировать для дебага
                result[field_name] = None
                
        return result


# ==========================================
# БЛОК ИЗОЛИРОВАННОГО ТЕСТИРОВАНИЯ
# ==========================================
if __name__ == '__main__':
    print("[-] Запуск тестирования модуля Scraper...")
    
    # 1. Сначала добавим фейковую задачу в БД, чтобы было что парсить
    db = DBManager()
    test_target_url = "https://quotes.toscrape.com/author/Albert-Einstein/"
    test_source_url = "https://quotes.toscrape.com/"
    
    print(f"[*] Добавляю тестовую карточку (детали автора) в базу: {test_target_url}")
    db.add_scraper_items(source_url=test_source_url, data_urls=[test_target_url])
    
    # 2. Запускаем экстрактор (только 1 элемент)
    # Запросим кастомные поля, специфичные для этого сайта-песочницы
    custom_fields_to_find = ['author_born_date', 'author_born_location']
    
    scraper = DataScraper()
    scraper.run(custom_fields=custom_fields_to_find, max_items_to_test=1)
    
    # 3. Проверяем результаты
    print("\n[!] Проверка статуса в базе:")
    with db.get_connection() as conn:
        item = conn.execute("SELECT * FROM scraper_items WHERE data_url = ?", (test_target_url,)).fetchone()
        if item:
            print(f"Статус: {item['status']}")
            print(f"Собранные данные (JSON): \n{item['extracted_data']}")