import sys
import os
import time
from urllib.parse import urljoin

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
        
    def run(self, max_pages_to_test=None, headless=True):
        """
        Основной цикл. Берет категории из базы и обрабатывает их.
        max_pages_to_test - ограничение страниц для тестирования.
        """
        task = self.db.get_pending_category()
        if not task:
            print("[-] Нет категорий в очереди (crawler_tasks).")
            return

        print(f"[*] Запуск краулера для категории: {task['category_url']}")
        page = self.browser.start(headless=headless) # Открываем браузер

        try:
            self._process_category(task, max_pages_to_test)
        finally:
            self.browser.close()

    def _process_category(self, task, max_pages_to_test):
        task_id = task['id']
        base_url = task['category_url']
        current_page = task['current_page']
        url_template = task['url_template']
        
        # Шаг 1: Если шаблона нет, получаем его через AI на 1-й странице
        if not url_template:
            print("[~] Шаблон пагинации не найден. Запрашиваю у AI...")
            self.browser.page.goto(base_url, timeout=60000)
            self.browser.check_captcha_and_pause()
            
            clean_html = self.browser.get_clean_html()
            ai_config = self.ai.get_pagination_config(clean_html, base_url)
            
            url_template = ai_config.get('url_template')
            item_selector = ai_config.get('item_selector')
            
            if not item_selector:
                print("[!] AI не смог найти селектор карточек. Пропуск.")
                self.db.update_category_progress(task_id, current_page, status='error')
                return
                
            print(f"[+] AI вернул шаблон: {url_template}")
            print(f"[+] AI вернул селектор карточек: {item_selector}")
            
            # Сохраняем шаблон в базу, чтобы не дергать AI при крашах
            self.db.update_category_progress(task_id, current_page, url_template=url_template)
        else:
            # Если шаблон уже есть, нам нужен только селектор карточек (можно кэшировать, но для надежности запросим снова или захардкодим для теста)
            # В рабочей версии селекторы лучше хранить в таблице ai_configs. Здесь для упрощения запросим у AI один раз.
            print("[~] Восстановление селектора...")
            self.browser.page.goto(base_url)
            clean_html = self.browser.get_clean_html()
            item_selector = self.ai.get_pagination_config(clean_html, base_url).get('item_selector')

        # Шаг 2: Цикл пагинации
        consecutive_empty_pages = 0
        
        while True:
            if max_pages_to_test and current_page > max_pages_to_test:
                print(f"[-] Достигнут лимит страниц для теста ({max_pages_to_test}).")
                break

            # Формируем URL текущей страницы
            if current_page == 1:
                target_url = base_url
            else:
                if url_template:
                    # Заменяем {page} на номер. Если AI вернул кривой шаблон без {page}, пытаемся приклеить
                    target_url = url_template.replace('{page}', str(current_page))
                else:
                    print("[-] Пагинации нет (шаблон null). Завершаем.")
                    break

            print(f"\n[>] Скрапинг страницы {current_page}: {target_url}")
            try:
                self.browser.page.goto(target_url, timeout=60000)
                self.browser.check_captcha_and_pause()
                
                # Даем время на подгрузку динамики (No-JS/JS)
                self.browser.page.wait_for_timeout(2000) 
            except Exception as e:
                print(f"[!] Ошибка загрузки страницы: {e}")
                time.sleep(5)
                continue

            # Ищем элементы по селектору от AI
            elements = self.browser.page.locator(item_selector).all()
            
            if not elements:
                print("[-] На странице не найдено ссылок. Возможно, это конец списка.")
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= 2:
                    print("[+] Пагинация завершена.")
                    self.db.update_category_progress(task_id, current_page, status='done')
                    break
                current_page += 1
                continue
                
            consecutive_empty_pages = 0
            
            # Извлекаем href и делаем абсолютные ссылки (добавляем домен, если ссылка вида /item/123)
            extracted_urls = []
            for el in elements:
                href = el.get_attribute('href')
                if href:
                    absolute_url = urljoin(base_url, href)
                    extracted_urls.append(absolute_url)

            # Сохраняем ссылки в базу (Очередь Режима 2). Дубликаты игнорируются автоматически.
            self.db.add_scraper_items(source_url=target_url, data_urls=extracted_urls)
            print(f"[+] Собрано ссылок: {len(extracted_urls)}")
            
            # Сохраняем прогресс (State Management)
            current_page += 1
            self.db.update_category_progress(task_id, current_page, url_template=url_template)
            
            time.sleep(1) # Небольшая пауза между страницами


# ==========================================
# БЛОК ИЗОЛИРОВАННОГО ТЕСТИРОВАНИЯ
# ==========================================
if __name__ == '__main__':
    print("[-] Запуск тестирования модуля Crawler...")
    
    # 1. Создаем тестовую задачу в БД.
    # Используем quotes.toscrape.com - легальный сайт-песочница для парсеров.
    db = DBManager()
    test_url = "https://quotes.toscrape.com/tag/life/"
    
    print(f"[*] Добавляю тестовый URL в базу: {test_url}")
    db.add_category_task(test_url)
    
    # 2. Запускаем краулер (ограничиваем 2 страницами, чтобы тест не шел долго)
    crawler = CategoryCrawler()
    crawler.run(max_pages_to_test=2)
    
    # 3. Проверяем результаты
    print("\n[!] Проверка результатов в базе (scraper_items):")
    with db.get_connection() as conn:
        items = conn.execute("SELECT data_url FROM scraper_items LIMIT 5").fetchall()
        for item in items:
            print(f"  - {item['data_url']}")
        
        count = conn.execute("SELECT COUNT(*) as c FROM scraper_items").fetchone()['c']
        print(f"\n[+] Всего ссылок сохранено в БД (уникальных): {count}")