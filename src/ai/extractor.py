import os
import json
import google.generativeai as genai
from dotenv import load_dotenv

# Загружаем переменные из .env
load_dotenv()

class AIExtractor:
    def __init__(self):
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("[!] Ошибка: GEMINI_API_KEY не найден в файле .env")
        
        genai.configure(api_key=api_key)
        
        # Используем Flash для скорости и JSON-режим для жесткого форматирования
        self.model = genai.GenerativeModel(
            'gemini-2.0-flash',
            generation_config={"response_mime_type": "application/json"}
        )

    def get_pagination_config(self, html_content: str, base_url: str) -> dict:
        """
        Анализирует HTML и возвращает шаблон пагинации и селектор для карточек.
        """
        prompt = f"""
        Ты - эксперт по веб-скрапингу. Проанализируй очищенный HTML-код страницы.
        Базовый URL: {base_url}
        
        Твоя задача:
        1. Найти паттерн пагинации.
        2. Найти CSS-селектор ссылок, ведущих на детальные карточки компаний/товаров.
        
        Верни строго JSON со следующими ключами:
        - "url_template": Строка. Шаблон для следующих страниц, где номер заменен на '{{page}}'. 
          Например, если вторая страница '?page=2', шаблон будет '{base_url}?page={{page}}'. 
          Если пагинация формата '/category/page/2', шаблон будет 'https://site.com/category/page/{{page}}'.
          Если пагинации нет, верни null.
        - "item_selector": Точный CSS-селектор (например, 'div.company-card > h2 > a') для ссылок на детальные страницы.
        
        HTML (обрезан для экономии):
        {html_content[:30000]} 
        """
        response = self.model.generate_content(prompt)
        return json.loads(response.text)

    def get_data_selectors(self, html_content: str, custom_fields: list = None, use_only_custom: bool = False) -> dict:
        """
        Генерирует локаторы (XPath или CSS) для извлечения данных из карточки.
        """
        # Если стоит галочка "Только кастомные", не добавляем стандартные поля
        fields = [] if use_only_custom else ['company_name', 'company_phone', 'company_email', 'company_website', 'company_fax']
        
        if custom_fields:
            fields.extend(custom_fields)
            
        prompt = f"""
        Ты - senior data engineer. Твоя задача — написать локаторы для извлечения данных из HTML для Playwright.
        
        КРИТИЧЕСКОЕ ПРАВИЛО ДЛЯ XPATH: На странице есть блоки с одинаковой структурой (например, данные Поставщика и данные Заказчика).
        Ты ОБЯЗАН использовать XPath со строгой привязкой к родительскому блоку! Иначе парсер перепутает данные.
        
        Правильный подход: Сначала найди уникальный родительский контейнер по тексту заголовка (например, "DATE IDENTIFICARE AUTORITATE CONTRACTANTA" или "Ofertant:"), и только внутри него ищи нужное поле.
        
        ПРИМЕР ПРАВИЛЬНОГО XPATH: //div[contains(., 'AUTORITATE CONTRACTANTA')]//span[contains(text(), 'CIF:')]/following-sibling::span
        ПРИМЕР ПЛОХОГО XPATH (найдет первый попавшийся на странице и приведет к ошибке): //span[contains(text(), 'CIF:')]/following-sibling::span
        
        Если данные отсутствуют (например, вместо факса стоит символ "-" или пустая строка), XPath должен указывать именно на этот элемент, это нормально.
        
        Список полей для поиска: {', '.join(fields)}
        
        Верни строго JSON, где ключи — названия полей, а значения — сгенерированные XPath (начинаются с //). 
        Если данных для поля нет в HTML, значение должно быть null.
        
        HTML:
        {html_content[:30000]}
        """
        response = self.model.generate_content(prompt)
        return json.loads(response.text)


# ==========================================
# БЛОК ИЗОЛИРОВАННОГО ТЕСТИРОВАНИЯ
# ==========================================
if __name__ == '__main__':
    print("[-] Запуск тестирования модуля AIExtractor...")
    try:
        extractor = AIExtractor()
        
        # Имитируем кусок HTML со списком компаний и пагинацией
        test_html_list = """
        <html><body>
            <div class="list">
                <div class="item"><a href="/company/123" class="details-btn">Узнать больше</a></div>
            </div>
            <div class="pagination">
                <a href="https://example.com/category?page=2">Вперед</a>
            </div>
        </body></html>
        """
        
        print("\n[1] Тест пагинации:")
        result_pagination = extractor.get_pagination_config(test_html_list, "https://example.com/category")
        print(json.dumps(result_pagination, indent=2, ensure_ascii=False))
        
        # Имитируем кусок HTML с деталями компании
        test_html_details = """
        <html><body>
            <h1 class="comp-title">ООО Вектор</h1>
            <div class="contacts">
                <a href="tel:+1234567" data-role="phone">+1 234 567</a>
                <span class="email">info@vector.com</span>
            </div>
            <div class="extra">
                <span class="director">Иванов И.И.</span>
            </div>
        </body></html>
        """
        
        print("\n[2] Тест селекторов данных (с добавлением кастомного поля):")
        result_selectors = extractor.get_data_selectors(test_html_details, custom_fields=['director_name'])
        print(json.dumps(result_selectors, indent=2, ensure_ascii=False))
        
        print("\n[+] Тестирование успешно завершено.")
        
    except Exception as e:
        print(f"\n[!] Ошибка при тестировании: {e}")