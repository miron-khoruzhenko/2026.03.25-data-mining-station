from playwright.sync_api import sync_playwright, Page, BrowserContext
from pathlib import Path
import time
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

class BrowserManager:
    def __init__(self, state_dir: str = None):
        if state_dir is None:
            self.state_dir = PROJECT_ROOT / "data" / "browser_state"
        else:
            self.state_dir = Path(state_dir)
            
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.state_dir / "state.json"
        
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

    def start(self, headless: bool = False) -> Page:
        """Запускает браузер с восстановлением прошлой сессии (cookies, localStorage)."""
        self.playwright = sync_playwright().start()
        
        # Настройки для минимизации детекта
        args = [
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-infobars"
        ]
        
        self.browser = self.playwright.chromium.launch(headless=headless, args=args)

        # Загрузка состояния, если файл существует
        if self.state_file.exists():
            self.context = self.browser.new_context(
                storage_state=str(self.state_file),
                viewport={'width': 1920, 'height': 1080}
            )
        else:
            self.context = self.browser.new_context(
                viewport={'width': 1920, 'height': 1080}
            )

        # Подмена navigator.webdriver
        self.context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        self.page = self.context.new_page()
        return self.page

    def check_captcha_and_pause(self):
        """Проверяет страницу на наличие капчи. Замораживает скрипт при обнаружении."""
        captcha_selectors = [
            "#cf-please-wait",                # Cloudflare старый
            "#challenge-running",             # Cloudflare новый
            "iframe[src*='recaptcha']",       # Google reCAPTCHA
            "iframe[src*='hcaptcha']",        # hCaptcha
            ".g-recaptcha"                    # Контейнер reCAPTCHA
        ]
        
        for selector in captcha_selectors:
            if self.page.locator(selector).count() > 0:
                self.trigger_alert()
                print("\n[!] ОБНАРУЖЕНА ЗАЩИТА (КАПЧА)!")
                print("[!] Скрипт остановлен. Реши капчу в открытом окне браузера.")
                print("[!] После решения нажми кнопку 'Resume' (▶) в окне Playwright Inspector.")
                
                # Замораживает выполнение Python до нажатия Resume в инспекторе
                self.page.pause()
                
                print("[+] Скрипт разморожен, продолжаем работу...\n")
                break

    def trigger_alert(self):
        """Издает звуковой сигнал. \007 (BEL) работает в большинстве терминалов Linux/Mac."""
        sys.stdout.write('\a')
        sys.stdout.flush()

    def get_clean_html(self) -> str:
        """
        Удаляет из DOM визуальный мусор, стили и скрипты.
        Критически важно для экономии токенов Gemini при поиске селекторов.
        """
        self.page.evaluate('''() => {
            const selectors = [
                'script', 'style', 'svg', 'noscript', 'iframe', 
                'path', 'symbol', 'header', 'footer', 'nav'
            ];
            document.querySelectorAll(selectors.join(', ')).forEach(el => el.remove());
            
            // Удаление пустых тегов и комментариев
            const allElements = document.querySelectorAll('*');
            allElements.forEach(el => {
                if (el.innerHTML.trim() === '' && !el.hasAttributes()) {
                    el.remove();
                }
            });
        }''')
        
        # Получаем очищенный HTML. Ограничиваем длину, если страница аномально огромная
        html_content = self.page.content()
        return html_content

    def save_state(self):
        """Сохраняет куки и токены для следующих запусков."""
        if self.context:
            self.context.storage_state(path=str(self.state_file))

    def close(self):
        """Корректное закрытие и сохранение состояния."""
        self.save_state()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()