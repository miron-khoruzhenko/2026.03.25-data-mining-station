from playwright.async_api import async_playwright
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

class BrowserManager:
    def __init__(self, state_dir=None):
        self.state_dir = Path(state_dir) if state_dir else PROJECT_ROOT / "data" / "browser_state"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.state_dir / "state.json"
        self.playwright = self.browser = self.context = None

    async def start(self, headless=False):
        self.playwright = await async_playwright().start()
        
        # Специальные флаги для выживания на слабых VPS и в Docker
        args = [
            "--disable-blink-features=AutomationControlled", 
            "--no-sandbox", 
            "--disable-infobars",
            "--disable-dev-shm-usage", # КРИТИЧНО для Docker: отключает лимит /dev/shm, предотвращая краши RAM
            "--disable-gpu",           # Выключает аппаратное ускорение
            "--disable-software-rasterizer" # Выключает программную отрисовку (экономит CPU)
        ]
        
        self.browser = await self.playwright.chromium.launch(headless=headless, args=args)
        self.context = await self.browser.new_context(
            storage_state=str(self.state_file) if self.state_file.exists() else None, 
            viewport={'width': 1920, 'height': 1080}
        )
        await self.context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
    async def new_page(self):
        return await self.context.new_page()

    async def check_captcha_and_pause(self, page):
        for s in ["#cf-please-wait", "#challenge-running", "iframe[src*='recaptcha']", "iframe[src*='hcaptcha']", ".g-recaptcha"]:
            if await page.locator(s).count() > 0:
                sys.stdout.write('\a'); sys.stdout.flush()
                await page.pause(); break

    async def get_clean_html(self, page):
        await page.evaluate("() => { const sel = ['script', 'style', 'svg', 'noscript', 'iframe', 'path', 'symbol', 'header', 'footer', 'nav']; document.querySelectorAll(sel.join(', ')).forEach(el => el.remove()); document.querySelectorAll('*').forEach(el => { if(el.innerHTML.trim() === '' && !el.hasAttributes()) el.remove(); }); }")
        return await page.content()

    async def close(self):
        if self.context: await self.context.storage_state(path=str(self.state_file))
        if self.browser: await self.browser.close()
        if self.playwright: await self.playwright.stop()