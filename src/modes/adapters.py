import json

class ELicitatieAdapter:
    """Плагин для перехвата API запросов на сайте e-licitatie.ro"""
    
    def __init__(self):
        self.api_data = {}

    async def intercept_response(self, response):
        """Слушатель трафика: ловит только нужные JSON пакеты."""
        # Ловим данные Офертанта (Поставщика)
        if "api-pub/Entity/getSUEntity" in response.url and response.status == 200:
            try: self.api_data['ofertant'] = await response.json()
            except: pass
            
        # Ловим данные Авторитета (Заказчика)
        elif "api-pub/Entity/getCAEntity" in response.url and response.status == 200:
            try: self.api_data['autoritate'] = await response.json()
            except: pass

    def extract_data(self) -> dict:
        """Переводит пойманный JSON в наш стандартный формат колонок."""
        result = {}
        
        # Маппинг для Офертанта
        su = self.api_data.get('ofertant', {})
        if su:
            result['ofertant_name'] = su.get('entityName')
            result['ofertant_cif'] = su.get('numericFiscalNumber')
            result['ofertant_email'] = su.get('email')
            result['ofertant_website'] = su.get('url')
            # Можно добавить телефон, адрес и т.д., если нужно

        # Маппинг для Авторитета
        ca = self.api_data.get('autoritate', {})
        if ca:
            result['autoritate_name'] = ca.get('entityName')
            result['autoritate_cif'] = ca.get('numericFiscalNumber')
            result['autoritate_email'] = ca.get('email')
            result['autoritate_website'] = ca.get('url')

        return result

# Реестр всех плагинов. Ключ - домен сайта.
ADAPTERS_REGISTRY = {
    "e-licitatie.ro": ELicitatieAdapter,
    "istoric.e-licitatie.ro": ELicitatieAdapter
}