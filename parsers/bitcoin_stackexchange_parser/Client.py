import asyncio
import aiohttp
import config
import time
import random
from bitcoin_stackexchange_parser.Scraper import Scraper

class Client:
    def __init__(self):
        self._domain = config.DOMAIN
        self._questions_url = config.QUESTIONS_URL
        self._session = aiohttp.ClientSession(headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36'})
        self.url = f'{self._domain}{self._questions_url}'
        self.pages_url = []
        self.questions_url = []

    async def __aexit__(self, *error_details):
        await self._session.close()

    async def __aenter__(self):
        return self
    
    async def get_page_with_questions_html(self, page_id, print_msg=True):
        print(f'======== ПОЛУЧАЮ СТРАНИЦУ № {page_id}') if print_msg else None
        url = self.url.format(page_id=page_id)
        response = await self._session.get(url, ssl=False)
        
        if response.status not in [200, 202]:
            print(f'Ошибка при получении страницы {page_id} | Код ошибки: {response.status}')
            return None

        return (await response.text())

    async def get_questions_html(self, urls_part):
        questions_html = []

        for url_part in urls_part:
            await asyncio.sleep(random.uniform(0.5, 1))

            url = f'{self._domain}{url_part}'
            html = await self.get_question_html(url)
            questions_html.append((url, html)) if html else None
            
        return questions_html

    async def get_question_html(self, url):
        print(f'==== Получаю вопрос - {url}')
        response = await self._session.get(url, ssl=False)

        if response.status not in [200, 202]:
            print(f'Ошибка при получении страницы {url} | Код ошибки: {response.status}')
            return None
        
        return (await response.text())