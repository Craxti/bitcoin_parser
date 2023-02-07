import asyncio
import aiohttp
import config

class Client:
    PAGES_COUNT = config.PAGES_COUNT

    def __init__(self):
        self._session = aiohttp.ClientSession(headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A'})
        self.url = f'{config.DOMAIN}{config.URL_PART}'
        self.get_pages_count = 0

    async def __aexit__(self, *error_details):
        await self._session.close()

    async def __aenter__(self):
        return self
    
    async def page_request(self, page_id):
        response = await self._session.get(self.url.format(id=page_id), ssl=False)
        self.get_pages_count += 1
        if response.status not in [200, 202]:
            print(f'Ошибка при получении страницы {page_id} | Код ошибки: {response.status}')
        
        return response        

    def get_requests_tasks(self):
        tasks = []
        for page_id in range(1, Client.PAGES_COUNT + 1):
            tasks.append(asyncio.create_task(self.page_request(page_id)))

        return tasks

    async def get_page_contents(self):
        responses = await asyncio.gather(*self.get_requests_tasks())
        return [await response.text() for response in responses]