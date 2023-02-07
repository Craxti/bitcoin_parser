import asyncio
import aiohttp
import config
from utils import save_last_post, get_last_post, find_bitcoin_address
from errors import AuthError
from log import log

class Client:
    AUTH_URL = f'{config.DOMAIN}{config.URL_AUTH_PART}'
    SUBREDDIT_URL = f'{config.DOMAIN_API}{config.URL_SUBREDDIT_PART}'.format(id='bitcoin')
    PREFIX = 't5_'

    def __init__(self):
        self._headers = config.HEADERS
        self._session = aiohttp.ClientSession(headers=config.HEADERS)
        self.is_auth = False
        self.is_ended = False

    async def __aexit__(self, *error_details):
        await self._session.close()

    async def __aenter__(self):
        return self
    
    async def auth(self):
        '''Авторизируем JWT токен на reddit'''
        log.info('Авторизация парсера...')

        self.is_auth = False
        auth = aiohttp.BasicAuth(config.API_ID, config.API_KEY)
        data = {'grant_type': 'password', 'username': config.API_USERNAME, 'password': config.API_PASSWORD}
        response = await self._session.post(Client.AUTH_URL, headers=self._headers, auth=auth, data=data)

        if response.status == 200:
            log.info('Авторизация прошла успешно!')

            access_token = (await response.json())['access_token']
            self._headers['Authorization'] = f'bearer {access_token}'
            self.is_auth = True

            return True
        
        raise AuthError()

    async def make_request(self, url):
        if not self.is_auth:
            await self.auth() # Авторизовываем парсер

        await asyncio.sleep(2)

        response = await self._session.get(url, headers=self._headers)

        if response.status != 200:
            if response.status == 403:
                # Необходима обновление токена
                await self.auth()
                response = await self._session.get(url, headers=self._headers)
            else:
                html = (await response.text())[:25]
                log.error(f'Неизвестный код ответа при получении постов: {response.status} | {html} | {url}')
                return

        return (await response.json())['data']

    async def get_posts(self, limit=config.LIMIT):
        try:
            posts = []
            last_id, count = get_last_post()

            url = f'{Client.SUBREDDIT_URL}{config.URL_SUBREDDIT_PARAMS}'.format(limit=limit, last_id=last_id)
            data = await self.make_request(url)
            
            save_last_post(data['after'], config.LIMIT + count) # Сохраняем последний id страницы для пагинации

            if not data['children']:
                # Элементы закончились
                self.is_ended = True

            for children in data['children']:
                try:
                    url = children['data']['url']
                    topic = children['data']['title']
                    text = children['data']['selftext']
                    name = children['data']['author']
                    asked_at = children['data']['created']
                    btc_addresses = find_bitcoin_address(text)

                    if btc_addresses:
                        posts.append({
                            'url': url,
                            'text': text[:60],
                            'topic': topic,
                            'name': name,
                            'btc_addresses': btc_addresses,
                            'asked_at': asked_at
                        })

                except Exception as ex:
                    log.error(f'Ошибка при извлечении данных {ex} {url}')

            return posts
        
        except AuthError:
            raise AuthError

        except Exception as ex:
            log.error(f'Ошибка при получении поста {ex}')