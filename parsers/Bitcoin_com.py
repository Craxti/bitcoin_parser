import time
from aiohttp_socks import ProxyConnector, ProxyType
from bs4 import BeautifulSoup
import aiohttp
import asyncio
from aiohttp import ClientTimeout
from datetime import datetime
import pymongo


import settings


class Bitcoin_com:
    def __init__(self):
        self.timeout = ClientTimeout(total=0)
        self.pages = []
        self.sub_pages = []
        self.sub_pages_without_pagination = []
        self.sub_pages_topic = []
        self.sub_pages_topic_without_pagination = []
        self.pages_without_pagination = []
        self.link_list_without_topic = []
        self.res_link_list = []
        self._data = {}
        self.start_time = time.time()

    def write_database(self, data):
        '''инициализация базы и запись в нее'''
        db_client = pymongo.MongoClient('localhost', 27017)
        current_db = db_client[settings.BITCOIN_COM_BASE]
        res_list = []
        collection = current_db[settings.BITCOIN_COM_USER_BTC]
        for item in data:
            res_list.append({'username': item, 'btc_address': data[item], 'timestamp': datetime.now()})
        res = collection.insert_many(res_list)
        print(res)

    def run(self):
        asyncio.run(self.gather_data())
        self.write_database()
        print(time.time() - self.start_time)
  
    async def gather_data(self):
        # connector = ProxyConnector(
        #     proxy_type=ProxyType.HTTP,
        #     host='213.232.71.124',
        #     port=8000,
        #     username='ocMABw',
        #     password='rkBJQQ',
        #     rdns=True
        # )
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            link_main = 'https://forum.bitcoin.com/'
            site_html = await session.get(link_main)
            soup = BeautifulSoup(await site_html.text(), "lxml")
            a_forum_titles = soup.find_all('a', class_='forumtitle')
            tasks = []
            for link in a_forum_titles:
                task = asyncio.create_task(self.get_page_data(session, link.get('href')))
                tasks.append(task)
            await asyncio.gather(*tasks)

            await asyncio.sleep(2)

            pages_list = self.create_links(self.pages)
            sub_pages_list = self.create_links(self.sub_pages)
            links = pages_list + sub_pages_list + self.pages + self.pages_without_pagination + self.sub_pages + self.sub_pages_without_pagination
            for link in links:
                task = asyncio.create_task(self.get_page_data_inner(session, link))
                tasks.append(task)
            await asyncio.gather(*tasks)
            await asyncio.sleep(2)
            for link in self.link_list_without_topic:
                task = asyncio.create_task(self.get_data_inner_topic(session, link))
                tasks.append(task)
            await asyncio.gather(*tasks)
            await asyncio.sleep(2)
            sub_page_topic_list = self.create_links_topic(self.sub_pages_topic) + self.sub_pages_without_pagination
            for link in sub_page_topic_list:
                task = asyncio.create_task(self.get_data_inner_topic(session, link))
                tasks.append(task)
            await asyncio.gather(*tasks)
            await asyncio.sleep(settings.BITCOIN_COM_SLEEP)

    def create_links_topic(self, pages):
        links = []
        for page in pages:
            page_split = page.split("-")
            number = page_split[-1]
            number_of_page = ''
            for c in number:
                if c.isdigit():
                    number_of_page += c
            for i in range(30, int(number_of_page), 30):
                links.append("-".join(page_split[:-1]) + f'-{i}.html')
        return links

    async def get_data_inner_topic(self, session, link):
        async with session.get(url=link) as response:
            response_text = await response.text()
            soup = BeautifulSoup(response_text, 'lxml')
            try:
                action_bar = soup.find('div', class_='action-bar')
                pagination = action_bar.find('div', class_='pagination')
                li_list = pagination.find_all('li')[-2]
                last_page = li_list.a.get('href')
                self.sub_pages_topic.append(last_page)
            except Exception as ex:
                self.sub_pages_topic_without_pagination.append(link)

            profiles = soup.find_all('dl', class_='postprofile')
            for profile in profiles:
                dd = profile.find('dd', class_="")
                if dd is not None:
                    link = dd.a.get('href')
                    username = profile.find('a', class_='username')
                    if username is not None:
                        self._data[username.text] = link.split("/")[-1]
                        print(len(self._data))
                    else:
                        username_admin = profile.find('a', class_='username-coloured')
                        if username_admin is not None:
                            self._data[username_admin.text] = link.split("/")[-1]

    async def get_page_data_inner(self, session, link):
        async with session.get(url=link) as response:
            response_text = await response.text()
            soup = BeautifulSoup(response_text, 'lxml')
            links = soup.find_all('a', class_='topictitle')
            for item in links:
                self.link_list_without_topic.append(item.get('href'))

    async def get_page_data(self, session, link):
        async with session.get(url=link) as response:
            response_text = await response.text()
            soup = BeautifulSoup(response_text, 'lxml')
            try:
                action_bar = soup.find('div', class_='action-bar')
                pagination = action_bar.find('div', class_='pagination')
                li_list = pagination.find_all('li')[-2]
                last_page = li_list.a.get('href')
                self.pages.append(last_page)
            except Exception as ex:
                self.pages_without_pagination.append(link)
            a_forum_title = soup.find_all('a', class_='forumtitle')
            if a_forum_title is not None:
                for link in a_forum_title:
                    link_href = link.get('href')
                    async with session.get(url=link_href) as response_inner:
                        response_text_in = await response_inner.text()
                        soup_inner = BeautifulSoup(response_text_in, 'lxml')
                        try:
                            action_bar = soup_inner.find('div', class_='action-bar')
                            pagination = action_bar.find('div', class_='pagination')
                            li_list = pagination.find_all('li')[-2]
                            last_page = li_list.a.get('href')
                            self.sub_pages.append(last_page)
                        except Exception as ex:
                            self.sub_pages_without_pagination.append(link_href)