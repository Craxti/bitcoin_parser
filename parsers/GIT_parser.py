import time

import re
from bs4 import BeautifulSoup
import pymongo
import requests

import settings


class Main:
    def __init__(self, find_it):
        self.baseUrl = 'https://github.com'
        self.start = 1
        self.query = find_it
        self.limit = settings.LIMIT_PARS_PAGES
        self.errorUrls = []
        self.startTimeOverall = time.time()
        self.newBtcCount = 0
        self.btcRegular = r'(\b([13]|bc1)[A-HJ-NP-Za-km-z1-9]{27,34})'
        self.init_mongo()

    def get_response(self, url):
        '''инициализировать сессию и передать url вернуть ответ'''
        requestsSession = requests.Session()
        response = requestsSession.get(url, headers=settings.HEADERS)
        return response

    def init_mongo(self):
        '''инициализируем pymongo, построчно: создаем клиента,
        подключаемся к базе, выбираем коллекцию и индексируем
        чтобы небыло повторений(и по индексу удобно искать'''
        client = pymongo.MongoClient(settings.HOST, settings.PORT)
        db = client[settings.PYMONGO_BASE]
        self.collection = db[settings.GIT_LINKS]
        self.collection.create_index('bitcoin_adress', unique=True)

    def run(self):
        '''основная функция запуска приложения:
        проверяем запросы от сервера на предмет возможности
        парса если можем то парсим'''
        for _ in range(self.limit):
            url = f"{self.baseUrl}/search?p={self.start}&q={self.query}&type=Repositories"
            print(f"\nСтраница результатов поиска:\n{url}")
            print(f'Еще запланировано для обхода: {self.limit}')
            response = self.get_response(url)

            if response.status_code == 200:
                html = BeautifulSoup(response.text, 'lxml')
                repos = html.select('.repo-list-item')
                print(f'Найдено репозиториев: {len(repos)}')
                self.pars_repositores(html, repos)
            else:
                print("Что-то пошло не так :(")
                self.errorUrls.append(url)

        self.endParsePage()

    def pars_repositores(self, html, repos):
        '''парс url репозитория'''
        for idx, repo in enumerate(repos):
            url = f"{self.baseUrl}{repo.select_one('.f4 a')['href']}"
            print(f"\nОбрабатываю {idx+1}/{len(repos)} {url}")
            self.parserepo(url)

        # находим номер следующей страницы
        nextPageUrl = html.select_one('a.next_page')
        if nextPageUrl:
            nextPageNumber = re.findall("[p=]([0-9]+)[&]", nextPageUrl['href'])
            self.start = nextPageNumber[0]
        else:
            self.endParsePage()

    def parserepo(self, url):
        '''парс репозитория'''
        startTime = time.time()
        response = self.get_response(url)
        bitcoins = []
        if response.status_code == 200:
            html = BeautifulSoup(response.text, 'lxml')
            text = html.select_one('#readme').text.strip()
            text = " ".join(text.split())
            capturingGroups = re.findall(self.btcRegular, text)
            bitcoins = []
            for b in capturingGroups:
                bitcoins.append(b[0])

        else:
            print("Что-то пошло не так :(")
            self.errorUrls.append(url)
        print(f"{round(time.time() - startTime, 2)} сек.\n", end="")
        if bitcoins:
            self.find_bitcoin_link(bitcoins, url)

    def find_bitcoin_link(self, bitcoins, url):
        '''поиск ссылок на bitcoin'''
        for bitcoin in bitcoins:
            repo = {}
            repo['timestamp'] = time.time()
            repo['link'] = url
            # repo['nick'] = html.select_one('a[rel="author"]').text
            repo['bitcoin_adress'] = bitcoin

            try:
                self.collection.insert_one(repo)
                self.newBtcCount += 1
                print(f"{bitcoin} - записан")
            except Exception as e:
                # pprint(e)
                print(f"{bitcoin} - уже есть")

    def endParsePage(self):
        print(f"\n{'='*60}\nПарсинг завершен за {round(time.time() - self.startTimeOverall, 2)} сек.\nПолучено новых btc-адресов: {self.newBtcCount}\n", end='')
        if (len(self.errorUrls) != 0):
            print(f"Ошибки 404({len(self.errorUrls)})")