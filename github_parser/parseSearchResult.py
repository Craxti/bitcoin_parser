import requests
import re
from bs4 import BeautifulSoup
from pprint import pp, pprint
import time
import pymongo


requestsSession = requests.Session()
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
baseUrl = 'https://github.com'
btcRegular = r'(\b([13]|bc1)[A-HJ-NP-Za-km-z1-9]{27,34})'
newBtcCount = 0
errorUrls = []
startTimeOverall = time.time()


client = pymongo.MongoClient('localhost', 27017)  # подключаемся к монго
db = client['BtcFromGithub'] # выбираем базу
collection = db['submited_link'] # создаем коллекцию
collection.create_index('bitcoin_adress', unique=True) # обозначаем поле, чтобы адрес кошелька не повторялся


def parsePage(start=1, query="bitcoin", limit=100):
    
    if (limit > 0):
        limit -=1
        url = f"{baseUrl}/search?p={start}&q={query}&type=Repositories"
        print(f"\nСтраница результатов поиска:\n{url}")
        print(f'Еще запланировано для обхода: {limit}')
        response = requestsSession.get(url, headers=headers)

        if response.status_code == 200:
            html = BeautifulSoup(response.text, 'lxml')
            repos = html.select('.repo-list-item')
            print(f'Найдено репозиториев: {len(repos)}')

            # запускаем парсинг репозиториев
            for idx, repo in enumerate(repos):
                url = f"{baseUrl}{repo.select_one('.f4 a')['href']}"
                print(f"\nОбрабатываю {idx+1}/{len(repos)} {url}")
                parseRepo(url)

            # находим номер следующей страницы
            nextPageUrl = html.select_one('a.next_page')
            if nextPageUrl:
                nextPageNumber = re.findall("[p=]([0-9]+)[&]", nextPageUrl['href'])
                parsePage(start=nextPageNumber[0], query=query, limit=limit)
            else: 
                endParsePage()
        else:
            print("Что-то пошло не так :(")
            errorUrls.append(url)
    else:
        endParsePage()

def parseRepo(url):
    startTime = time.time()
    response = requestsSession.get(url, headers=headers)
    bitcoins = []
    
    if response.status_code == 200:
        html = BeautifulSoup(response.text, 'lxml')
        text = html.select_one('#readme').text.strip()
        text = " ".join(text.split())
        
        capturingGroups = re.findall(btcRegular, text)
        bitcoins = []
        for b in capturingGroups:
            bitcoins.append(b[0])

    else:
        print("Что-то пошло не так :(")
        errorUrls.append(url)

    print(f"{round(time.time() - startTime, 2)} сек.\n", end="")

    if (bitcoins):
        for bitcoin in bitcoins:
            repo = {}
            repo['timestamp'] = time.time()
            repo['link'] = url
            # repo['nick'] = html.select_one('a[rel="author"]').text
            repo['bitcoin_adress'] = bitcoin

            try:
                collection.insert_one(repo)
                newBtcCount += 1
                print(f"{bitcoin} - записан")
            except Exception as e:
                # pprint(e)
                print(f"{bitcoin} - уже есть")
                
def endParsePage():
    global startTimeOverall
    global newBtcCount
    print(f"\n{'='*60}\nПарсинг завершен за {round(time.time() - startTimeOverall, 2)} сек.\nПолучено новых btc-адресов: {newBtcCount}\n", end='')
    if (len(errorUrls) != 0):
        print(f"Ошибки 404({len(errorUrls)})")


# запускаем парсер
'''
query: по какой фразе ищем репозитории. по умолчанию "bitcoin"
start: номер страницы результата поиска, с которой начнется парсинг. по умолчанию 1
limit: сколько страниц будем обрабатывать. если пусто, то обрабатываем все 100
'''
parsePage(query="bitcoin", start=1, limit=100)
