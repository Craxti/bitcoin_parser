import re
import pymongo
import time
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup as bs
import datetime
from webdriver_manager.chrome import ChromeDriverManager

chromedriver_path = 'chromedriver' # путь к chromedriver
wait = 10 * 60 # ожидание между парсингами
wair_error = 60 # ожидание в случае ошибки подключения

class Parser:
    def __init__(self):
        user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36"
        self.headers={'user_agent': user_agent,}

        client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
        db = client['btc_users']
        self.collection = db['mentions_on_the_internet']

    def get_links(self):
        options = webdriver.ChromeOptions()
        driver = webdriver.Chrome(ChromeDriverManager().install())
        URL = 'https://zen.yandex.ru/search?query=bitcoin)'
        driver.get(URL)

        for i in range(1, 25000, 200):
            driver.execute_script(f"window.scrollTo(0, {i})")
            time.sleep(0.05)

        links = []
        links_parent = driver.find_elements(By.CLASS_NAME, value='card-image-compact-view__image-wrapper._without-paddings')
        for link in links_parent:
            links.append(link.find_element(By.CLASS_NAME, value='card-image-compact-view__clickable').get_attribute('href'))

        return links

    def check_adresses(self, adresses):
        for mention in self.collection.find():
            for adress in adresses:
                if adress in mention['adresses']:
                    return True
        return False

    def get_data(self, link, domain):       
        while True:
            try:
                res = requests.get(url=link, headers=self.headers)
                if res.status_code == 200: break
                else: time.sleep(wair_error)
            except Exception:
                time.sleep(wair_error)

        soup = bs(res.text, "html.parser")

        article = soup.find_all("p", class_="article-render__block article-render__block_underlined-text-enabled article-render__block_bold-italic-combination-enabled article-render__block_unstyled")
        paragraph = ''
        for p in article: paragraph += p.text + '\n'

        adresses =  re.findall("[13][1-9A-HJ-NP-Za-z]{25,34}", paragraph)

        if adresses == [] or self.check_adresses(adresses): return -1

        topic = soup.find_all("h1", class_="article__title")[0].text
        author = soup.find_all("div", class_="ui-lib-channel-info__title")[0].text
        data = soup.find_all("div", class_="article-stats-view__item")[0].text
        
        timestamp = datetime.datetime.now().strftime("%d-%m-%Y %H:%M")

        mention = {
            'topic': topic,
            'author': author,
            'data': data,
            'paragraph': paragraph,
            'link': link,
            'domain': domain,
            'adresses': adresses,
            'timestamp': timestamp
        }

        return mention

    def main(self):
        while True:
            links = self.get_links()
            for link in links:
                try:
                    mention = self.get_data(link, 'https://zen.yandex.ru/')
                    if mention != -1:
                        self.collection.insert_one(mention)
                except Exception:
                    continue
            time.sleep(wait)

parser = Parser()
parser.main()