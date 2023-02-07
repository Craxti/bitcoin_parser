from selenium.common.exceptions import InvalidSessionIdException
from selenium.webdriver.common.by import By
import time
import requests
import random
from dateutil import parser

from src.config import *
from .utils import *
from src.log import log

class Parser:
    QUESTIONS_LINKS_CSS = '.qu-pb--medium a.q-box'
    CONTENT_CONTAINER_CSS = '.qu-pt--medium'
    DATE_CSS = '.post_timestamp'
    TOPIC_CSS = '.qu-userSelect--text'

    def __init__(self, browser, page_browser, posts_queue, run_event, db):
        self.run_event = run_event
        self.posts_queue = posts_queue
        
        self.browser = browser
        self.driver = self.browser.driver

        self.page_browser = page_browser
        self.page_driver = page_browser.driver

        self.db = db

    def start_scrolling(self):
        '''Запуск скроллинг страницы'''
        try:
            self.driver.get(URL)
            time.sleep(2)
            self.scroll_down()
        except InvalidSessionIdException:
            log.critical('[!] Браузер закрыт, выход...')
        except KeyboardInterrupt:
            log.info('[!] Выход из программы ...')
        except Exception as ex:
            log.error(f'[!] Ошибка при получении сайта {ex}')
        finally:
            self.run_event.set()
            self.browser.destroy()
            self.page_browser.destroy()

    def scrape_posts(self):
        '''Проверяет каждый пост, полученный скроллингом'''
        log.info('Запуск обработки каждого поста...')
        while not self.run_event.is_set():
                time.sleep(2)
                if not self.posts_queue.empty():
                    for _ in range(self.posts_queue.qsize()):
                        try:
                            if self.run_event.is_set(): return
                            url = self.posts_queue.get()
                            print(f'Обрабатываю ссылку: {url}')

                            self.page_driver.get(url)
                            time.sleep(2)
                            self.find_btc_address(url)
                        except Exception as ex:
                            log.error(f'Ошибка при парсинге поста {ex}')

    def get_profile(self, content):
        '''Получаем имя профиля'''
        for link in self.page_driver.find_elements(By.CSS_SELECTOR, 'a'):
            if 'profile' in link.get_attribute('href'):
                try:
                    return link.find_element(By.CSS_SELECTOR, 'span span').text
                except:
                    pass

        return None

    def get_date(self, content):
        '''Получаем дату поста'''
        try:
            str_date = content.find_elements(By.CSS_SELECTOR, Parser.DATE_CSS)[0].text.strip().replace('\n', '')
            return str(parser.parse(str_date).utcnow())
        except Exception:
            return None

    def get_topic(self, content):
        '''Получаем заголовок поста'''
        for item in content.find_elements(By.CSS_SELECTOR, Parser.TOPIC_CSS):
            if item.text.strip():
                return item.text
        return None

    def get_text(self, content):
        '''Получаем текст из поста'''
        for item in content.find_elements(By.CSS_SELECTOR, Parser.TOPIC_CSS)[1:]:
            try:
                text_el = item.find_element(By.CSS_SELECTOR, 'span')

                if text_el.text().strip():
                    return text_el.text.strip()[:80]
            except:
                pass

        return content.text.strip()[40:100]

    def find_btc_address(self, url):
        '''Если нашелся btc адрес, добавляем данные в бд'''
        data = []
        content = self.page_driver.find_element(By.CSS_SELECTOR, Parser.CONTENT_CONTAINER_CSS)

        if content:
            btc_addresses = find_bitcoin_address(content.text)
            
            if btc_addresses:
                name = self.get_profile(content)
                asked_at = self.get_date(content)
                text = self.get_text(content)
                topic = self.get_topic(content)
                data.append({
                    'btc_addresses': btc_addresses,
                    'name': name,
                    'asked_at': asked_at,
                    'topic': topic,
                    'text': text,
                    'url': url
                })

                self.db.insert_unique_many(data)

        return False

    def scroll_down(self):
        '''Скроллит страницу до последнего поста'''
        log.info('Запуск скроллинга всех постов...')
        last_height = self.driver.page_source
        loop_scroll = True
        all_links = []
        attempt = 0
        
        waiting_scroll_time = round(random.uniform(2, 4), 1)
        max_waiting_time = round(random.uniform(20, 40), 1)

        # Спускаемся внизя до самого последнего поста
        while loop_scroll:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            new_height = self.driver.page_source
            if new_height == last_height:
                # Контент не поменялся, ждем, дополнительное время
                waiting_scroll_time = max_waiting_time
                attempt += 1
                if attempt == 9:  # in the third attempt we end the scrolling
                    loop_scroll = False
            else:
                posts = self.driver.find_elements(By.CSS_SELECTOR, Parser.QUESTIONS_LINKS_CSS)
                links = filter_links([post.get_attribute('href') for post in posts if post.get_attribute('href') not in all_links])
                all_links = all_links + links

                for link in links:
                    self.posts_queue.put(link)

                attempt = 0
                waiting_scroll_time = round(random.uniform(2, 4), 1)

            last_height = new_height