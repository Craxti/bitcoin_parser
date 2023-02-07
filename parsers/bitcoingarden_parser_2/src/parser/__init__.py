from selenium.common.exceptions import InvalidSessionIdException
import time
import pickle
import re

from parsers.bitcoingarden_parser_2.src.log import log
from parsers.bitcoingarden_parser_2.src.config import URL, MEMBERS_PARAMS, POSTS_PARAMS

from parsers.bitcoingarden_parser_2.src.parser.utils import get_last_member_id, save_last_member_id, find_bitcoin_addresses
                
class Parser:
    HEADER_XPATH = "//div[contains(@id, 'header')]"
    LAST_MEMBER_XPATH = "//*[@id='upshrinkHeaderIC']/p[1]/strong[1]/a" # Путь до блока с последним зарегестрированным пользователем

    def __init__(self, browser, db):
        self.db = db
        self.browser = browser
        self.driver = self.browser.driver
        self.cloudflare = CloudFlare(self.browser, Parser.HEADER_XPATH)

    def start_parse_users(self):
        '''Парсинг всей необходимой информации о пользователях'''
        last_nickname = self.get_last_member_nickname() # Получаем последний никнейм (зарегестрированный полоьзователь)
        user_id = get_last_member_id() # Получаем последний ид пользователя, который парсился парсером

        while True:
            try:
                log.info(f'Парсим пользователя № {user_id}.')

                # Проверяем, есть ли каптча
                self.cloudflare.input_from_human_if_exists()

                # Заходим к пользователю, которого будем парсить
                url = f'{URL}{MEMBERS_PARAMS}{user_id}' 
                self.driver.get(url) 

                # Увеличиваем id и сохраняем
                user_id += 1
                save_last_member_id(user_id)

                # Достаем всю информацию с пользователя
                user = UserParser(user_id, self.browser)
                user = user.parse_info()

                if not user:
                    # Пользователь не существует, пропускаем
                    continue

                if not user['btc_addresses']:
                    # Биткоин адрессы у пользователя не найдены, пропускаем 
                    continue

                self.db.insert(user)
                log.info(f'[+] Пользователь №{user_id} сохранен!')

                #Сохраняем пользователя в бд
            except InvalidSessionIdException:
                log.critical('[!] Браузер закрыт, выход...')
                return
            except KeyboardInterrupt:
                self.browser.destroy()
                log.info('[!] Выход из программы ...')
                return
            except Exception as ex:
                log.error(f'[!] Ошибка при парсинге пользователя № {user_id} | {ex}')
        
    def get_last_member_nickname(self):
        '''Получаем последний никнейм на форуме, необходимо, что-бы понять, закончились люди в парсинге или нет'''
        if self.driver.current_url != URL:
            # Если текущая страница не будет главной, то заходим на нее
            self.driver.get(URL)

        element = self.browser.get_waited_element_or_none(Parser.LAST_MEMBER_XPATH, 10) # Ожидаем блока с последним пользователям

        if not element:
            # Если путь до блока с пользователям не найден, выход
            return None

        return element.text

    def start(self):
        #Заходим на сайт и дожидаемся проверки cloudflare
        self.driver.get(URL)
        self.browser.load_cookies_if_exists()
        self.cloudflare.input_from_human_if_exists()

        self.start_parse_users() # Запуск !!!

class UserParser:
    PROFILE_TITLE = '//*[@class="content"]//dt'
    PROFILE_INFO = '//*[@class="content"]//dd'
    PROFILE_SIGNATURE = '//*[@class="signature"]'
    PROFILE_AVATAR = '//img[@class="avatar"]'
    PROFILE_NICKNAME = '//*[@class="username"]/h4'

    PAGES_COUNT = '//div[@class="pagesection"]'
    PAGES_TOPICS = '//*/div[@class="topic"]'
    TOPIC_URL = '//div[@class="topic"]//div[@class="topic_details"]/h5/strong/a[2]'

    def __init__(self, user_id, browser):
        self.user_id = user_id
        self.url = f'{URL}{MEMBERS_PARAMS}{user_id}'
        self.browser = browser
        self.driver = browser.driver
        self.data = {}

    def parse_all_info(self):
        '''Получаем всю необходимую информацию о пользователе'''
        pass

    def get_signature_from_profile(self):
        '''Получаем signature пользователя, если существует'''
        signature = self.browser.get_element_or_none(UserParser.PROFILE_SIGNATURE)

        if signature:
            return signature.get_attribute('innerHTML').strip()
        
        return ''

    def get_avatar_url_from_profile(self):
        '''Получаем ссылку на аватар пользователя, если существует'''
        avatar = self.browser.get_element_or_none(UserParser.PROFILE_AVATAR)

        if avatar:
            return avatar.get_attribute('src')

        return ''

    def get_nickname_from_profile(self):
        '''Получаем никнейм пользователя'''
        nickname = self.browser.get_element_or_none(UserParser.PROFILE_NICKNAME)

        if nickname:
            return re.sub('(<.+>)', '', nickname.get_attribute('innerHTML'), flags = re.IGNORECASE | re.MULTILINE).strip()

        return ''

    def get_btc_address_from_profile(self):
        '''Ищем все биткоины адрессы на главной странице пользователя'''
        btc_addresses = []
        for value in self.data.copy().values():
            for btc_address in find_bitcoin_addresses(value):
                btc_addresses.append(btc_address)

        return btc_addresses

    def get_count_pages(self):
        pages = self.browser.get_element_or_none(UserParser.PAGES_COUNT)

        if pages:
            counts = [int(s) for s in pages.text.split() if s.isdigit()]

            if bool(counts):
                return max(counts)

        return 1

    def get_parsed_info_from_posts(self):
        '''Ищем биткоин адресс в постах'''
        self.data['btc_posts'] = []

        url = f'{URL}{POSTS_PARAMS}'.format(self.user_id, 0)
        self.driver.get(url)

        count_pages = self.get_count_pages()

        if count_pages == 1:
            return self.parse_topics()

        for idx in range(1, count_pages + 1):
            try:
                self.driver.get(url)
                url = f'{URL}{POSTS_PARAMS}'.format(self.user_id, idx * 15)
                self.parse_topics()
            except Exception as ex:
                log.error(f'Ошибка при получении постов {ex}')

    def parse_topics(self):
        '''Проверяем каждый пост отдельно'''
        topics = self.driver.find_elements_by_xpath(UserParser.PAGES_TOPICS)

        for topic in topics:
            btc_candidates = find_bitcoin_addresses(topic.get_attribute('innerHTML'))
            for btc_address in btc_candidates:
                if btc_address not in self.data['btc_addresses']:
                    self.data['btc_addresses'].append(btc_address)
                    log.info('[+] Найден биткоин адрес!')

                    topic_el = self.browser.get_element_or_none(UserParser.TOPIC_URL)
                    if topic_el:
                        topic_url = topic_el.get_attribute('href')
                        self.data['btc_posts'].append(topic_url)

    def get_parsed_info_from_profile(self):
        '''Получаем всю информацию с профиля'''
        titles = [
            item.text.lower().strip().replace(' ', '_').replace(':', '') 
            for item in self.driver.find_elements_by_xpath(UserParser.PROFILE_TITLE)
        ]
        info = [item.text.lower() for item in self.driver.find_elements_by_xpath(UserParser.PROFILE_INFO)]
        self.data = dict(zip(titles, info))

        if not bool(self.data):
            # Если пользователь пуст, выходим
            return None

        self.data['name'] = self.get_nickname_from_profile()
        self.data['avatar_url'] = self.get_avatar_url_from_profile()
        self.data['signature'] = self.get_signature_from_profile()
        self.data['btc_addresses'] = self.get_btc_address_from_profile()
        self.data['url'] = URL
        
        return self.data

    def parse_info(self):
        self.get_parsed_info_from_profile()

        if not bool(self.data):
            # Если пользователь пуст, выходим
            return None

        self.get_parsed_info_from_posts()

        return self.data

class CloudFlare:
    CLOUDFLARE_XPATH = "//div[contains(@class, 'cf-im-under-attack')]"

    def __init__(self, driver, original_site_xpath):
        self.driver = driver
        self.original_site_xpath = original_site_xpath

    def is_exists(self):
        '''Проверяем, имеется ли страница от cloudflare'''
        return self.driver.get_element_or_none(CloudFlare.CLOUDFLARE_XPATH)

    def is_original_site_exists(self):
        '''Проверяем, открыл ли нужный сайт, без cloudflare'''
        return self.driver.get_element_or_none(self.original_site_xpath)

    def input_from_human_if_exists(self):
        '''Если clouflare имеется, просим вручную заного зайти на сайт'''
        try:
            is_exists = False
            while True:
                if self.is_exists():
                    # Если cloudflare i'm under attack имеется, продолжаем ждать
                    if not is_exists:
                        is_exists = True
                        log.warning('[!] Если обнаружен cloudflare:\n1) Зайдите занова на сайт, открыв новую вкладку\n2) Дождитесь завершение проверки CloudFlare')
                    time.sleep(2)
                    continue

                if not self.driver.get_waited_element_or_none(self.original_site_xpath, 2):
                    # Если главная страница не загружена, продолжаем ждать
                    continue

                if is_exists:
                    log.info('[+] Проверка на человека пройдена!')

                self.driver.close_all_not_active_tabs() # Закрываем старые вкладки
                self.driver.save_cookies() # Сохраняем актуальные куки

                return
        except Exception as ex:
            log.error(f'Ошибка при проверке cloudflare {ex}')