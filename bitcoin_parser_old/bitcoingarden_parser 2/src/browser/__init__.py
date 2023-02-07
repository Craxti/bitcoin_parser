import time
import sys
import signal
import pathlib
import os
import pickle

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By

from .utils import get_options, force_exit
from src.log import log

os.environ['WDM_LOG_LEVEL'] = '0'

class Browser:
    def __init__(self, is_firefox=False, is_headless=False):
        try:
            log.info('[+] Инициализация браузера, это займет время...')
            self.is_firefox = is_firefox
            self.is_headless = is_headless
            options = get_options(is_firefox, is_headless)

            if is_firefox:
                self.driver = webdriver.Firefox(executable_path=GeckoDriverManager().install(), options=options)
            else:
                self.driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
            
            time.sleep(4)
            
            self._remove_webdriver_property() # Удаляем свойство webdriver
            self.driver.maximize_window() # Экран на всю высоту

            time.sleep(6)
        except Exception as ex:
            log.error(f'[!] Ошибка при инициализации браузера {ex}')
            time.sleep(5)
            sys.exit(1)

    def _remove_webdriver_property(self):
        '''Убираем ключевое свойство webdriver из драйвера'''

        if self.is_firefox:
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.execute_script('delete navigator.__proto__.webdriver')
        else:
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        self.driver.execute_script("Object.defineProperty(navigator, 'userAgent', {get: () => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.53 Safari/537.36'})")
        self.driver.execute_script("Object.defineProperty(navigator, 'language', {get: () => 'ru-RU'})")
        self.driver.execute_script("Object.defineProperty(navigator, 'deviceMemory', {get: () => 2})")
        self.driver.execute_script("Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 2})")
        self.driver.execute_script("Object.defineProperty(navigator, 'platform', {get: () => 'MacIntel'})")
        
    def destroy(self):
        '''Полное завершение работы браузера'''
        try:
            self.driver.close() # Закрывает вкладку
        except Exception:
            pass

        try:
            self.driver.quit() # Закрывает сессиюю selenium
        except Exception as ex:
            log.error(f'Ошибка при закрытии браузера {ex}')

    def get_waited_element_or_none(self, xpath, wait_sec = 10):
        '''Ожидаем путь по xpath в течении заданного времени'''
        try:
            wait = WebDriverWait(self.driver, wait_sec)
            element = wait.until(EC.visibility_of_element_located((By.XPATH, xpath)))
            return element
        except Exception as ex:
            log.error(f'[!] Ошибка при ожидании элемента {ex}')
            return None

    def get_element_or_none(self, xpath):
        '''Пытаемся найти элемент по xpath, с обработкой ошибки'''
        try:
            return self.driver.find_element_by_xpath(xpath)
        except NoSuchElementException:
            return None

    def close_all_not_active_tabs(self):
        '''Закрываем все неактивные вкладки'''
        if len(self.driver.window_handles) == 1:
            return

        current = self.driver.current_window_handle
        for handle in self.driver.window_handles:
           self.driver.switch_to.window(handle)
           if handle != current:
              self.driver.close()

        self.driver.switch_to.window(self.driver.window_handles[-1]) # Переходим в новую вкладку с новым открытым сайтом

    def load_cookies_if_exists(self):
        if os.path.exists('cookies.pkl'):
            cookies = pickle.load(open("cookies.pkl", "rb"))
            for cookie in cookies:
                self.driver.add_cookie(cookie)

    def save_cookies(self):
        '''Сохраняем актуальные cookies'''
        return pickle.dump(self.driver.get_cookies(), open("cookies.pkl","wb"))