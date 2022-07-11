from cgitb import text
from distutils.debug import DEBUG
from os import link
from pymongo import MongoClient
from bs4 import BeautifulSoup
from datetime import datetime
from db import MONGODB_HOST, MONGODB_PORT, MONGODB_DB, MONGODB_COLLECTION
import threading
import requests
import time
import sys
import re

# Configuration
"""
Настройка выполняется переменными в секции Configuration
 * MAX_THREADS - Максимальное количество потоков, с скоростью канала в 100 Мб/с рекомендую не больше 5, поскольку будет часто выпадать ошибки соединения.
 * MAX_RETRIES - Количество повторных попыток опросить страницу при неудачном соединении (Код ответа отличный от 200). При значении 0 - попытки будут бесконечны (не рекомендуется, может зависнуть весь скрипт)
 * DEBUG - Дебаг режим. True - включен, False - выключен
"""
DEBUG = False
MAX_THREADS = 3
MAX_RETRIES = 2 # 0 - unlimit
# /Configuration

# Global vars (not modify!)
CONNECTION = None
DB = None
COLLECTION = None
HOST = 'https://bitco.in'

# Functions
def println(string):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return print(f'[{timestamp}] {string}')

def check_correct_btc(btc):
    if re.match(r'^[A-z0-9]{25,45}$', btc.strip()):
        try:
            check = requests.get(f'https://www.blockchain.com/btc/address/{btc.strip()}')
            if check.status_code == 200:
                return True
            else:
                return False
        except:
            return False
    else:
        return False

def search_btc_in_text(text):
    if text:
        matches = re.findall(r'([A-z0-9]{25,45})', text.strip())
        btc = []
        if matches:
            for match in matches:
                if check_correct_btc(match):
                    btc.append(match)
            if btc:
                return btc
            else:
                return False
        else:
            return False
    else:
        return False

def get_pages_of_thread(content):
    soup = BeautifulSoup(content, 'lxml')
    page_block = soup.select('div.pageNav')
    pages = []
    if page_block:
        _pages = page_block[0].select('ul.pageNav-main > li.pageNav-page > a')
        for page in _pages:
            if page.text.isnumeric():
                pages.append(page.text)
        return int(pages[-1])
    else:
        pages = None
        return pages

def get_messages(content,thread=None):
    soup = BeautifulSoup(content, 'lxml')
    messages_body = soup.select('article.message')
    return messages_body

def get_profile_info(link,thread=None):
    data = requests.get(link)
    retries = 0
    while data.status_code != 200:
        if thread:
            println(f'[T#{str(thread)}] Error connect to profile link: {link}. Error code: {str(data.status_code)}.')
            if MAX_RETRIES > 0:
                if retries != MAX_RETRIES:
                    println(f'[T#{str(thread)}] Retry connect for 3 seconds...')
                    retries += 1
                    time.sleep(3)
                    data = requests.get(link)
                else:
                    println(f'[T#{str(thread)}] Max retries for connect. Skip.')
                    return False
            else:
                println(f'[T#{str(thread)}] Skip.')
                return False
    soup = BeautifulSoup(data.content, 'lxml')
    lock_profile = soup.select('div.blockMessage--error')
    if lock_profile:
        date_registered = None
        last_activity = None
    else:
        dates_inner = soup.select('div.uix_memberHeader__extra > div.memberHeader-blurb')

        try:
            date_registered = dates_inner[0].find('dt', text='Member since').findNext('dd').findNext('time')['title']
        except:
            date_registered = None

        try:
            last_activity = dates_inner[1].find('dt', text='Last seen').findNext('dd').findNext('time')['title']
        except:
            last_activity = None          
    return {'date_registered': date_registered, 'last_activity': last_activity}


def parse_message(message,thread=None):
    soup = BeautifulSoup(message, 'lxml')
    inner_user = soup.select('div.message-inner > div.message-cell--user')[0]
    inner_main = soup.select('div.message-inner > div.message-cell--main')[0]

    avatar_image_link = inner_user.select('div.message-avatar-wrapper > a > img')
    if avatar_image_link:
        avatar = HOST + str(avatar_image_link[0]['src'])
    else:
        avatar = None

    username = inner_user.select('h4.message-name > a.username')[0].text.strip()
    profile_link = HOST + inner_user.select('h4.message-name > a.username')[0]['href'].strip()
    status = inner_user.select('h5.userTitle')[0].text.strip()

    message_link = HOST + inner_main.select('header.message-attribution > ul.message-attribution-opposite > li > a')[0]['href']
    message_text = str(inner_main.select('article.message-body > div.bbWrapper')[0])
    signature = inner_main.select('aside.message-signature > div.bbWrapper')
    if signature:
        signature_text = inner_main.select('aside.message-signature > div.bbWrapper')[0].text
    else:
        signature_text = None

    message = {
        'name': username,
        'domain': HOST.replace('https://',''),
        'link': profile_link,
        'avatar': avatar,
        'icq': None,
        'aim': None,
        'msn': None,
        'email': None,
        'website': None,
        'gender': None,
        'age': None,
        'status': status,
        'location': None,
        'language': None,
        'signature': signature_text,
        'description': None,
        'message_text': message_text,
        'all_links_btc': message_link,
    }
    return message

def save_to_db(collection):
    global COLLECTION
    id = COLLECTION.insert_one(collection).inserted_id
    return id

THREADS = []
COMPLITE = 0
TH_ID = 0
TOTAL = 0

def thread_subfunction(data, thread):
    global THREADS, COMPLITE, MAX_RETRIES
    global COLLECTION

    messages = get_messages(data,thread)
    btc = []
    for message in messages:
        _message = parse_message(str(message),thread)
        btc_im_message = search_btc_in_text(_message['message_text'])
        if btc_im_message:
            for _btc in btc_im_message:
                if _btc not in btc:
                    btc.append(_btc)

        btc_im_signature = search_btc_in_text(_message['signature'])
        if btc_im_signature:
            for _btc in btc_im_signature:
                if _btc not in btc:
                    btc.append(_btc)

        if btc:
            del _message['message_text']
            for _btc in btc:
                result = COLLECTION.find_one({'bitcoin_adress': _btc.strip()})
                if not result:
                    profile = get_profile_info(_message['link'])
                    if profile:
                        _message = _message | profile
                    _message['timestamp'] = datetime.utcnow()
                    _message['bitcoin_adress'] = _btc.strip()
                    id = save_to_db(_message)
                    println(f'[T#{str(thread)}] BTC [{_btc.strip()}] saved to database. Inserted ID: {str(id)}')
    try:
        THREADS.remove(thread)
        COMPLITE += 1
        println(f'[T#{str(thread)}] Thread complite.')
        return True
    except:
        return True

def thread_body(link, thread):
    global THREADS, COMPLITE, MAX_RETRIES
    
    println(f"[T#{str(thread)}] Start parsing page {link}...")
    data = requests.get(link)
    while data.status_code != 200:
        println(f'[T#{str(thread)}] Error connect to link: {link}. Error code: {str(data.status_code)}.')
        if MAX_RETRIES > 0:
            if retries != MAX_RETRIES:
                println(f'[T#{str(thread)}] Retry connect for 3 seconds...')
                retries += 1
                time.sleep(3)
                data = requests.get(link)
            else:
                println(f'[T#{str(thread)}] Max retries for connect. Skip.')
                THREADS.remove(thread)
                COMPLITE += 1
                return False
        else:
            println(f'[T#{str(thread)}] Skip.')
            THREADS.remove(thread)
            COMPLITE += 1
            return False

    #Paging
    pages = get_pages_of_thread(data.content)
    if pages:
        println(f"[T#{str(thread)}] Found {str(pages)} page(s) in thread.")
        for page in range(pages):
            if page > 0:
                if page == 1:
                    link = link+f'page-{str(page+1)}'
                else:
                    link = link.replace(f'page-{str(page)}', f'page-{str(page+1)}')
                retries = 0
                println(f"[T#{str(thread)}] Start parsing page {link}...")
                data = requests.get(link)
                while data.status_code != 200:
                    println(f'[T#{str(thread)}] Error connect to link: {link}. Error code: {str(data.status_code)}.')
                    if MAX_RETRIES > 0:
                        if retries != MAX_RETRIES:
                            println(f'[T#{str(thread)}] Retry connect for 3 seconds...')
                            retries += 1
                            time.sleep(3)
                            data = requests.get(link)
                        else:
                            println(f'[T#{str(thread)}] Max retries for connect. Skip.')
                            THREADS.remove(thread)
                            COMPLITE += 1
                            return False
                    else:
                        println(f'[T#{str(thread)}] Skip.')
                        THREADS.remove(thread)
                        COMPLITE += 1
                        return False
            thread_subfunction(data.content, thread)
    else:
        thread_subfunction(data.content, thread)
    return True

def main(url):
    println('Job started. Press CTRL+C to cancel job.')
    # Check DB connection
    global MONGODB_HOST, MONGODB_PORT, MONGODB_DB, MONGODB_COLLECTION
    global CONNECTION, DB, COLLECTION
    global THREADS, COMPLITE, TH_ID, TOTAL, DEBUG
    println(f'Connection to database (MongoDB: mongodb://{MONGODB_HOST}:{str(MONGODB_PORT)})...')
    try:
        CONNECTION = MongoClient(MONGODB_HOST, MONGODB_PORT)
        DB = CONNECTION[MONGODB_DB]
        COLLECTION = DB[MONGODB_COLLECTION]
    except Exception as e:
        println(f'Error connect to database.\n\tError: {str(e)}')
        return sys.exit(1)
    println(f'Connection to database complite.')

    # Start parse sitemap.xml
    println(f'Starting downloading sitemap from {str(url)}...')
    try:
        data = requests.get(url)
    except Exception as e:
        println(f"Error downloading sitemap.xml from {url}.\n\tError: {str(e)}")
        return sys.exit(1)
    if data.status_code == 200:
        size_sitemap = round((len(data.content)/1024),2)
        println(f'Sitemap complite download ({str(size_sitemap)} Kb). Start parsing...')
        soup = BeautifulSoup(data.content, 'xml')
        links = []
        items = soup.find_all('url')
        for item in items:
            link = item.find('loc').get_text()
            if '/threads/' in link:
                links.append(link.strip())    
        TOTAL = len(links)
        println(f'Parse complite. Founded {str(TOTAL)} link(s) of threads. Start parse threads...')

        if DEBUG:
            println(f'!!! DEBUG MODE !!!')
            link = links[1]
            data = requests.get(link)
            print(get_messages(data.content))
            raise SystemExit

        while COMPLITE != TOTAL:
            if len(THREADS) < MAX_THREADS:
                link = links[1]
                th = threading.Thread(target=thread_body, args=(link,TH_ID))
                THREADS.append(TH_ID)
                TH_ID += 1
                links.remove(link)
                th.start()
            else:
                time.sleep(0.5)
        println(f'Job complite!')
        sys.exit(0)
    else:
        println(f'Error downloading sitemap.xml. Response code: {str(data.status_code)}. Please retry again.')
        return sys.exit(1)

# /Functions


if __name__ == '__main__':
    sitemap = 'https://bitco.in/forum/sitemap.xml'
    try:
        main(sitemap)
    except KeyboardInterrupt:
        println(f'Job canceled. Wait complite all threads and closing app.')
        raise SystemExit