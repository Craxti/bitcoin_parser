from inspect import signature
from db import MONGODB_HOST, MONGODB_PORT, MONGODB_DB, MONGODB_COLLECTION
from sitemap_generator import generate_sitemap
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from bs4 import BeautifulSoup
from datetime import datetime
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
"""

MAX_THREADS = 3
MAX_RETRIES = 2 # 0 - unlimit
# /Configuration

# Global vars (not modify!)
CONNECTION = None
DB = None
COLLECTION = None
HOST = 'https://bitalk.org'

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
    try:
        pages = soup.select('div.PageNav')[0]['data-last']
    except:
        return None

    if pages:
        return int(pages)
    else:
        return None

def get_messages(content,thread=None):
    soup = BeautifulSoup(content, 'lxml')
    messages_body = soup.select('li.message')
    return messages_body

def get_profile_info(link,thread=None):
    data = requests.get(link,{'card': 1})
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
    
    username = soup.select('div.userInfo > h3.username')[0].text.strip()

    signature = soup.select('div.userInfo > blockquote.status')
    if signature:
        signature = signature[0].text.strip()
    else:
        signature = None

    try:
        date_registered = soup.select('dl.userStats')[0].find('dt', text='На форуме с:').findNext('dd').text.strip()
    except:
        date_registered = None

    try:
        last_activity = soup.select('dl.lastActivity')[0].find('dt', text=f'Последняя активность {username}:').findNext('dd').text.strip()
    except:
        last_activity = None   
    age = None
    try:
        gender = soup.select('div.userInfo > div.userBlurb')[0].text.strip()
        splitter = gender.split(', ')
        if len(splitter) > 1:
            age = int(splitter[1])
            gender = splitter[0].strip()
    except:
        gender = None

    return {'date_registered': date_registered, 'last_activity': last_activity, 'signature': signature, 'gender': gender, 'age': age}


def parse_message(message,thread=None):
    soup = BeautifulSoup(message, 'lxml')
    inner_user = None
    inner_main = None
    try:
        inner_user = soup.select('div.messageUserInfo > div.messageUserBlock')[0]
        inner_main = soup.select('div.messageInfo > div.messageContent')[0]
    except:
        if thread:
            println(f'[T#{str(thread)}] Incorrect thread. Skip.')
        else:
            println(f'Incorrect thread. Skip.')
        return None

    avatar_image_link = inner_user.select('div.avatarHolder > a > img')
    if avatar_image_link:
        avatar = HOST + '/' + str(avatar_image_link[0]['src'])
    else:
        avatar = None

    username = inner_user.select('h3.userText > a.username')[0].text.strip()
    try:
        profile_link = HOST + '/' + inner_user.select('h3.userText > a.username')[0]['href'].strip()
    except:
        profile_link = None
    try:
        status = inner_user.select('h3.userText > em.userTitle')[0].text.strip()
    except:
        status = None

    message_link = HOST + '/' + soup.select('div.messageInfo > div.messageMeta > div.publicControls > a')[0]['href']
    message_text = str(inner_main)

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
        'status': status,
        'location': None,
        'language': None,
        'signature': None,
        'description': None,
        'timestamp': datetime.utcnow(),
        'message_text': message_text,
        'all_links_btc': message_link,
    }
    return message

def save_to_db(collection):
    global COLLECTION
    try:
        id = COLLECTION.insert_one(collection).inserted_id
    except DuplicateKeyError as e:
        print(str(e))
        return False
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
        if _message != None: 
            #print(_message)
            btc_im_message = search_btc_in_text(_message['message_text'])
            if btc_im_message:
                for _btc in btc_im_message:
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
                        if _message['signature']:
                            btc_im_signature = search_btc_in_text(_message['signature'])
                            if btc_im_signature:
                                for _btc in btc_im_signature:
                                    if _btc not in btc:
                                        btc.append(_btc)
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

def download_sitemap(url):
    # Start parse sitemap.xml
    println(f'Starting downloading sitemap from {str(url)}...')
    try:
        data = requests.get(url)
    except Exception as e:
        println(f"Error downloading sitemap.xml from {url}.\n\tError: {str(e)}")
        return False
    if data.status_code == 200:
        size_sitemap = round((len(data.content)/1024),2)
        println(f'Sitemap complite download ({str(size_sitemap)} Kb). Start parsing...')
        soup = BeautifulSoup(data.content, 'xml')
        links = []
        items = soup.find_all('url')
        for item in items:
            link = item.find('loc').get_text()
            if '/t/' in link:
                links.append(link.strip())
    else:
        println(f"Error connection to {url}.\n\tError code: {str(data.status_code)}")
        return False
    return links

def main(url):
    println('Job started. Press CTRL+C to cancel job.')
    # Check DB connection
    global MONGODB_HOST, MONGODB_PORT, MONGODB_DB, MONGODB_COLLECTION
    global CONNECTION, DB, COLLECTION
    global THREADS, COMPLITE, TH_ID, TOTAL
    global HOST
    println(f'Connection to database (MongoDB: mongodb://{MONGODB_HOST}:{str(MONGODB_PORT)})...')
    try:
        CONNECTION = MongoClient(MONGODB_HOST, MONGODB_PORT)
        DB = CONNECTION[MONGODB_DB]
        COLLECTION = DB[MONGODB_COLLECTION]
    except Exception as e:
        println(f'Error connect to database.\n\tError: {str(e)}')
        return sys.exit(1)
    println(f'Connection to database complite.')

    links = download_sitemap(url)

    if not links:      
        links = generate_sitemap(HOST + '/')

    TOTAL = len(links)
    println(f'Parse complite. Founded {str(TOTAL)} link(s) of threads. Start parse threads...')

    while COMPLITE != TOTAL:
        if len(THREADS) < MAX_THREADS:
            if len(links) > 0:
                link = links[0]
                th = threading.Thread(target=thread_body, args=(link,TH_ID))
                THREADS.append(TH_ID)
                TH_ID += 1
                links.remove(link)
                th.start()
            else:
                println(f'Job complite!')
                sys.exit(0)
        else:
            time.sleep(0.5)
    println(f'Job complite!')
    sys.exit(0)

# /Functions


if __name__ == '__main__':
    sitemap = 'https://bitalk.org/sitemap.xml'
    try:
        main(sitemap)
    except KeyboardInterrupt:
        println(f'Job canceled. Wait complite all threads and closing app.')
        raise SystemExit