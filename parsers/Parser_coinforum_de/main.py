from db import MONGODB_HOST, MONGODB_PORT, MONGODB_DB, MONGODB_COLLECTION
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

"""
!!! ВНИМАНИЕ, WARNING, ACHTUNG !!!
На сайте coinforum.de стоит защита CloudFront. Да бы не получить блок по IP рекомендую использовать 1-2 потока.
Так же добавил переменную THREAD_WAIT для задержки между потоками, что бы не было бана IP. Рекомендуемое время задержки 3 и более секунд (рекомендуется 10)
"""

MAX_THREADS = 2
MAX_RETRIES = 3 # 0 - Off
THREAD_WAIT = 3 # 0 - Off wait
# /Configuration

# Global vars (not modify!)
CONNECTION = None
DB = None
COLLECTION = None
HOST = 'https://coinforum.de'

# Functions
def println(string):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return print(f'[{timestamp}] {string}')

def check_correct_btc(btc):
    if re.match(r'^[A-z0-9]{25,45}$', btc.strip()):
        try:
            check = requests.get(f'https://www.blockchain.com/btc/address/{btc.strip()}', timeout=10)
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
    page_block = soup.select('ul.ipsPagination > li.ipsPagination_last > a')
    if page_block:
        return int(page_block[0]['data-page'].strip())
    else:
        return None
    

def get_messages(content,thread=None):
    soup = BeautifulSoup(content, 'lxml')
    messages_body = soup.select('article.cPost')
    return messages_body

def get_profile_info(link,thread=None):
    if link:
        if THREAD_WAIT > 0:
            time.sleep(0.1)
        data = requests.get(link, params={'tab': 'field_core_pfield_15'}, timeout=10)
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
        dates_inner = soup.select('ul.ipsList_reset')[0]

        try:
            date_registered = dates_inner.find('li').findNext('h4', text='Benutzer seit').findNext('time')['title'].strip()
        except:
            date_registered = None

        try:
            last_activity = dates_inner.find('li').findNext('h4', text='Letzter Besuch').findNext('time')['title'].strip()
        except:
            last_activity = None 

        try:
            signature = str(soup.select('div#ipsTabs_elProfileTabs_elProfileTab_field_core_pfield_15_panel > div.ipsType_richText')[0])
        except:
            signature = None
        return {'date_registered': date_registered, 'last_activity': last_activity, 'signature' :signature}
    else:
        return {'date_registered': None, 'last_activity': None, 'signature' :None}


def parse_message(message,thread=None):
    soup = BeautifulSoup(message, 'lxml')
    inner_user = None
    inner_main = None
    try:
        inner_user = soup.select('aside.ipsComment_author')[0]
        inner_main = soup.select('div.ipsColumn > div.ipsComment_content')[0]
    except:
        if thread:
            println(f'[T#{str(thread)}] Incorrect thread. Skip.')
        else:
            println(f'Incorrect thread. Skip.')
        return None

    avatar_image_link = inner_user.select('ul > li.cAuthorPane_photo > div > a > img')
    if avatar_image_link:
        avatar = 'https:' + str(avatar_image_link[0]['src'])
    else:
        avatar = None

    username = inner_user.select('h3.cAuthorPane_author')[0].text.strip()
    try:
        profile_link = inner_user.select('h3.ipsType_sectionHead > strong > a')[0]['href'].strip()
    except:
        profile_link = None
    status = inner_user.select("ul > li[data-role='group'] > span")[0].text.strip()

    message_link = inner_main.select('div.ipsComment_meta > div.ipsType_reset > a')[0]['href']
    message_text = str(inner_main.select('div.cPost_contentWrap')[0])
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
TH_ID = 1
TOTAL = 0

def thread_subfunction(data, thread):
    global THREADS, COMPLITE, MAX_RETRIES, THREAD_WAIT
    global COLLECTION

    messages = get_messages(data,thread)
    btc = []
    for message in messages:
        _message = parse_message(str(message),thread)
        if _message != None: 
            btc_im_message = search_btc_in_text(_message['message_text'])
            if btc_im_message:
                for _btc in btc_im_message:
                    if _btc not in btc:
                        btc.append(_btc)

            if btc:
                del _message['message_text']
                profile = get_profile_info(_message['link'])
                if profile:
                    _message = _message | profile
                    btc_im_signature = search_btc_in_text(_message['signature'])
                    if btc_im_signature:
                        for _btc in btc_im_signature:
                            if _btc not in btc:
                                btc.append(_btc)
                # clear signature
                if _message['signature']:
                    soup = BeautifulSoup(_message['signature'],'lxml')
                    _message['signature'] = soup.text.strip()
                # /clear signature
                for _btc in btc:
                    result = COLLECTION.find_one({'bitcoin_adress': _btc.strip()})
                    if not result:
                        _message['bitcoin_adress'] = _btc.strip()
                        id = save_to_db(_message)
                        println(f'[T#{str(thread)}] BTC [{_btc.strip()}] saved to database. Inserted ID: {str(id)}')
    try:
        THREADS.remove(thread)
        COMPLITE += 1
        if THREAD_WAIT > 0:
            println(f'[T#{str(thread)}] Thread complite. Sleep {str(THREAD_WAIT)} sec...')
            time.sleep(THREAD_WAIT)
        else:
            println(f'[T#{str(thread)}] Thread complite.')
        return True
    except:
        return True

def thread_body(link, thread):
    global THREADS, COMPLITE, MAX_RETRIES, THREAD_WAIT
    retries = 0
    println(f"[T#{str(thread)}] Start parsing page {link}...")
    data = None
    data = requests.get(link, timeout=10)
    # if not data:
    #     print(f'{str(thread)} | err |{str(data.status_code)}|')
    # else:
    #     print(f'{str(thread)} | ok |{str(data.status_code)}|')

    while data.status_code != 200:
        println(f'[T#{str(thread)}] Error connect to link: {link}. Error code: {str(data.status_code)}.')
        if MAX_RETRIES > 0:
            if data.status_code == 403:
                println(f'[T#{str(thread)}] CloudFront block detected. Sleep 2 min.')
                time.sleep(120)
            if retries != MAX_RETRIES:
                println(f'[T#{str(thread)}] Retry connect for {str(THREAD_WAIT+3)} seconds...')
                retries += 1
                time.sleep(THREAD_WAIT+3)
                data = requests.get(link, timeout=10)
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
                    link = link+f'page/{str(page+1)}/'
                else:
                    link = link.replace(f'page/{str(page)}/', f'page/{str(page+1)}/')
                retries = 0
                println(f"[T#{str(thread)}] Start parsing page {link}...")
                data = requests.get(link, timeout=10)
                while data.status_code != 200:
                    println(f'[T#{str(thread)}] Error connect to link: {link}. Error code: {str(data.status_code)}.')
                    if data.status_code == 403:
                        println(f'[T#{str(thread)}] CloudFront block detected. Sleep 2 min.')
                        time.sleep(120)
                    if MAX_RETRIES > 0:
                        if retries != MAX_RETRIES:
                            println(f'[T#{str(thread)}] Retry connect for 3 seconds...')
                            retries += 1
                            time.sleep(3)
                            data = requests.get(link, timeout=10)
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
    global THREADS, COMPLITE, TH_ID, TOTAL
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
        data = requests.get(url, timeout=10)
    except Exception as e:
        println(f"Error downloading sitemap.xml from {url}.\n\tError: {str(e)}")
        return sys.exit(1)
    if data.status_code == 200:
        size_sitemap = round((len(data.content)/1024),2)
        println(f'Sitemap complite download ({str(size_sitemap)} Kb). Start parsing...')
        soup = BeautifulSoup(data.content, 'xml')
        maps = []
        links = []
        sitemaps = soup.find_all('sitemap')
        total_size = 0
        if sitemaps:
            println(f'Found > 1 sitemap. Start parsing another sitemap\'s...')
            for _sitemap in sitemaps:
                map_link = _sitemap.find('loc').get_text()
                if 'forums_Topic_' in map_link:
                    maps.append(map_link.strip())
            println(f'Parsed {str(len(maps))} sitemap(s) links. Start download sitemap(s)...')

            for map_url in maps:
                println(f'Starting downloading sitemap from {str(map_url)}...')
                try:
                    data = requests.get(map_url)
                except Exception as e:
                    println(f"Error downloading sitemap ({map_url}).\n\tError: {str(e)}")
                    return sys.exit(1)
                if data.status_code == 200:
                    size_sitemap = round((len(data.content)/1024),2)
                    total_size += size_sitemap
                    println(f'Sitemap complite download ({str(size_sitemap)} Kb). Download next sitemap...')
                    soup = BeautifulSoup(data.content, 'xml')
                    items = soup.find_all('url')
                    for item in items:
                        link = item.find('loc').get_text()
                        if '/topic/' in link:
                            links.append(link.strip())
                else:
                    println(f'Error downloading sitemap ({map_url}). Response code: {str(data.status_code)}. Please retry again.')
                    return sys.exit(1)
            total_size = round(total_size,2)
            if total_size > 1024:
                total_size = str(round((total_size/1024),2)) + ' Mb.'
            else:
                total_size = str(total_size) + 'Kb.'
            println(f'Complite download {str(len(maps))} sitemap(s). Total size: {str(total_size)}')
        else:
            items = soup.find_all('url')
            for item in items:
                link = item.find('loc').get_text()
                if '/topic/' in link:
                    links.append(link.strip())
            
        TOTAL = len(links)
        println(f'Parse complite. Founded {str(TOTAL)} link(s) of threads. Wait 1 min. and start parse threads...')
        time.sleep(60)

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
    else:
        println(f'Error downloading sitemap.xml. Response code: {str(data.status_code)}. Please retry again.')
        return sys.exit(1)

# /Functions


if __name__ == '__main__':
    sitemap = 'https://coinforum.de/sitemap.php'
    try:
        main(sitemap)
    except KeyboardInterrupt:
        println(f'Job canceled. Wait complite all threads and closing app.')
        raise SystemExit