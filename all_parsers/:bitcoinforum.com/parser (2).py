from pymongo import MongoClient
from bs4 import BeautifulSoup
from datetime import datetime
import threading
import requests
import time
import json
import sys
import re

# Configuration
"""
Настройка выполняется переменными в секции Configuration
 * MAX_THREADS - Максимальное количество потоков, с скоростью канала в 100 Мб/с рекомендую не больше 5, поскольку будет часто выпадать ошибки соединения.
 * MAX_RETRIES - Количество повторных попыток опросить страницу при неудачном соединении (Код ответа отличный от 200). При значении 0 - попытки будут бесконечны (не рекомендуется, может зависнуть весь скрипт)

 По базе данных. На своем стенде использовал MongoDB 5.0.9 Community (Windows version)
 * MONGODB_HOST - Хост сервера БД
 * MONGODB_PORT - Порт сервера БД
 * MONGODB_DB - Имя базы данных
 * MONGODB_COLLECTION - Имя коллекции
"""
MAX_THREADS = 4
MAX_RETRIES = 2 # 0 - unlimit

#   DB
MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
MONGODB_DB = 'parser'
MONGODB_COLLECTION = 'users_btc'
# /Configuration

# Global vars (not modify!)
CONNECTION = None
DB = None
COLLECTION = None

# Functions
def is_topic(code):
    soup = BeautifulSoup(code, 'lxml')
    topic = soup.select('h3.catbg > span#author')
    if len(topic) > 0:
        return True
    else:
        return False

def get_messages(code):
    soup = BeautifulSoup(code, 'lxml')
    messages = soup.select('#forumposts > div[class^="windowbg2"], div[class^="windowbg"] > div.post_wrapper')
    return messages

def parse_message(code):
    soup = BeautifulSoup(code, 'lxml')
    author = soup.select('div.post_wrapper > div.poster > h4 > a')
    try:
        btc_profile = soup.select('div.post_wrapper > div.poster > ul.reset > li.custom > a')[0]['href'][8:]
    except:
        btc_profile = None

    btc = []

    if btc_profile != None:
        if check_correct_btc(btc_profile):
            btc.append(btc_profile)
    
    message_link = soup.select('div.post_wrapper > div.postarea > div.flow_hidden > div.keyinfo > h5 > a')[0]['href']
    message = soup.select('div.post_wrapper > div.postarea > div.post')[0].text
    try:
        signature = soup.select('div.post_wrapper > div.moderatorbar > div.signature')[0].text
    except:
        signature = None

    find_btc_message = search_btc_in_text(message)
    if find_btc_message:
        for _btc in find_btc_message:
            if _btc not in btc:
                btc.append(_btc)
    if signature != None:
        find_btc_signature = search_btc_in_text(signature)
        if find_btc_signature:
            for _btc in find_btc_signature:
                if _btc not in btc:
                    btc.append(_btc)

    if not btc:
        return False

    result = {'btc': btc, 'all_links_btc': message_link}
    userinfo = get_user(username=author[0].text, SearchFromUsername=True)

    return result | userinfo

def get_user(url=None,username=None,SearchFromUsername=False):

    if SearchFromUsername:
        data = requests.get(f'https://bitcoinforum.com/profile/{username}')
    else:
        data = requests.get(url)
    
    soup = BeautifulSoup(data.text, 'lxml')
    profile = soup.select('div#profileview')[0]

    username = profile.select('div#basicinfo > div.windowbg > div.content > div.username > h4')[0]
    rank = username.select('span')[0].text
    username = username.text.replace(rank,'').strip()
    try:
        avatar = profile.select('div#basicinfo > div.windowbg > div.content > img')[0]['src']
    except:
        avatar = None
    links = '\n'.join([lnk['href'] for lnk in profile.select('div#basicinfo > div.windowbg > div.content > ul.reset > li > a')])

    if not links:
        links = None

    info = profile.select('div#detailedinfo > div.windowbg2 > div.content')[0]
    try:
        description = info.select('dl')[0].find('dt', text='Personal Text: ').findNext('dd').text
    except:
        description = None

    try:    
        gender = info.select('dl')[0].find('dt', text='Gender: ').findNext('dd').text
    except:
        gender = None

    try:
        age = info.select('dl')[0].find('dt', text='Age:').findNext('dd').text
    except:
        age = None
    
    try:
        location = info.select('dl')[0].find('dt', text='Location:').findNext('dd').text
    except:
        location = None

    try:
        reg_date = info.select('dl.noborder')[0].find('dt', text='Date Registered: ').findNext('dd').text
    except:
        reg_date = None

    try:
        last_active = info.select('dl.noborder')[0].find('dt', text='Last Active: ').findNext('dd').text
    except:
        last_active = None
    
    try:
        language = info.select('dl.noborder')[0].find('dt', text='Language:').findNext('dd').text
    except:
        language = None

    try:
        signature = str(info.select('div.signature')[0]).replace('<h5>Signature:</h5>','').replace('<div class="signature">','').replace('\n</div>','').strip()
    except:
        signature = None

    result = {
        'name': username,
        'domain': 'bitcoinforum.com',
        'link': f'https://bitcoinforum.com/profile/{username}/',
        'avatar': avatar,
        'date_registered': reg_date,
        'last_active': last_active,
        'icq': None,
        'aim': None,
        'msn': None,
        'email': None,
        'website': links,
        'gender': gender,
        'age': age,
        'status': rank,
        'location': location,
        'language': language,
        'signature': signature,
        'description': description,
    }

    return result

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
    matches = re.findall(r'([A-z0-9]{25,45})', text.strip())
    btc = []
    if matches:
        for match in matches:
            if check_correct_btc(match):
                btc.append(match)
        return btc
    else:
        return False

def save_to_db(collection):
    global DB, COLLECTION
    list_btc = collection['btc']
    for btc in list_btc:
        result = COLLECTION.find_one({'bitcoin_adress': btc})
        if not result:
            _collection = collection
            del _collection['btc']
            _collection['bitcoin_adress'] = btc
            id = COLLECTION.insert_one(_collection).inserted_id
            return id
    return False


THREADS = []
COMPLITE = 0
TH_ID = 0

def thread_body(link, thread_id):
    global THREADS, COMPLITE, MAX_RETRIES
    retries = 0
    print(f"[T#{str(thread_id)}]: Start parsing page {link}...")
    data = requests.get(link,{'all':1})

    while data.status_code != 200:
        print(f"[T#{str(thread_id)}]: Error connect to site. Error code: #{data.status_code}. Wait 5 sec. and retry...")
        time.sleep(5)
        if retries != MAX_RETRIES:
            retries += 1
            data = requests.get(link,{'all':1})
        else:
            print(f"[T#{str(thread_id)}]: Error connect to site. Error code: #{data.status_code}. URL: {link}. Max retries, close thread!")
            THREADS.remove(thread_id)
            COMPLITE += 1
            return False


    if is_topic(data.text):
        messages = get_messages(data.text)
        print(f"[T#{str(thread_id)}]: Found {len(messages)} message(s)")
        for message in messages:
            response = parse_message(str(message))
            if response:
                btc = response['btc']
                page = response['all_links_btc']
                print(f"[T#{str(thread_id)}]: Found BTC: {btc}, in page: {page}")
                id = save_to_db(response)
                if id:
                    print(f"[T#{str(thread_id)}]: Saved to DB. Inserted ID: {str(id)}")
                else:
                    print(f"[T#{str(thread_id)}]: Not saved. Duplicate entry.")
    print(f"[T#{str(thread_id)}]: Complite.")
    THREADS.remove(thread_id)
    COMPLITE += 1

# /Functions

# DEBUG
# data = requests.get('https://bitcoinforum.com/gambling/free-18$-(0-0023-bitcoins)-21080/',{'all':1})
# messages = get_messages(data.text)
# for msg in messages:
#     print(f'\n===========================\n{msg}\n===========================\n')


# text = "Test text from btc LPVZE6id3W74LYnNFkVz1i9UrFtBZaKpnc from 1NQVApiMSURE2uGYPbRFbjGcBkt146WVPg coin base"
# print(search_btc_in_text(text))
# sys.exit()
# /DEBUG

# Script
try:
    URL = 'https://bitcoinforum.com/sitemap/?xml'

    print(f'Connection to database MongoDB: {MONGODB_HOST}:{str(MONGODB_PORT)}')

    CONNECTION = MongoClient(MONGODB_HOST, MONGODB_PORT)
    DB = CONNECTION[MONGODB_DB]
    COLLECTION = DB[MONGODB_COLLECTION]

    print(f'Loading sitemap file from url: {URL}')
    request = requests.get(URL)
    start_time = datetime.now()
    print(f'Start parsing sitemap. Start time: {start_time}')
    soup = BeautifulSoup(request.content, 'xml')
    print('Search url\'s...')

    items = soup.find_all('url')

    links = []
    for item in items:
        link = item.find('loc').get_text()
        links.append(link)

    total = len(links)

    print(f'Parsed {total} link(s). Starting threads...')
    print('Press CTRL+C from cancel job.')
except Exception as e:
    print(f"Error! Call exception: {str(e)}")
try:
    while COMPLITE != total:
        if len(THREADS) < MAX_THREADS:
            link = links[-1]
            th = threading.Thread(target=thread_body, args=(link,TH_ID))
            THREADS.append(TH_ID)
            TH_ID += 1
            links.remove(link)
            th.start()
        else:
            time.sleep(0.1)
    end_time = datetime.now() - start_time
    print(f'Job complite. Run time: {end_time}')
except KeyboardInterrupt:
    print('Job canceled. Wait close all threads...')
    while len(THREADS) != 0:
        time.sleep(0.5)
    sys.exit()
except Exception as e:
    print(f'WARN: Call exception: {e}')
# /Script