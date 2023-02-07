from bs4 import BeautifulSoup
from datetime import datetime
import requests
import time
import re

FORUMS = []
THREADS = []
HOST = None
def println(string):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return print(f'[{timestamp}] {string}')

def parse_forum_links(link):
    global HOST, FORUMS
    try:
        data = requests.get(link, timeout=10)
    except Exception as e:
        println(f'Error connect to {link}. Error: {str(e)}')
        return False
    
    if data.status_code != 200:
        println(f'Error connect to {link}. Status code: {str(data.status_code)}')
        return False
    else:
        content = data.content
        soup = BeautifulSoup(content, 'lxml')
        forum_links = soup.select("a[href^='forums/']")

        threads_links = soup.select("h3.title > a[href^='threads/']")
        if threads_links:
            for thread_link in threads_links:
                if '.rss' not in thread_link['href']:
                    _link = str(HOST + thread_link['href'])
                    _link = re.sub('\?.*', '', _link, flags=re.DOTALL)
                    if _link not in THREADS:
                        #println(f'Found thread link: {_link}')
                        THREADS.append(_link)
        if forum_links:
            for forum_link in forum_links:
                if '.rss' not in forum_link['href']:
                    _link = str(HOST + forum_link['href'])
                    _link = re.sub('\?.*', '', _link, flags=re.DOTALL)
                    if _link not in FORUMS:
                        #println(f'Found forum link: {_link}')
                        FORUMS.append(_link)
                        parse_forum_links(_link)  

def generate_sitemap(link):
    global HOST, FORUMS
    HOST = link
    println('Wait. Parsing all forum links. This procedure takes ~5 min.')
    parse_forum_links(link)
    println(f'Found {str(len(THREADS))} thread(s) in {str(len(FORUMS))} forum(s).')
    return THREADS
    

if __name__ == '__main__':
    generate_sitemap('https://bitalk.org/')
