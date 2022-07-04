from bs4 import BeautifulSoup
import requests
from mongodb import dbCollection, insert_filtered_data
import sqlite3
import pymongo
import time 

def search_btc_mention(btc_addr=''):
    all_links = ' '

    if not btc_addr:
        return ''

    url = "https://www.google.com/search?q=" + btc_addr + "+bitcointalk.org&start="
    headers = {"user-agent" : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:65.0) Gecko/20100101 Firefox/65.0"}
    n = 0

    while (n < 50):
        time.sleep(2)
        r = requests.get(url+str(n), headers=headers)

        n = n + 10

        if r.status_code == 200:

            all_site = BeautifulSoup(r.text, "lxml")

            all_a = all_site.find_all("a")

            for a in all_a :
                link = a.get("href")
                if ("https://bitcointalk.org/index.php?topic=" in str(link) and "https://translate.google.com" not in str(link)):
                    all_links = all_links + '    ' + link
        else:
            print("Google Error " + str(r.status_code))
        
        print("Search links...")
    
    return all_links

def main ():
    url = "https://bitcointalk.org/index.php?action=profile;u="

    last_element = dbCollection.find_one({}, sort=[( '_id', pymongo.DESCENDING )] )

    if (last_element == None):
        n_url = 0
    else :
        n_url = last_element['id']

    print('[+] Working...')

    while (n_url < 3500000):
        #time.sleep(0.3)
        n_url = n_url + 1
        
        r = requests.get(url+str(n_url)) 
        all_site = BeautifulSoup(r.text, "lxml") 

        try :
            all_info = all_site.find("td", class_="windowbg").find_all("tr")

            db_payload = {
                'id': n_url,
                'domain': 'bitcointalk.org',
                'link': url + str(n_url),
                'description': '',
                'all_links_btc': '',
                'bitcoin_address': '',
                'avatar': '',
                'signature': ''
            }

            for idx, info in enumerate(all_info):
                try:
                    if not info.find('td'): 
                        continue   
               
                    title_key = info.find('td').text.lower().strip().replace(' ', '_').replace(':', '').replace('\n', '')
                    if not title_key: 
                        continue     

                    if 'signature' in title_key and len(title_key) > len('signature'):
                        db_payload['signature'] = info.find('td').text.replace('\n', '').replace('Signature:', '')
                        continue
                    
                    db_payload[title_key] = info.find_all('td')[1].text.replace('\n', '').replace('N/A', '').replace('hidden', '')
                    
                except IndexError as ex:
                    pass
            
            db_payload['all_links_btc'] = search_btc_mention(db_payload['bitcoin_address'])
            db_payload['signature'] = db_payload['signature'].replace('Signature:', '')
            
            # if the bitcoin address is empty, skip
            if not db_payload['bitcoin_address']:
                continue

            avatar = "https://bitcointalk.org/useravatars/avatar_" + str(n_url) + ".png"
            if requests.get(avatar).status_code == 200:
                db_payload['avatar'] = avatar

            insert_filtered_data(db_payload)

            print('[+] {}'.format(url+str(n_url)))

        except AttributeError as ex:
            #print(f"{n_url} is void")
            pass

if (__name__ == '__main__'):
    main()