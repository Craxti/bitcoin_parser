from Client import Client
from Scraper import Scraper
from MongoDB import MongoDB
import asyncio 
import shelve

def get_last_page_id():
    with shelve.open('last_page') as state:
        return state.get('last_page_id', 1)

def save_last_page_id(idx = 1):
    with shelve.open('last_page') as state:
        state['last_page_id'] = idx

    return True

async def get_max_page():
    async with Client() as client:
        html = await client.get_page_with_questions_html(1, print_msg=False)
        return int(Scraper.get_max_page(html))

async def main():
    max_pages = (await get_max_page()) + 1
    last_page = get_last_page_id()
    db = MongoDB()

    print('[!] Всего страниц: ', max_pages)
    print('[!] Последняя страница: ', last_page)

    async with Client() as client: 
        for page_id in range(last_page, max_pages):
            try:
                await asyncio.sleep(4)
                page = await client.get_page_with_questions_html(page_id)
                questions_links = Scraper.get_all_questionslink_from_page(page)
                questions_html = await client.get_questions_html(questions_links)
                questions = Scraper.find_questions_btc_addres(questions_html)
                db.insert_unique_many(questions)
                save_last_page_id(page_id)
            except Exception as ex:
                print(f'[!!] Произошла ошибка {ex}')

if __name__ == '__main__':
    print('[!] Запускаю парсинг...')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()