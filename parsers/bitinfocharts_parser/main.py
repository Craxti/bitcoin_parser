from parsers.bitinfocharts_parser.Client import Client
from parsers.bitinfocharts_parser.Scraper import Scraper
from parsers.bitinfocharts_parser.MongoDB import MongoDB
import asyncio 

async def main():
    db = MongoDB()
    try:
        async with Client() as client: 
            print('[!] 1) Делаю запросы на все страницы...')
            pages = await client.get_page_contents()

            print('[!] 2) Получаю всю информацию со страниц...')
            scraper = Scraper(pages)
            data = scraper.get_all_data()

            print('[!] 3) Сохраняю новые записи в базу данных...')
            db.insert_unique_many(data)

            print('[+] Парсинг адресов завершен: ')
            print('==== Получено страниц: ', client.get_pages_count)
            print('==== Получено кошельков: ', scraper.wallet_all)
            print('==== Корректные кошельки: ', scraper.wallet_correct_count) 
            print('==== Кошельки с цифрами: ', scraper.wallet_with_number_count)
            print('==== Пустые кошельки: ', scraper.wallet_with_empty_count)
            print('==== Новые записи в бд: ', db.new_added_count)
    except Exception as ex:
        print(f'[!] Ошибка {ex}')


class Main:
    
    def run(self):
        print('[!] Запускаю парсинг...')
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
        loop.close()