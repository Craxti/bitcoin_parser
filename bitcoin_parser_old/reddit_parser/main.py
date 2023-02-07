import asyncio
from Client import Client
from Database import MongoDB
from errors import AuthError
from utils import get_last_post
from log import log

async def main():
	try:
		db = MongoDB()
		async with Client() as client:
			while True:
				if client.is_ended:
					log.info('Все посты получены!')
					break

				data = await client.get_posts()
				db.insert_unique_many(data)

				get_posts_count = get_last_post()[1]
				print(f'Всего постов обработано: {get_posts_count}\nНовых записей в бд: {db.new_added_count}')
	except KeyboardInterrupt:
		log.info('Выход из парсера.')
	except Exception as ex:
		log.fatal(f'[СRITICAL_ERROR] Произошла ошибка при парсинге: {ex}')

if __name__ == '__main__':
	asyncio.run(main())