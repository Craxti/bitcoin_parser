import threading
from queue import Queue
import signal
from src.log import log
from src.browser import Browser
from src.parser import Parser
from src.database import MongoDB


def main():
	browser_scrolling = Browser(is_firefox=False, is_headless=False)
	browser_page = Browser(is_firefox=False, is_headless=False)
	database = MongoDB()

	posts_queue = Queue()
	run_event = threading.Event()

	def exit(signum, frame):
		raise SystemExit
		run_event.set()
		browser_scrolling.destroy()
		browser_page.destroy()

	try:
		signal.signal(signal.SIGINT, exit)
		parser = Parser(browser_scrolling, browser_page, posts_queue, run_event, database)
		scrape_post_thread = threading.Thread(target=parser.scrape_posts)
		scrape_post_thread.start()
		parser.start_scrolling()
	except KeyboardInterrupt:
		log.info('Выход из парсера.')
	except Exception as ex:
		log.fatal(f'[СRITICAL_ERROR] Произошла ошибка при парсинге: {ex}')
	finally:
		run_event.set()
		browser_scrolling.destroy()
		browser_page.destroy()

if __name__ == '__main__':
	main()