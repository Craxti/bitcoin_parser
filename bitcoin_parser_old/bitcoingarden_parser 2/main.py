from src.parser import Parser
from src.browser import Browser
from src.db import MongoDB

from src.log import log

def main():
    browser = Browser(is_firefox=False)
    try:
        db = MongoDB()
        parser = Parser(browser, db)
        parser.start()
    except Exception as ex:
        log.error(f'[!] Ошибка | {ex}')
    finally:
        browser.destroy()

if __name__ == '__main__':
    main()
