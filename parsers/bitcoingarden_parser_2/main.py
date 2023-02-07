from parsers.bitcoingarden_parser_2.src.parser import Parser
from parsers.bitcoingarden_parser_2.src.browser import Browser
from parsers.bitcoingarden_parser_2.src.db import MongoDB

from parsers.bitcoingarden_parser_2.src.log import log

class Main:
    def run(self):
        browser = Browser(is_firefox=False)
        try:
            db = MongoDB()
            parser = Parser(browser, db)
            parser.start()
        except Exception as ex:
            log.error(f'[!] Ошибка | {ex}')
        finally:
            browser.destroy()

