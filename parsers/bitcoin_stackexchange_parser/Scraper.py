from bs4 import BeautifulSoup
from utils import get_first_from_list, find_bitcoin_address
from parsers.bitcoin_stackexchange_parser import config

class Scraper:
    PAGE_MAX_CSS = 'a.s-pagination--item.js-pagination-item'
    QUESTIONS_PATH_CSS = '#questions .s-link'
    QUESTION_TOPIC_CSS = '#question-header .question-hyperlink'
    QUESTION_TEXT_CSS = '.js-post-body'
    QUESTION_ASKED_AT_CSS = 'time'
    QUESTION_NAME_CSS = '.user-details a'

    @classmethod
    def get_max_page(cls, html):
        soup = BeautifulSoup(html, 'html.parser')
        return soup.select(Scraper.PAGE_MAX_CSS)[4].get_text()

    @classmethod
    def get_all_questionslink_from_page(cls, html):
        try:
            soup = BeautifulSoup(html, 'html.parser')
            questions = soup.select(cls.QUESTIONS_PATH_CSS)
            return [question.get('href') for question in questions]
        except Exception as ex:
            print('Ошибка при парсинге ссылок со страницы вопросов ', ex)
            return []

    @classmethod
    def find_questions_btc_addres(cls, questions_html):
        data = []
        for url, html in questions_html:
            try:
                soup = BeautifulSoup(html, 'html.parser')
                text = get_first_from_list(soup.select(Scraper.QUESTION_TEXT_CSS))

                topic = get_first_from_list(soup.select(Scraper.QUESTION_TOPIC_CSS)).get_text()
                text = text.get_text().strip() if text else None
                name = get_first_from_list(soup.select(Scraper.QUESTION_NAME_CSS)).get_text()
                asked_at = get_first_from_list(soup.select(Scraper.QUESTION_ASKED_AT_CSS))['datetime']
                btc_addresses = find_bitcoin_address(text) 

                if btc_addresses:
                    data.append({
                        'text': text,
                        'topic': topic,
                        'name': name,
                        'asked_at': asked_at,
                        'btc_addresses': btc_addresses,
                        'url': url
                    })
            except Exception as ex:
                print('Парсинге вопроса: ', ex)

        return data