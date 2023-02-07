from lxml import etree
from io import StringIO
from parsers.bitinfocharts_parser.utils import get_first_from_list, remove_wallet_text
from parsers.bitinfocharts_parser.MongoDB import Model
from dataclasses import asdict


class Scraper:
    ITEM_XPATH = '//tr/td[2]/a/../../../tr'
    WALLET_SIGNATURE_XPATH = './/small/a'
    BTC_XPATH = './td[2]/a'

    def __init__(self, pages):
        self.pages = pages 
        self.wallet_with_number_count = 0
        self.wallet_with_empty_count = 0
        self.wallet_correct_count = 0
        self.wallet_all = 0

    def extract_data_from_page(self, page):
        data = []
        
        parser = etree.HTMLParser()
        tree  = etree.parse(StringIO(page), parser)
        all_elements = tree.xpath(Scraper.ITEM_XPATH)
        self.wallet_all += len(all_elements)

        for item in all_elements:
            wallet_el = get_first_from_list(item.xpath(Scraper.WALLET_SIGNATURE_XPATH))
            
            if wallet_el is not None:

                wallet_text = remove_wallet_text(wallet_el.text)
                wallet_link = config.DOMAIN + wallet_el.attrib['href'] 

                if wallet_text.isdecimal():
                    self.wallet_with_number_count += 1
                    continue
                else:
                    self.wallet_correct_count += 1

                btc_el = get_first_from_list(item.xpath(Scraper.BTC_XPATH))
                btc_text = btc_el.text
                btc_link = btc_el.attrib['href']

                data.append(asdict(Model(wallet_text, wallet_link, btc_text, btc_link)))
            else:
                self.wallet_with_empty_count += 1
        
        return data
        
    def get_all_data(self):
        data = []
        
        for page in self.pages:
            for item in self.extract_data_from_page(page):
                if item: data.append(item)

        return data