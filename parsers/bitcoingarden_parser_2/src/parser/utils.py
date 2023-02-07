import shelve
import re

from parsers.bitcoingarden_parser_2.src.config import STORE_FILE

def get_last_member_id():
    with shelve.open(STORE_FILE) as states:
        return states.get('last_member_idx', 1)

def save_last_member_id(idx = 1):
    with shelve.open(STORE_FILE) as states:
        states['last_member_idx'] = idx

    return True

def find_bitcoin_addresses(text):
    '''Ищем все биткоин адрессы в полученном тексте'''
    BTC_REGEXP = r'\b(bc(0([ac-hj-np-z02-9]{39}|[ac-hj-np-z02-9]{59})|1[ac-hj-np-z02-9]{8,87})|[13][a-km-zA-HJ-NP-Z1-9]{25,55})\b'
    addresses = []

    for item in re.findall(BTC_REGEXP, text):
        try:
            addresses.append(item[0])
        except (ValueError, IndexError):
            pass

    return addresses