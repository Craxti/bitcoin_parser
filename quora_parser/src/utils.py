import shelve
import config
import re

def get_last_post():
    with shelve.open(config.STORE_PATH) as states:
        return states.get('last_post', (None, 0))

def save_last_post(last_id, count):
    with shelve.open(config.STORE_PATH) as states:
        states['last_post'] = (last_id, count)

    return True

def find_bitcoin_address(text):
    BTC_REGEXP = r'\b(bc(0([ac-hj-np-z02-9]{39}|[ac-hj-np-z02-9]{59})|1[ac-hj-np-z02-9]{8,87})|[13][a-km-zA-HJ-NP-Z1-9]{25,55})\b'
    addresses = []

    for item in re.findall(BTC_REGEXP, text):
        try:
            addresses.append(item[0])
        except (ValueError, IndexError):
            pass

    return addresses