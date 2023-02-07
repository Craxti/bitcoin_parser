import re

def get_first_from_list(l): 
    return l[0] if l else None

def find_bitcoin_address(text):
    BTC_REGEXP = r'\b(bc(0([ac-hj-np-z02-9]{39}|[ac-hj-np-z02-9]{59})|1[ac-hj-np-z02-9]{8,87})|[13][a-km-zA-HJ-NP-Z1-9]{25,55})\b'
    addresses = []

    for item in re.findall(BTC_REGEXP, text):
        try:
            addresses.append(item[0])
        except (ValueError, IndexError):
            pass

    return addresses