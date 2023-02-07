import re

def get_first_from_list(l): 
    return l[0] if l else None

def remove_wallet_text(text):
    return re.sub('wallet:', '', text, flags = re.IGNORECASE).strip()