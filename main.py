import multiprocessing

from parsers.GIT_parser import Main as Git_parser
from parsers.Bitcoin_com import Bitcoin_com
from parsers.bitinfocharts_parser.main import Main as B_charts
from parsers.bitcointalk_parser_2.main import Main as B_talk2
from parsers.bitcoingarden_parser_2.main import Main as B_garden
from parsers.bitcoin_stackexchange_parser.main import Main as St_exchange
from parsers.bitco_forum.main import Main as Bit_forum
from parsers.bitco_forum.main11 import Main as Bit_forum2
from parsers.bitcoinforum_com.parser import Main as Bf_com
from parsers.forum_bitcoin_com_ import main as bitocin_com
from parsers.git_parser_crypc import parserSearchResultCrypt as git_pars
from parsers.keybase_3.keybase import middlewares as key_base
from parsers.Parser_bitalk_org import main as Parser_bitalk_org
from parsers.Parser_coinforum_de import main as Parser_coinforum_de
from parsers.Parser_cryptoforum_com import main_cryptoforum_com as Parser_cryptoforum_com
from parsers.parser_forum_bits_media import main as parser_forum_bits_media
from parsers.quora_parser import main as quora_parser
from parsers.reddit_parser import main as reddit_parser
from parsers.yandex_parser import main as yandex_parser
def Factory(res):
    factory_di = {
        'bitocin_com': bitocin_com,
        'git_pars': git_pars,
        'key_base': key_base,
        'Parser_bitalk_org': Parser_bitalk_org,
        'Parser_coinforum_de': Parser_coinforum_de,
        'Parser_cryptoforum_com': Parser_cryptoforum_com,
        'parser_forum_bits_media': parser_forum_bits_media,
        'quora_parser': quora_parser,
        'reddit_parser': reddit_parser,
        'yandex_parser': yandex_parser,
        'bitcoin': Git_parser,
        'crypto': Git_parser,
        'bitcoin_com': Bitcoin_com,
        'b_charts': B_charts,
        'b_talk2': B_talk2,
        'b_garden': B_garden,
        'St_exchange': St_exchange,
        'bit_forum': Bit_forum,
        'bit_forum2': Bit_forum2,
        'bitcoin_forum': Bf_com,
    }
    if res == 'all':
        return factory_di
    else:
        return factory_di.get(res)


class Super_cls:

    def run(self):
        res = input('запуск всех- all,  одного любое значение\n')
        if res in ['all', 'a', 'все']:
            di = Factory('all')
            for key, val in di.items():
                if key in ['bitcoin', 'crypto']:
                    proc = multiprocessing.Process(target=self.cls_pack, args=(val, key))
                    proc.start()
                else:
                    proc = multiprocessing.Process(target=self.cls_pack, args=(val,))
                    proc.start()
        else:
            clss = Factory(res)
            if clss:
                p_res = clss()
                p_res.run()
            else:
                print('неверно введено значение')
    @staticmethod
    def cls_pack(cls, val=None):
        if val:
            p_res = cls(val)
        else:
            p_res = cls()
        p_res.run()


if __name__ == '__main__':
    m = Super_cls()
    m.run()