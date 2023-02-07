import scrapy


class User(scrapy.Item):
    btc = scrapy.Field()
    domain = scrapy.Field()
    link = scrapy.Field()
    nic = scrapy.Field()
    avatar = scrapy.Field()
    date_of_registration = scrapy.Field()
    last_active = scrapy.Field()
    icq = scrapy.Field()
    aim = scrapy.Field()
    msn = scrapy.Field()
    email = scrapy.Field()
    website = scrapy.Field()
    gender = scrapy.Field()
    age = scrapy.Field()
    location = scrapy.Field()
    description = scrapy.Field()
    name = scrapy.Field()
    signature = scrapy.Field()
    timestamp = scrapy.Field()
