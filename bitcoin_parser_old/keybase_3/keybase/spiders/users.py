import scrapy
import logging
import re
from keybase.items import User
import time


class userSpider(scrapy.Spider):
    name = 'users'
    start_urls = ['https://keybase.io/persiafighter']


    def parse(self, response):
        item = User()

        item['domain'] = 'https://keybase.io'
        item['link'] = response.url
        item['nic'] = response.url.replace('https://keybase.io/', '')
        item['timestamp'] = time.time()

        avatar = response.css('.user-profile-picture img').attrib['src']
        avatar = None if avatar == '/images/no-photo/placeholder-avatar-180-x-180@2x.png' else avatar
        item['avatar'] = avatar

        contacts = response.css('.identity-table .it-item')
        for contact in contacts:
            icon = contact.css('.it-icon::attr(srcset)').get()
            if 'services/web' in str(icon):
                website = contact.css('a').attrib['href']
                item['website'] = website

            if 'services/web' in str(icon):
                website = contact.css('a').attrib['href']
                item['website'] = website

        description = response.css('.profile-heading .bio::text')
        if description:
            item['description'] = description.get().strip().replace('\n', '')

        location = response.css('.profile-heading .location::text')
        if location:
            item['location'] = location.get().strip()

        name = response.xpath("//div[@class='full-name ']/text()").extract()
        if name:
            item['name'] = name[1].strip()

        item['date_of_registration'] = ''
        item['last_active'] = ''
        item['icq'] = ''
        item['aim'] = ''
        item['msn'] = ''
        item['email'] = ''
        item['gender'] = ''
        item['age'] = ''
        item['signature'] = ''

        # если есть адрес биткоина, то сохраняем в бд
        btc = response.css('a.currency-address')
        if btc:
            if btc.attrib['data-type'] == 'bitcoin':
                item['btc'] = btc.attrib['data-address']
                yield item

        # ищем ссылки на других пользователей   
        next_urls = response.css('#profile-tracking-section .follower-row .td-follower-info a::attr(href)').getall()
        if next_urls:
            for url in set(next_urls):
                yield response.follow(f"{item['domain']}{url}", callback=self.parse)
