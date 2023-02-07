Парсер сайта: 
https://www.reddit.com/r/Bitcoin/

Проходимся по каждому сабреддиту, и находим в постах биткоин адреса, если они найдены и уникальны в бд, добавляем.

Поля которые получаем:
1) btc_address (:btc_address_1, :btc_address_2, ...)
2) url
3) topic
4) text
5) name
6) asked_at
7) timestamp

База данных: MongoDB

Перед запуском:
1) pip install -r requirements.txt
2) python main.py