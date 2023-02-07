Парсер сайта: 
https://bitcoin.stackexchange.com/questions

Проходимся по каждому вопросу, и находим в них биткоин адресс, если он найден и уникален в бд, добавляем.

Поля которые получаем:
1) btc_address (:btc_address_1, :btc_address_2, ...)
2) url
3) topic
4) text
5) asked_at
6) name

База данных: MongoDB

Перед запуском:
1) pip install -r requirements.txt
2) python main.py