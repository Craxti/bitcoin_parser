import requests
from bs4 import BeautifulSoup
import pymongo
from pymongo import MongoClient
import certifi
ca = certifi.where()

cluster = MongoClient("mongodb+srv://localhost:27017@cluster0.i1galhp.mongodb.net/?retryWrites=true&w=majority", tlsCAFile=ca)
db = cluster["test"]
collection = db["test"]

url = "https://forum.bits.media/index.php?/online/&filter=group_3"
link_list = list()

def main(url):
	global link_list, collection
	page = requests.get(url)
	soup = BeautifulSoup(page.text, 'lxml')

	users = soup.find_all('a', class_="ipsUserPhoto ipsUserPhoto_mini")
	for user in users:
		link = user.get('href')
		link_list.append(link)

	for get in link_list:
		account = requests.get(get)
		soup_2 = BeautifulSoup(account.text, 'lxml')
		try:
			new_users = soup_2.find('ul', class_="ipsDataList ipsDataList_reducedSpacing ipsSpacer_top").find_all('li', class_="ipsDataItem")

			for new_user in new_users:
				link_new_user = new_user.find_all('a')[0].get('href')
				if not (link_new_user in link_list):
					link_list.append(link_new_user)
		except Exception as e:
			pass

		try:
			sub_users = soup_2.find_all('li', class_="ipsGrid_span3 ipsType_center")
			for sub_user in sub_users:
				link_sub_user = sub_user.find('a').get('href')
				if not (link_sub_user in link_list):
					link_list.append(link_sub_user)
		except Exception as e:
			pass

		try:
			sub_link = soup_2.find('p', class_="ipsType_right ipsType_reset ipsPad_half ipsType_small ipsType_light ipsAreaBackground_light").find('a').get('href')
			sub_page = requests.get(sub_link)
			soup_3 = BeautifulSoup(sub_page.text, 'lxml')

			new_sub_users_1 = soup_3.find_all('a', class_="ipsUserPhoto ipsUserPhoto_tiny")
			for new_sub_user_1 in new_sub_users_1:
				link_new_sub_user_1 = new_sub_user_1.get('href')
				if not (link_new_sub_user_1 in link_list):
					link_list.append(link_new_sub_user_1)
		except Exception as e:
			pass

		try:
			strong = soup_2.find('strong', string="Bitcoin кошелек").text

			try:
				try:
					strongs = soup_2.find_all('div', attrs={'data-location':'customFields'})[-1].find_all('strong')
				except Exception as e:
					strongs = soup_2.find_all('div', attrs={'data-location':'customFields'})[-1].find('strong')
			except Exception as e:
				try:
					strongs = soup_2.find('div', attrs={'data-location':'customFields'}).find_all('strong')
				except Exception as e:
					strongs = soup_2.find('div', attrs={'data-location':'customFields'}).find('strong')
			
			for i in range(10):
				check_s = strongs[i].text
				if check_s == strong:
					break

			try:
				Bitcoin_l = soup_2.find_all('div', attrs={'data-location':'customFields'})[-1].find_all('div', class_="ipsType_break ipsContained")[i].text
			except Exception as e:
				Bitcoin_l = soup_2.find('div', attrs={'data-location':'customFields'})[-1].find_all('div', class_="ipsType_break ipsContained")[i].text

			# Collecting data

			try:
				name = soup_2.find('div', class_="ipsPos_left ipsPad cProfileHeader_name ipsType_normal").find('h1').text
			except Exception as e:
				name = ""

			try:
				status = soup_2.find('span', class_="ipsPageHead_barText").find('span').text
			except Exception as e:
				status = ""

			try:
				posts = soup_2.find('ul', class_="ipsList_reset ipsFlex ipsFlex-ai:center ipsFlex-fw:wrap ipsPos_left ipsResponsive_noFloat").find_all('li')[0].text
			except Exception as e:
				posts = ""

			try:
				date_registered = soup_2.find('ul', class_="ipsList_reset ipsFlex ipsFlex-ai:center ipsFlex-fw:wrap ipsPos_left ipsResponsive_noFloat").find_all('li')[1].find('time').text
			except Exception as e:
				date_registered = ""

			try:
				wins = soup_2.find('ul', class_="ipsList_reset ipsFlex ipsFlex-ai:center ipsFlex-fw:wrap ipsPos_left ipsResponsive_noFloat").find_all('li')[-1].find('span').text
			except Exception as e:
				wins = ""

			try:
				g_strong = soup_2.find('strong', string="Пол").text
				for i in range(10):
					check_s = strongs[i].text
					if check_s == g_strong:
						break

				gender = soup_2.find('div', attrs={'data-location':'customFields'}).find_all('div', class_="ipsType_break ipsContained")[i].text
			except Exception as e:
				gender = ""

			try:
				t_strong = soup_2.find('strong', string="Город: ").text
				t_strongs = soup_2.find('div', attrs={'data-location':'defaultFields'}).find_all('strong')
				for i in range(10):
					check_s = t_strongs[i].text
					if check_s == t_strong:
						break

				town = soup_2.find('div', attrs={'data-location':'defaultFields'}).find_all('div', class_="ipsType_break ipsContained")[i].text
			except Exception as e:
				town = ""

			try:
				photo = soup_2.find('a', class_="ipsUserPhoto ipsUserPhoto_xlarge").get('href')
			except Exception as e:
				photo = ""

			all_links_btc = ""

			try:
				posts_ad = soup_2.find_all('span', class_="ipsType_break ipsContained")
				for post_ad in posts_ad:
					post_link = post_ad.find('a').get('href')
					all_links_btc = all_links_btc + post_link + "          "
			except Exception as e:
				pass

			post = {"name": str(" ".join(name.split())), "domain": "forum.bits.media", "link": str(" ".join(get.split())), "avatar": str(" ".join(photo.split())), "date_registered": str(" ".join(date_registered.split())), "status": str(" ".join(status.split())), "posts": str(" ".join(posts.split())),  "wins": str(" ".join(wins.split())), "gender": str(" ".join(gender.split())), "location": str(" ".join(town.split())), "bitcoin_address": str(" ".join(Bitcoin_l.split())), "all_links_btc": str(all_links_btc)}

			collection.insert_one(post)
			
			
		except Exception as e:
			print(e)

if __name__ == "__main__":
	main(url)