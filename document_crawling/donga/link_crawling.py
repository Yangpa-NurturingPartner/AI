import requests
from bs4 import BeautifulSoup

class LinkCrawling:

    def link(self):

        # 변수 선언
        links = []
 

        url = f'https://www.donga.com/news/Series/70040100000217?p=1&prod=news&ymd=&m='

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }

        response = requests.get(url, headers=headers)
        crawling = BeautifulSoup(response.text, 'html.parser')

        news_parts = crawling.find_all('article', class_='news_card')

        for card in news_parts:
            link = card.find('a')
            
            if link and 'href' in link.attrs:
                links.append(link['href'])
            else:
                print("링크를 찾을 수 없습니다.")



        return links
