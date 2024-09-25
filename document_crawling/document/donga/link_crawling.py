import requests
from bs4 import BeautifulSoup

class LinkCrawling:

    def link(self):

        # 변수 선언
        links = []

        i = 0

        while i < 197:

    
            url = f'https://www.donga.com/news/Series/70040100000217?p={i}&prod=news&ymd=&m='

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

            i += 15

        return links
