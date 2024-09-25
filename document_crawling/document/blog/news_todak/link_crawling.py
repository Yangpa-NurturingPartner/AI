import requests
from bs4 import BeautifulSoup

class LinkCrawling:

    def news_link(self, urls):    
        news_links = []
        
        for url in urls:
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')

            # iframe의 src 속성 찾기
            iframe = soup.find('iframe', id='mainFrame')
            if iframe and 'src' in iframe.attrs:
                iframe_src = iframe['src']
                # iframe의 src가 상대 경로인 경우 완전한 URL로 만들기
                if iframe_src.startswith('/'):
                    iframe_src = f"https://blog.naver.com{iframe_src}"

                # iframe 내용 가져오기
                iframe_response = requests.get(iframe_src)
                iframe_soup = BeautifulSoup(iframe_response.text, 'html.parser')

                link = iframe_soup.find('a', class_='se_og_box __se_link')

                if link:
                    news_link = link.get('href')
                    news_links.append(news_link)  # 링크의 href 속성만 추가
                else:
                    print("No link found in iframe")  # 링크를 찾지 못한 경우 출력
                    news_links.append(None)  # 링크를 찾지 못한 경우 None 추가
            else:
                print("No iframe found")  # iframe을 찾지 못한 경우 출력
                news_links.append(None)  # iframe을 찾지 못한 경우 None 추가

        return news_links