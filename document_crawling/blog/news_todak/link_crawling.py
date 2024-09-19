import requests
from bs4 import BeautifulSoup

class LinkCrawling:

    def news_link(self, url):    
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
                return link.get('href')  # 링크의 href 속성만 반환
            else:
                return None  # 링크를 찾지 못한 경우 None 반환

        return None  # iframe을 찾지 못한 경우 None 반환