import requests
from bs4 import BeautifulSoup
import re

class BlogContent:

    def get_content(self, url):
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

            content_element = iframe_soup.find('div', class_='se-main-container')
            title_element = iframe_soup.find('div', class_='se-module se-module-text se-title-text')

            if content_element and title_element:
                # 모든 텍스트 내용을 추출합니다
                text_content = content_element.get_text(strip=True, separator='\n')
                text_title = title_element.get_text(strip=True)
                
                # 줄바꿈을 정리합니다
                text_content = re.sub(r'\n+', ' ', text_content)  # 연속된 줄바꿈을 공백으로 대체합니다
                
                # 문장 단위로 분리하고 다시 조합합니다
                sentences = re.split(r'(?<=[.!?])\s+', text_content)
                formatted_content = '\n\n'.join(sentences)
                
                # 제목과 본문 내용을 반환합니다
                return text_title, formatted_content
            else:
                return None, None