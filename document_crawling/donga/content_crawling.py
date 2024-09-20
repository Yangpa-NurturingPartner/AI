import requests
from bs4 import BeautifulSoup

class ContentCrawling:

    def content(self, url):

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }

        response = requests.get(url, headers=headers)
        crawling = BeautifulSoup(response.text, 'html.parser')

        # 기사 제목 추출
        title = crawling.find('section', class_='head_group').find('h1').text.strip()

        # "[오은영의 부모마음 아이마음]" 부분 제거
        title = title.replace("[오은영의 부모마음 아이마음]", "").strip()

        # print(f"제목: {title}")

        # 기사 본문 추출
        content = crawling.find('section', class_='news_view')

        if content:
            # 불필요한 요소 제거
            for element in content(['figure', 'div', 'script']):
                element.decompose()
            
            # 남은 텍스트 추출 및 정리
            text = content.get_text(strip=True)
            
            # <br> 태그를 기준으로 문단 나누기
            paragraphs = text.split('오은영 정신건강의학과 전문의·오은영 소아청소년클리닉 원장')[0].split('br')
            
            full_content = f"제목: {title}\n\n"

            for paragraph in paragraphs:
                cleaned_paragraph = paragraph.strip()
                if cleaned_paragraph:
                    full_content += cleaned_paragraph + '\n\n'

            return full_content
        
        else:
            return "기사 본문을 찾을 수 없습니다."
