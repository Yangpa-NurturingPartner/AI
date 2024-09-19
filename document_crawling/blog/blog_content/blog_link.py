import requests
from bs4 import BeautifulSoup
import json


class BlogLink:

    def get_link(self):

        max_pages = 76

        i = 1

        while i <= max_pages:

            url = f"https://blog.naver.com/PostList.naver?from=postList&blogId=eyohlovec&categoryNo=1&currentPage={i}"

            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')

            blog_link = soup.find('div', id='socialPluginInfoJson')

            if blog_link:
                posts_data = json.loads(blog_link.string)

                i += 1

                # 각 포스트의 링크를 yield합니다
                for post in posts_data:
                    yield post['source']

            else:
                break  # 더 이상 포스트가 없으면 루프 종료


