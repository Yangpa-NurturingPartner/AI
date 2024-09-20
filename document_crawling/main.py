# 0. 블로그 임포트
import blog
# 1. 프로퍼티 임포트
from properties import properties
# 2. 블로그 내용 크롤링
from blog.blog_content import blog_link, blog_content
# 3. 화해 크롤링
from blog.news_make_up import blog_link, link_crawling, news_content
# 4. 토닥 크롤링
from blog.news_todak import blog_link,link_crawling,news_content
# 5. 동아 크롤링
from donga import blog_link, link_crawling, news_content
# 6. qna 크롤링
from qna import crawl_data
# 7. api 크롤링 
import properties
# 8. openai 임포트
import openai




def main():

    # 1.블로그 내용 인스턴스 생성
    bblog_link_instance = blog.blog_content.blog_link.BlogLink()
    bblog_content_instance = blog.blog_content.blog_content.BlogContent()

    # 2. 화해 인스턴스 생성
    make_up_blog_link_instance = blog.news_make_up.blog_link.BlogLink()
    make_up_link_crawling_instance = blog.news_make_up.link_crawling.LinkCrawling()
    make_up_news_content_instance = blog.news_make_up.news_content.NewsContent()

    # 3. 토닥 인스턴스 생성
    todak_blog_link_instance = blog.news_todak.blog_link.BlogLink()
    todak_link_crawling_instance = blog.news_todak.link_crawling.LinkCrawling()
    todak_news_content_instance = blog.news_todak.news_content.NewsContent()

    # 4. 동아 인스턴스 생성
    donga_blog_link_instance = blog.donga.blog_link.BlogLink()  
    donga_link_crawling_instance = blog.donga.link_crawling.LinkCrawling()
    donga_news_content_instance = blog.donga.news_content.NewsContent()

    # 5. qna 인스턴스 생성
    qna_crawl_data_instance = blog.qna

    # 6. 프로퍼티 인스턴스 생성
    properties_instance = properties.Properties()

    # 7. sql 연결 
    connection = properties_instance.sql()
    cursor = connection.cursor()

    # 8. api key 가져오기
    embedding_api_key, client = properties_instance.api_key()

    # 9. 임베딩 모델 사용

    embedding_client = OpenAI(
    api_key= embedding_api_key,
    base_url="https://api.upstage.ai/v1/solar"
    )

    # 9. 모델 가져오기
    model, system_prompt = properties_instance.model()




    # 1. 블로그 내용 크롤링 작성
    bblog_links = bblog_link_instance.get_link()

    bcrawled_data = []

    for url in bblog_links:
        title, content = blog_content.BlogContent().get_content(url)

        if title and content:
            content_data = f"제목: {title}\n내용: {content}"
            bcrawled_data.append({"제목": title, "내용": content_data})

        for data in bcrawled_data:
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": data["내용"]}
            ]

            response = client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=0,
            )

    









