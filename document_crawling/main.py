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
from qna import qna_link, qna
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
    donga_news_content_instance = blog.donga.content_crawling.ContentCrawling()

    # 5. qna 인스턴스 생성
    qna_link_instance = qna.qna_link.QnALink()
    qna_crawl_data_instance = qna.qna.QnACrawler()

    # 6. 프로퍼티 인스턴스 생성
    properties_instance = properties.Properties()

    # ㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡ
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

    # 10. 모델 가져오기
    model, system_prompt = properties_instance.model()

    #11. 삽입 쿼리문
    insert_query = """
    INSERT INTO blog_summary (document_no, title, url, behavior, analysis, solution, behavior_emb, behavior_analysis_emb)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    # ㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡ
    # 12. 임베딩 및 데이터 삽입
    def process_content(idx, title, content, url):

        content_data = f"제목: {title}\n내용: {content}"
        
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": content_data}
            ],
            temperature=0,
        )

        summary = response.choices[0].message.content

        if "아이의 문제행동:" in summary:
            problem_behavior, behavior_analysis, solution = parse_summary(summary)
            behavior_emb = create_embedding(problem_behavior)
            behavior_plus_analysis_emb = create_embedding(problem_behavior + "," + behavior_analysis)
            
            cursor.execute(insert_query, (
                idx, title, url, problem_behavior, behavior_analysis, solution,
                behavior_emb, behavior_plus_analysis_emb
            ))
        else:
            cursor.execute(insert_query, (
                idx, title, url, None, None, summary.strip(), None, None
            ))
        
        connection.commit()

    # 13. 요약 파싱
    def parse_summary(summary):

        parts = summary.split("아이의 문제행동:")[1].split("문제행동 분석:")
        problem_behavior = parts[0].strip()
        behavior_analysis, solution = parts[1].split("해결방안:")

        return problem_behavior, behavior_analysis.strip(), solution.strip()

    # 14. 임베딩 생성
    def create_embedding(text):
        return embedding_client.embeddings.create(
            model="solar-embedding-1-large-passage",
            input=text
        ).data[0].embedding
    
    # ㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡ
    # 1. 블로그 내용 크롤링 작성
    bblog_links = bblog_link_instance.get_link()

    for idx, url in enumerate(bblog_links, start=1):
        blog_link, title, content = bblog_content_instance.get_content(url)

        if title and content:
            process_content(idx, title, content, blog_link)

    # 2. 화해 크롤링 작성
    blog_posts = make_up_blog_link_instance.get_post_links()
    news_links = make_up_link_crawling_instance.news_link(blog_posts)

    for idx, url in enumerate(news_links, start=idx + 1):
        title, content = make_up_news_content_instance.news_crawling(url)
        if title and content:
            process_content(idx, title, content, url)

    # 3. 토닥 크롤링 작성
    todak_posts = todak_blog_link_instance.get_post_links()
    todak_links = todak_link_crawling_instance.news_link(todak_posts)

    for idx, url in enumerate(todak_links, start=idx + 1):
        title, content = todak_news_content_instance.news_crawling(url)
        if title and content:
            process_content(idx, title, content, url)

    # 4. 동아 크롤링 작성
    donga_posts = donga_blog_link_instance.links()
    donga_links = donga_link_crawling_instance.news_link(donga_posts)

    for idx, url in enumerate(donga_links, start=idx + 1):
        title, content = donga_news_content_instance.news_crawling(url)
        if title and content:
            process_content(idx, title, content, url)

    # 5. QnA 크롤링 작성
    qna_links = qna_link_instance.get_links()

    for idx, url in enumerate(qna_links, start=idx + 1):
        title, content = qna_crawl_data_instance.crawl_data(url)
        if title and content:
            process_content(idx, title, content, url)
    
    # 6. 데이터베이스 연결 종료
    connection.close()
    print("크롤링 및 요약 결과가 데이터베이스에 저장되었습니다.")

if __name__ == "__main__":
    main()

        

    









