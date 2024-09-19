import blog_link
import link_crawling
import news_content

def main():
    # 0. BlogLink 클래스의 인스턴스를 생성
    blog_link_instance = blog_link.BlogLink()

    #1. 오은영의 화해 카테고리 블로그 글들 링크 크롤링
    blog_posts = blog_link_instance.get_post_links()

    #2. 블로그 글에서 뉴스 링크 크롤링 
    link_crawling_instance = link_crawling.LinkCrawling()
    for blog_url in blog_posts:  # blog_posts가 이제 URL 문자열의 리스트입니다
        news_url = link_crawling_instance.news_link(blog_url)
        
        if news_url:
            #3. 뉴스 내용 크롤링
            title, content = news_content.NewsContent.news_crawling(news_url)
            
            print("제목:", title)
            print("내용:", content)
            print("-------------------")  # 각 기사 사이에 구분선을 추가합니다

if __name__ == "__main__":
    main()
