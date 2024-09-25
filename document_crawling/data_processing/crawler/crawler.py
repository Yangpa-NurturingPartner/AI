from document.blog.blog_content.blog_link import BlogLink
from document.blog.blog_content.blog_content import BlogContent
from document.blog.news_make_up.blog_link import BlogLink as MakeUpBlogLink
from document.blog.news_make_up.link_crawling import LinkCrawling as MakeUpLinkCrawling
from document.blog.news_make_up.news_content import NewsContent as MakeUpNewsContent
from document.blog.news_todak.blog_link import BlogLink as TodakBlogLink
from document.blog.news_todak.link_crawling import LinkCrawling as TodakLinkCrawling
from document.blog.news_todak.news_content import NewsContent as TodakNewsContent
from document.donga.link_crawling import LinkCrawling as DongaLinkCrawling
from document.donga.content_crawling import ContentCrawling as DongaContentCrawling
from document.qna.qna_link import QnALink
from document.qna.qna import QnACrawler

class Crawler:
    
    # def crawl_blog(self):

    #     # 1. 블로그 내용 인스턴스 생성
    #     bblog_link_instance = BlogLink()
    #     bblog_content_instance = BlogContent()

    #     bblog_links = bblog_link_instance.get_link()

    #     results = []
    #     for idx, url in enumerate(bblog_links, start=1):
    #         title, content, blog_url = bblog_content_instance.get_content(url)
    #         url = blog_url
    #         if title and content:
    #             results.append((idx, title, url, content))
    #     return results

    # def crawl_make_up(self):

    #     # 2. 화해 인스턴스 생성
    #     make_up_blog_link_instance = MakeUpBlogLink()
    #     make_up_link_crawling_instance = MakeUpLinkCrawling()
    #     make_up_news_content_instance = MakeUpNewsContent()

    #     blog_posts = list(make_up_blog_link_instance.get_post_links())
    #     news_links = make_up_link_crawling_instance.news_link(blog_posts)

    #     results = []

    #     for idx, url in enumerate(news_links, start=1):
    #         title, content = make_up_news_content_instance.news_crawling(url)
    #         if title and content:
    #             results.append((idx, title, url, content))

    #     return results

    # def crawl_todak(self):

    #     # 3. 토닥 인스턴스 생성
    #     todak_blog_link_instance = TodakBlogLink()
    #     todak_link_crawling_instance = TodakLinkCrawling()
    #     todak_news_content_instance = TodakNewsContent()

    #     todak_posts = list(todak_blog_link_instance.get_post_links())
    #     todak_links = todak_link_crawling_instance.news_link(todak_posts)

    #     results = []

    #     for idx, url in enumerate(todak_links, start=1):
    #         title, content = todak_news_content_instance.news_crawling(url)
    #         if title and content:
    #             results.append((idx, title, url, content))

    #     return results

    # def crawl_donga(self):

    #     # 4. 동아 인스턴스 생성
    #     donga_link_crawling_instance = DongaLinkCrawling()
    #     donga_news_content_instance = DongaContentCrawling()

    #     donga_links = donga_link_crawling_instance.link()

    #     results = []

    #     for idx, url in enumerate(donga_links, start=1):
    #         title, content = donga_news_content_instance.content(url)
    #         if title and content:
    #             results.append((idx, title, url, content))

    #     return results

    def crawl_qna(self):

        # 5. qna 인스턴스 생성
        qna_link_instance = QnALink()
        qna_crawl_data_instance = QnACrawler()

        qna_links = qna_link_instance.get_links()

        results = []

        for idx, url in enumerate(qna_links, start=1):
            title, content = qna_crawl_data_instance.crawl_data(url)
            if title and content:
                results.append((idx, title, url, content))

        return results

    def crawl_all(self):
        results = []
        # try:
        #     results.extend(self.crawl_blog())
        # except Exception as e:
        #     print(f"crawl_blog에서 오류 발생: {e}")

        # try:
        #     results.extend(self.crawl_make_up())
        # except Exception as e:
        #     print(f"crawl_make_up에서 오류 발생: {e}")

        # try:
        #     results.extend(self.crawl_todak())
        # except Exception as e:
        #     print(f"crawl_todak에서 오류 발생: {e}")

        # try:
        #     results.extend(self.crawl_donga())
        # except Exception as e:
        #     print(f"crawl_donga에서 오류 발생: {e}")

        try:
            results.extend(self.crawl_qna())
        except Exception as e:
            print(f"crawl_qna에서 오류 발생: {e}")

        return results
    
