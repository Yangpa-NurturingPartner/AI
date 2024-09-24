import blog
from blog.blog_content import blog_link, blog_content
from blog.news_make_up import blog_link as make_up_blog_link, link_crawling as make_up_link_crawling, news_content as make_up_news_content
from blog.news_todak import blog_link as todak_blog_link, link_crawling as todak_link_crawling, news_content as todak_news_content
from donga import blog_link as donga_blog_link, link_crawling as donga_link_crawling, news_content as donga_news_content
from qna import qna_link, qna
import json

class Crawler:
    def __init__(self, cursor, connection, opensearch_client):
        self.cursor = cursor
        self.connection = connection
        self.opensearch_client = opensearch_client
        self.index_name = 'document_data'

    def process_content(self, idx, title, url):
        insert_query = """
        INSERT INTO blog_summary (document_no, title, url)
        VALUES (%s, %s, %s)
        """
        self.cursor.execute(insert_query, (idx, title, url))
        self.connection.commit()

    def index_data(self, idx, title, url):
        row = {
            'document_no': idx,
            'title': title,
            'url': url,
            'behavior_emb': json.dumps([0.0] * 4096),  # 예시 임베딩 데이터
            'behavior_plus_analysis_emb': json.dumps([0.0] * 4096)  # 예시 임베딩 데이터
        }
        self.opensearch_client.index_data(self.index_name, row)

    def crawl_blog(self):
        bblog_link_instance = blog.blog_content.blog_link.BlogLink()
        bblog_content_instance = blog.blog_content.blog_content.BlogContent()
        bblog_links = bblog_link_instance.get_link()

        for idx, url in enumerate(bblog_links, start=1):
            blog_link, title, content = bblog_content_instance.get_content(url)
            if title and content:
                self.process_content(idx, title, blog_link)
                self.index_data(idx, title, blog_link)

    def crawl_make_up(self):
        make_up_blog_link_instance = make_up_blog_link.BlogLink()
        make_up_link_crawling_instance = make_up_link_crawling.LinkCrawling()
        make_up_news_content_instance = make_up_news_content.NewsContent()
        blog_posts = make_up_blog_link_instance.get_post_links()
        news_links = make_up_link_crawling_instance.news_link(blog_posts)

        for idx, url in enumerate(news_links, start=1):
            title, content = make_up_news_content_instance.news_crawling(url)
            if title and content:
                self.process_content(idx, title, url)
                self.index_data(idx, title, url)

    def crawl_todak(self):
        todak_blog_link_instance = todak_blog_link.BlogLink()
        todak_link_crawling_instance = todak_link_crawling.LinkCrawling()
        todak_news_content_instance = todak_news_content.NewsContent()
        todak_posts = todak_blog_link_instance.get_post_links()
        todak_links = todak_link_crawling_instance.news_link(todak_posts)

        for idx, url in enumerate(todak_links, start=1):
            title, content = todak_news_content_instance.news_crawling(url)
            if title and content:
                self.process_content(idx, title, url)
                self.index_data(idx, title, url)

    def crawl_donga(self):
        donga_blog_link_instance = donga_blog_link.BlogLink()
        donga_link_crawling_instance = donga_link_crawling.LinkCrawling()
        donga_news_content_instance = donga_news_content.NewsContent()
        donga_posts = donga_blog_link_instance.links()
        donga_links = donga_link_crawling_instance.news_link(donga_posts)

        for idx, url in enumerate(donga_links, start=1):
            title, content = donga_news_content_instance.news_crawling(url)
            if title and content:
                self.process_content(idx, title, url)
                self.index_data(idx, title, url)

    def crawl_qna(self):
        qna_link_instance = qna.qna_link.QnALink()
        qna_crawl_data_instance = qna.QnACrawler()
        qna_links = qna_link_instance.get_links()

        for idx, url in enumerate(qna_links, start=1):
            title, content = qna_crawl_data_instance.crawl_data(url)
            if title and content:
                self.process_content(idx, title, url)
                self.index_data(idx, title, url)

    def crawl_all(self):
        self.crawl_blog()
        self.crawl_make_up()
        self.crawl_todak()
        self.crawl_donga()
        self.crawl_qna()