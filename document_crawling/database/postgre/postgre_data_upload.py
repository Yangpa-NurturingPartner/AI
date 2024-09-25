import database
from crawling_and_summary import crawler

class PostgreDataUpload:

    def upload_data(self):

        # 크롤러 인스턴스 생성
        crawler_instance = crawler.Crawler()
        # 데이터 크롤링
        results = crawler_instance.crawl_all()

        # 데이터베이스 인스턴스 생성
        db = database.Database()
        # 크롤링한 데이터 처리

        for idx, title, url in results:
            db.process_content(idx, title, url)


