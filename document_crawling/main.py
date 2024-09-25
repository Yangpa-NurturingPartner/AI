from data_processing.crawler import crawler
from database import database
from data_processing.summary_and_embedding.summary_and_embedding import SummaryAndEmbedding


class Main:

    def main():

        # 1. 크롤링 및 크롤링 결과 가져오기
        crawl_results = crawler.Crawler().crawl_all()

        # 2. 크롤링 내용을 데이터 베이스에 저장하기
        db = database.Database()
        db.process_content(crawl_results)

        # 3. 크롤링 내용을 요약하고 임베딩하기
        summaries, embeddings = SummaryAndEmbedding().summary_and_embedding(crawl_results)

        # 4. 요약 및 임베딩 내용을 오픈서치에 적재하기
        database.Database().process_and_upload_to_opensearch(summaries, embeddings)

if __name__ == "__main__":
    Main.main()

