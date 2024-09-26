from data_processing.crawler.crawler import Crawler
from database import database
from data_processing.summary_and_embedding.summary_and_embedding import SummaryAndEmbedding
from database.opensearch.create_opensearch import CreateOpensearch
from properties import Properties


class Main:

    def main():

        # 1. 크롤링 및 크롤링 결과 가져오기
        crawl_results = Crawler().crawl_all()

        print("크롤링이 끝났습니다.")

        # 2. 크롤링 내용을 데이터 베이스에 저장하기
        db = database.Database()
        db.process_content(crawl_results)

        print("데이터 적재가 끝났습니다.")

        # 3. 크롤링 내용을 요약하고 임베딩하기
        summaries, embeddings = SummaryAndEmbedding().summary_and_embedding(crawl_results)

        print("요약 및 임베딩이 끝났습니다.")

        # 4.오픈서치 인덱스 생성
        properties_instance = Properties()
        opensearch_client = properties_instance.opensearch()
        index_name = "data_document"
        
        CreateOpensearch().create_index(opensearch_client, index_name)

        print("오픈서치 인덱스가 생성되었습니다.")

        # 5. 요약 및 임베딩 내용을 오픈서치에 적재하기
        db.process_and_upload_to_opensearch(summaries, embeddings)

        print("오픈서치 적재가 끝났습니다.")

if __name__ == "__main__":
    Main.main()

