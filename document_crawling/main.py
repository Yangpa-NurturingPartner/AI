from document_crawling.crawler import Crawler
from document_crawling.opensearch import OpenSearchClient
from properties import Properties
import openai
import os
from dotenv import load_dotenv

def main():
    load_dotenv()

    # 1. 프로퍼티 인스턴스 생성
    properties_instance = Properties()

    # 2. sql 연결 
    connection = properties_instance.sql()()
    cursor = connection.cursor()

    # 3. api key 가져오기
    embedding_api_key, client = properties_instance.api_key()

    # 4. 임베딩 모델 사용
    embedding_client = properties_instance.embedding_model()

    # 5. 모델 가져오기
    model, system_prompt = properties_instance.model()

    # 6. 오픈서치 연결 및 인덱스 생성
    opensearch_client = OpenSearchClient()
    opensearch_client.create_index()

    # 7. 크롤러 인스턴스 생성 및 크롤링 수행
    crawler = Crawler(cursor, connection, opensearch_client)
    crawler.crawl_all()

    # 8. 데이터베이스 연결 종료
    connection.close()
    print("크롤링 및 요약 결과가 데이터베이스에 저장되었습니다.")

if __name__ == "__main__":
    main()