import os
import requests
import numpy as np
from dotenv import load_dotenv
from opensearchpy import OpenSearch

# 환경변수 로드
load_dotenv()
opensearch_password = os.getenv("opensearch_password")

# OpenSearch 클라이언트 연결
client = OpenSearch(
    hosts=[{'host': 'localhost', 'port': 9200}],
    http_auth=('admin', opensearch_password),
    use_ssl=True,
    verify_certs=False
)

# Upstage Solar 모델로 임베딩 생성
def get_embedding(query_text):
    return get_embedding_from_solar(query_text)

# RRF로 결합하는 함수
def reciprocal_rank_fusion(bm25_results, neural_results):
    fusion_scores = {}
    
    # BM25 결과 순위 기반 점수 부여
    for rank, hit in enumerate(bm25_results):
        doc_id = hit['_id']
        score = 1 / (rank + 1)
        if doc_id not in fusion_scores:
            fusion_scores[doc_id] = 0
        fusion_scores[doc_id] += score

    # Neural 검색 결과 순위 기반 점수 부여
    for rank, hit in enumerate(neural_results):
        doc_id = hit['_id']
        score = 1 / (rank + 1)
        if doc_id not in fusion_scores:
            fusion_scores[doc_id] = 0
        fusion_scores[doc_id] += score

    # 점수 순으로 정렬
    sorted_fusion = sorted(fusion_scores.items(), key=lambda item: item[1], reverse=True)
    return sorted_fusion

# OpenSearch 하이브리드 검색 쿼리
def hybrid_search(query_text, index_name):
    # Solar API로 임베딩 생성
    query_vector = get_embedding(query_text)
    
    # BM25 기반 multi_match 검색
    bm25_query = {
        "query": {
            "multi_match": {
                "query": query_text,
                "fields": ["title", "description"]
            }
        }
    }
    bm25_results = client.search(index=index_name, body=bm25_query)['hits']['hits']

    # Neural 기반 시멘틱 검색 (여러 필드에서 각각 상위 3개씩 검색)
    neural_query = {
        "knn": {
            "queries": [
                {
                    "field": "behavior_emb",
                    "query_vector": query_vector,
                    "k": 3
                },
                {
                    "field": "behavior_analysis_emb",
                    "query_vector": query_vector,
                    "k": 3
                }
            ]
        }
    }
    neural_results = client.search(index=index_name, body=neural_query)['hits']['hits']

    # RRF로 결과 결합
    final_results = reciprocal_rank_fusion(bm25_results, neural_results)
    return final_results

# 검색 실행
if __name__ == "__main__":
    index_name = 'video_data'
    query_text = "example search keyword"
    
    
    # 하이브리드 검색 실행
    results = hybrid_search(query_text, index_name)
    print("Hybrid search results with RRF:", results)
