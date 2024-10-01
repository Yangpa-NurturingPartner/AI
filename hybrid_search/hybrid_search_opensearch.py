import os
import warnings
import numpy as np
from langchain_upstage import UpstageEmbeddings
from dotenv import load_dotenv
from opensearchpy import OpenSearch
from time import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple, Any, Optional

warnings.filterwarnings("ignore")
# Load environment variables
load_dotenv()
UPSTAGE_API_KEY = os.getenv("UPSTAGE_API_KEY")
OPENSEARCH_KEY = os.getenv("OPENSEARCH_KEY")
host = "192.168.0.152"
port = 9200
opensearch_auth = ('admin', OPENSEARCH_KEY)
# Set up the embedding function
embedding_function = UpstageEmbeddings(
  api_key=UPSTAGE_API_KEY,
  model="solar-embedding-1-large"
)

# Connect to OpenSearch client
opensearch_client = OpenSearch(
    hosts=[{'host': host, 'port': port}],
    http_auth=opensearch_auth,
    use_ssl=True,
    verify_certs=False
)

def normalize_tmm(scores, fixed_min_value=0.0):
	arr = np.array(scores)
	max_value = np.max(arr)
	norm_score = (arr - fixed_min_value) / (max_value - fixed_min_value)
	return norm_score


def keyword_multi_search(query, index_names, size, text_field):
    response = opensearch_client.search(
        index=index_names,  # 리스트로 전달
        body={
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": [text_field]  # 필드 이름은 인덱스에서 동일
                }
            },
            "size": size
        }
    )
    # Extract documents and include OpenSearch document ID
    docs = []
    for hit in response['hits']['hits']:
        source = hit['_source']
        source['id'] = hit['_id']  # Include the document ID
        docs.append({'doc': source, 'score': hit['_score']})
    return docs

def vector_multi_search(query, index_names, size, vector_field):
    # OpenAI 또는 사용자 정의 임베딩 생성 함수로 임베딩 생성
    embedding = embedding_function.embed_query(query)

    # 단일 필드에 대한 벡터 검색 쿼리 생성
    knn_query = {
        "knn": {
            vector_field: {
                "vector": embedding,
                "k": size  # 상위 k개 결과를 반환
            }
        }
    }

    # 벡터 검색 쿼리 생성
    search_query = {
        "size": size,
        "query": knn_query  # 단일 필드에 대한 KNN 검색
    }

    # 여러 인덱스를 대상으로 검색
    response = opensearch_client.search(
        index=index_names,  # 인덱스 리스트 또는 쉼표로 구분된 문자열을 전달
        body=search_query
    )
    
    # 검색 결과 처리
    hits = response['hits']['hits']
    
    results = []
    for hit in hits:
        doc_content = {}
        doc_content['id'] = hit["_id"]  # 문서 ID
        for key, value in hit["_source"].items():
            doc_content[key] = value  # 메타데이터
        results.append({'doc': doc_content, 'score': hit["_score"]})

    return results

# BM25 keyword search function
def keyword_search(query, index_name, size, text_field, field=None, user_no=None):
    if field=="chat":
        response = opensearch_client.search(
            index=index_name,
            body={
                "size": size,
                "query": {
                    "bool": {
                        "must": {
                            "match": {
                                text_field: query  # text_field에 쿼리 검색
                            }
                        },
                        "filter": {
                            "term": {
                                "user_no": user_no  # user_no에 따라 필터링
                            }
                        }
                    }
                }
            }
        )
    else:
        response = opensearch_client.search(
            index=index_name,
            body={
                "query": {
                    "match": {
                        text_field: query  # text_field is passed here
                    }
                },
                "size": size
            }
        )
    # Extract documents and include OpenSearch document ID
    docs = []
    for hit in response['hits']['hits']:
        source = hit['_source']
        source['id'] = hit['_id']  # Include the document ID
        docs.append({'doc': source, 'score': hit['_score']})
    return docs


# Vector search function
def vector_search(query, index_name, size, text_field, vector_field, pk, field=None, user_no=None):
    # OpenAI 또는 사용자 정의 임베딩 생성 함수로 임베딩 생성
    embedding = embedding_function.embed_query(query)
    
    if field == "chat":
        # 유저 번호에 따라 필터링이 필요한 경우
        pre_filter = {
            "term": {
                "user_no": user_no  # user_no에 따라 필터링
            }
        }
        # OpenSearch k-NN 쿼리 작성 (L2 거리)
        search_query = {
            "size": size,
            "query": {
                "bool": {
                    "filter": pre_filter,
                    "must": {
                        "knn": {
                            vector_field: {
                                "vector": embedding,
                                "k": size  # 상위 k개 결과를 반환
                            }
                        }
                    }
                }
            }
        }
    else:
        # 필터가 없는 경우 기본 벡터 검색 (L2 거리)
        search_query = {
            "size": size,
            "query": {
                "knn": {
                    vector_field: {
                        "vector": embedding,
                        "k": size  # 상위 k개 결과를 반환
                    }
                }
            }
        }
    
    response = opensearch_client.search(
        index=index_name,
        body=search_query
    )
    
    # 검색 결과 처리
    hits = response['hits']['hits']
    
    results = []
    for hit in hits:
        doc_content = {}
        doc_content['id'] = hit["_id"]  # 문서 ID
        for key, value in hit["_source"].items():
            doc_content[key] = value  # 메타데이터
        results.append({'doc': doc_content, 'score': hit["_score"]}) 
    return results

# Perform searches once and reuse results for all hybrid search algorithms
def perform_multi_searches(query, index_names, text_field, vector_field, size):
    # Perform BM25 and vector searches once
    with ThreadPoolExecutor() as executor:
        bm25_future = executor.submit(keyword_multi_search, query, index_names, size, text_field)
        vector_future = executor.submit(vector_multi_search, query, index_names, size, vector_field)
        
        bm25_results = bm25_future.result()
        vector_results = vector_future.result()
    return bm25_results, vector_results

# Perform searches once and reuse results for all hybrid search algorithms
def perform_searches(query, index_name, text_field, vector_field, size, pk, field=None, user_no=None):
    # Perform BM25 and vector searches once
    with ThreadPoolExecutor() as executor:
        bm25_future = executor.submit(keyword_search, query, index_name, size, text_field, field, user_no)
        vector_future = executor.submit(vector_search, query, index_name, size, text_field, vector_field, pk, field, user_no)
        
        bm25_results = bm25_future.result()
        vector_results = vector_future.result()
    return bm25_results, vector_results

def tmmcc_hybrid_search_with_results(bm25_results, vector_results, bm25_weight=0.3, vector_weight=0.7):
    # Extract BM25 and vector scores
    bm25_scores = [res['score'] for res in bm25_results]
    vector_scores = [res['score'] for res in vector_results]
    
    # Normalize both sets of scores
    norm_bm25_scores = normalize_tmm(bm25_scores, fixed_min_value=0.0) if len(bm25_scores) != 0 else bm25_scores
    norm_vector_scores = normalize_tmm(vector_scores, fixed_min_value=0.0) if len(vector_scores) != 0 else vector_scores
    combined_scores = {}
    for i, bm25_res in enumerate(bm25_results):
        doc_id = bm25_res['doc']['id']
        bm25_norm_score = norm_bm25_scores[i] * bm25_weight
        combined_scores[doc_id] = bm25_norm_score
    
    for i, vector_res in enumerate(vector_results):
        doc_id = vector_res['doc']['id']
        vector_norm_score = norm_vector_scores[i] * vector_weight
        if doc_id in combined_scores:
            combined_scores[doc_id] += vector_norm_score
        else:
            combined_scores[doc_id] = vector_norm_score
    # Sort the documents based on combined TMMCC scores
    sorted_docs = sorted(combined_scores.items(), key=lambda x: x[1], reverse=True)

    # Retrieve the documents based on sorted scores
    docs = []
    for doc_id, score in sorted_docs:
        doc = next((res['doc'] for res in bm25_results if res['doc']['id'] == doc_id), None)
        if not doc:
            doc = next((res['doc'] for res in vector_results if res['doc']['id'] == doc_id), None)
        if doc:
            docs.append([doc, score])
    return docs

# Main execution block
if __name__ == "__main__":
    query_text = "아이가 말끝마다 '싫어'라고 대답하는데 어떻게 하면 말을 듣게 할 수 있을까요?"
    text_field = "behavior_analysis"
    vector_field = "behavior_analysis_emb"
    size = 5
    bm25_weight = 0.2
    vector_weight = 0.8
    pk='prob_no'
    index_name = "data_video"
    bm25_result, vector_result = perform_searches(query_text, index_name, text_field, vector_field, size=size, pk=pk)

    tmmcc_results = tmmcc_hybrid_search_with_results(bm25_result, vector_result, bm25_weight=bm25_weight, vector_weight=vector_weight)
    for index, (doc, score) in enumerate(tmmcc_results):
        print(f"{index} 순위의 behavior:\n {doc[text_field]}")
        print(f"Combined Score: {score}")
        print("-" * 10)
    print("*"*50)
