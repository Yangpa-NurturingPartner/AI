import os
import warnings
import numpy as np
from langchain_community.vectorstores import OpenSearchVectorSearch
from langchain_upstage import UpstageEmbeddings
from dotenv import load_dotenv
from opensearchpy import OpenSearch
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
  model="solar-embedding-1-large-query"
)

# Connect to OpenSearch client
opensearch_client = OpenSearch(
    hosts=[{'host': host, 'port': port}],
    http_auth=opensearch_auth,
    use_ssl=True,
    verify_certs=False
)

# Create OpenSearch vector store
index_name = 'data_video'
vector_store = OpenSearchVectorSearch(
    embedding_function=embedding_function,
    opensearch_url=f"http://{host}:{port}",
    http_auth=opensearch_auth,
    index_name=index_name,
    use_ssl=True,
    verify_certs=False
)

def normalize_tmm(scores, fixed_min_value=0.0):
	arr = np.array(scores)
	max_value = np.max(arr)
	norm_score = (arr - fixed_min_value) / (max_value - fixed_min_value)
	return norm_score

def get_id(prob_no, index_name):
    """Search OpenSearch by prob_no and retrieve the full row of the document."""
    query_body = {
        "query": {
            "match": {
                "prob_no": prob_no  # Match based on the prob_no field
            }
        }
    }
    # Search in OpenSearch
    response = opensearch_client.search(index=index_name, body=query_body)
    # Extract the first matching document (assuming prob_no is unique)
    if len(response['hits']['hits']) > 0:
        document_id = response['hits']['hits'][0]['_id']  # Get the document ID
        return document_id  # Return both ID and document content
    return None  

# BM25 keyword search function
def keyword_search(query, index_name, size, text_field):
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
def vector_search(query, index_name, size, text_field, vector_field):
    # Perform similarity search
    docs = vector_store.similarity_search_with_relevance_scores(query, k=size, text_field=text_field, vector_field=vector_field)
    # Format the results
    results = []
    for doc, score in docs:
        doc_content = {
            'id': get_id(doc.metadata.get('prob_no'), "data_video"),
            'video_no': doc.metadata.get('video_no'),  # Retrieve from metadata
            'prob_no': doc.metadata.get('prob_no'),
            'behavior': doc.metadata.get('behavior'),
            'analysis': doc.metadata.get('analysis'),
            'solution': doc.metadata.get('solution'),
            'behavior_emb': doc.metadata.get('behavior_emb'),
            'behavior_analysis_emb': doc.metadata.get('behavior_analysis_emb'),
            'behavior_analysis': doc.metadata.get('behavior_analysis'),
        }
        results.append({'doc': doc_content, 'score': score})
    return results

# Perform searches once and reuse results for all hybrid search algorithms
def perform_searches(query, index_name, text_field, vector_field, size):
    # Perform BM25 and vector searches once
    bm25_results = keyword_search(query, index_name, size, text_field)
    vector_results = vector_search(query, index_name, size, text_field, vector_field)
    return bm25_results, vector_results

def rrf_hybrid_search_with_results(bm25_results, vector_results, k=60):
    # Build rankings for RRF
    bm25_ranking = {res['doc']['id']: rank + 1 for rank, res in enumerate(bm25_results)}
    vector_ranking = {res['doc']['id']: rank + 1 for rank, res in enumerate(vector_results)}

    # Combine rankings using RRF
    combined_scores = {}
    all_doc_ids = set(bm25_ranking.keys()).union(vector_ranking.keys())
    for doc_id in all_doc_ids:
        score = 0
        if doc_id in bm25_ranking:
            score += 1 / (k + bm25_ranking[doc_id])
        if doc_id in vector_ranking:
            score += 1 / (k + vector_ranking[doc_id])
        combined_scores[doc_id] = score

    # Sort documents by combined scores
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

def cc_hybrid_search_with_results(bm25_results, vector_results, bm25_weight=0.3, vector_weight=0.7):
    combined_scores = {}
    for bm25_res in bm25_results:
        doc_id = bm25_res['doc']['id']
        bm25_score = bm25_res['score'] * bm25_weight
        combined_scores[doc_id] = bm25_score
    
    for vector_res in vector_results:
        doc_id = vector_res['doc']['id']
        vector_score = vector_res['score'] * vector_weight
        if doc_id in combined_scores:
            combined_scores[doc_id] += vector_score
        else:
            combined_scores[doc_id] = vector_score

    # Sort the documents based on combined scores
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

def tmmcc_hybrid_search_with_results(bm25_results, vector_results, bm25_weight=0.3, vector_weight=0.7):
    # Extract BM25 and vector scores
    bm25_scores = [res['score'] for res in bm25_results]
    vector_scores = [res['score'] for res in vector_results]
    
    # Normalize both sets of scores
    norm_bm25_scores = normalize_tmm(bm25_scores, fixed_min_value=0.0) if len(bm25_scores) != 0 else bm25_scores
    norm_vector_scores = normalize_tmm(vector_scores, fixed_min_value=0.0)
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
    bm25_result, vector_result = perform_searches(query_text, index_name, text_field, vector_field, size=size)

    tmmcc_results = tmmcc_hybrid_search_with_results(bm25_result, vector_result, bm25_weight=bm25_weight, vector_weight=vector_weight)
    for index, (doc, score) in enumerate(tmmcc_results):
        print(f"{index} 순위의 behavior:\n {doc[text_field]}")
        print(f"Combined Score: {score}")
        print("-" * 10)
    print("*"*50)
