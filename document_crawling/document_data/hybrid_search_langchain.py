import os
from langchain_community.vectorstores import OpenSearchVectorSearch
from langchain_upstage import UpstageEmbeddings
from dotenv import load_dotenv
from opensearchpy import OpenSearch

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
    http_auth=('admin', OPENSEARCH_KEY),
    use_ssl=False,
    verify_certs=False
)

# Create OpenSearch vector store
index_name = 'data_video'
vector_store = OpenSearchVectorSearch(
    embedding_function=embedding_function,
    opensearch_url=f"https://{host}:{port}",
    http_auth=opensearch_auth,
    index_name=index_name,
    text_field="behavior",  # Assuming the field that contains the text is "behavior"
    vector_query_field="behavior_emb"
)

# BM25 keyword search function
def keyword_search(query, index_name):
    response = opensearch_client.search(
        index=index_name,
        body={
            "query": {
                "match": {
                    "behavior": query
                }
            }
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
def vector_search(query, index_name):
    # Get the embedding for the query text
    query_vector = embedding_function.embed_query(query)
    # Perform similarity search
    docs = vector_store.similarity_search_with_score_by_vector(query_vector)
    # Format the results
    results = []
    for doc, score in docs:
        doc_content = {
            'id': doc.metadata.get('id', doc.metadata.get('_id')),
            'content': doc.page_content
        }
        results.append({'doc': doc_content, 'score': score})
    return results

# Hybrid search using Reciprocal Rank Fusion (RRF)
def hybrid_search(query, index_name, k=60):
    # Get BM25 and vector search results
    bm25_results = keyword_search(query, index_name)
    print("*"*100)

    vector_results = vector_search(query, index_name)
    
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
    for doc_id, _ in sorted_docs:
        # Try to find the document in BM25 results first
        doc = next((res['doc'] for res in bm25_results if res['doc']['id'] == doc_id), None)
        # If not found, find in vector results
        if not doc:
            doc = next((res['doc'] for res in vector_results if res['doc']['id'] == doc_id), None)
        if doc:
            docs.append(doc)
    return docs

# Execute the search
if __name__ == "__main__":
    query_text = "아이가 엄마 뒤에 자꾸 숨습니다."
    # Perform hybrid search
    results = hybrid_search(query_text, index_name)
    print("Hybrid search results:", results)
