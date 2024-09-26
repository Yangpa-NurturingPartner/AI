# 86
import os
from dotenv import load_dotenv
from opensearchpy import OpenSearch
load_dotenv()
OPENSEARCH_KEY = os.getenv("OPENSEARCH_KEY")
host = "192.168.0.152"
port = 9200
opensearch_auth = ('admin', OPENSEARCH_KEY)
# Connect to OpenSearch client
opensearch_client = OpenSearch(
    hosts=[{'host': host, 'port': port}],
    http_auth=opensearch_auth,
    use_ssl=True,
    verify_certs=False
)

index_name = 'data_video'
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

def delete_document_by_prob_no(prob_no, index_name):
    # 먼저 prob_no로 문서의 ID를 조회
    document_id = get_id(prob_no, index_name)
    
    if document_id:
        # 문서 ID가 존재하면 해당 문서를 삭제
        response = opensearch_client.delete(index=index_name, id=document_id)
        print(f"Document with prob_no {prob_no} deleted. Response: {response}")
    else:
        print(f"No document found with prob_no {prob_no}")

if __name__=="__main__":
    delete_document_by_prob_no(86, index_name)