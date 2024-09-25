import os
from opensearchpy import OpenSearch
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

OPENSEARCH_KEY = os.getenv("OPENSEARCH_KEY")
host = "192.168.0.152"

def connect_to_opensearch():
    """Connect to OpenSearch."""
    client = OpenSearch(
        hosts=[{'host': host, 'port': 9200}],
        http_auth=('admin', OPENSEARCH_KEY),
        use_ssl=True,
        verify_certs=False
    )
    return client

def update_mapping_with_behavior_analysis(client, index_name):
    """Update the OpenSearch index mapping to add a new field 'behavior_analysis'."""
    mapping = {
        "properties": {
            "behavior_analysis": {"type": "text"}
        }
    }
    client.indices.put_mapping(index=index_name, body=mapping)
    print(f"Mapping for index '{index_name}' updated successfully.")

def fetch_all_documents(client, index_name, size=1000):
    """Fetch all documents from the OpenSearch index."""
    body = {
        "query": {
            "match_all": {}
        },
        "size": size  # Adjust size based on the number of documents you expect
    }
    response = client.search(index=index_name, body=body)
    return response['hits']['hits']

def update_documents_with_behavior_analysis(client, index_name, documents):
    """Update each document with a new 'behavior_analysis' field."""
    for doc in documents:
        doc_id = doc['_id']
        source = doc['_source']
        # Concatenate 'behavior' and 'analysis' fields to create 'behavior_analysis'
        behavior = source.get('behavior', '')
        analysis = source.get('analysis', '')
        behavior_analysis = behavior + "\n" + analysis
        
        # Update the document with the new 'behavior_analysis' field
        update_body = {
            "doc": {
                "behavior_analysis": behavior_analysis
            }
        }
        response = client.update(index=index_name, id=doc_id, body=update_body)
        print(f"Updated document ID {doc_id} with 'behavior_analysis'")

def main():
    index_name = 'data_video'  # Replace with your actual index name
    client = connect_to_opensearch()
    
    # Step 1: Update the index mapping to include the new 'behavior_analysis' field
    update_mapping_with_behavior_analysis(client, index_name)
    
    # Step 2: Fetch all documents from the index
    documents = fetch_all_documents(client, index_name)
    
    # Step 3: Update each document with the 'behavior_analysis' field
    update_documents_with_behavior_analysis(client, index_name, documents)

if __name__ == "__main__":
    main()
