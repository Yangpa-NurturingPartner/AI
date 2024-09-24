import os
import json
import numpy as np
import pandas as pd
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

def create_index(client, index_name):
    """Create an index in OpenSearch with k-NN settings."""
    # Index creation mapping
    mapping = {
        "settings": {
            "index": {
                "knn": True  # Enable k-NN
            }
        },
        "mappings": {
            "properties": {
                "video_no": {"type": "integer"},
                "prob_no": {"type": "integer"},
                "behavior": {"type": "text"},
                "analysis": {"type": "text"},
                "solution": {"type": "text"},
                "behavior_emb": {
                    "type": "knn_vector",
                    "dimension": 4096,
                    "method": {
                        "engine": "nmslib",
                        "name": "hnsw",
                        "space_type": "l2",
                        "parameters": {
                            "ef_construction": 128,
                            "m": 24
                        }
                    }
                },
                "behavior_analysis_emb": {
                    "type": "knn_vector",
                    "dimension": 4096,
                    "method": {
                        "engine": "nmslib",
                        "name": "hnsw",
                        "space_type": "l2",
                        "parameters": {
                            "ef_construction": 128,
                            "m": 24
                        }
                    }
                }
            }
        }
    }

    if client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)

    client.indices.create(index=index_name, body=mapping)
    print(f"Index '{index_name}' created successfully.")

def index_data(client, index_name, row):
    """Index a single row of data into OpenSearch."""
    row['behavior_emb'] = json.loads(row['behavior_emb'])
    row['behavior_analysis_emb'] = json.loads(row['behavior_analysis_emb'])  
    row['behavior_emb'] = [float(x) for x in row['behavior_emb']]
    row['behavior_analysis_emb'] = [float(x) for x in row['behavior_analysis_emb']]

    document = row[['video_no', 'prob_no', 'behavior', 'analysis', 'solution', 'behavior_emb', 'behavior_analysis_emb']].to_dict()  # Select only the relevant columns
    response = client.index(index=index_name, body=document)
    print(f"Indexed document: {response['_id']}")
