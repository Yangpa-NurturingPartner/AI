import os
import json
import numpy as np
import pandas as pd
from opensearchpy import OpenSearch
from dotenv import load_dotenv

load_dotenv()

OPENSEARCH_KEY = os.getenv("OPENSEARCH_KEY")
host = "192.168.0.152"

index_name = 'document_data'

# 1.오픈서치 연결
def connect_to_opensearch():
    client = OpenSearch(
        hosts=[{'host': host, 'port': 9200}],
        http_auth=('admin', OPENSEARCH_KEY),
        use_ssl=True,
        verify_certs=False
    )
    return client

# 2. 인덱스 생성
def create_index(client, index_name):

    index_name = index_name

    mapping = {
    "settings": {
        "index": {
            "knn": True  # k-NN 활성화
        }
    },
    "mappings": {
        "properties": {
            "document_no": {"type": "integer"},
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
            "behavior_plus_analysis_emb": { 
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
    print(f" '{index_name}' 인덱스 생성 완료.")


# 3. 문서 데이터 인덱싱
def index_data(client, index_name, row):

    #1.OpenSearch에 단일 행 데이터를 인덱싱
    row['behavior_emb'] = json.loads(row['behavior_emb'])
    row['behavior_plus_analysis_emb'] = json.loads(row['behavior_plus_analysis_emb']) 
    row['behavior_emb'] = [float(x) for x in row['behavior_emb']]
    row['behavior_plus_analysis_emb'] = [float(x) for x in row['behavior_plus_analysis_emb']]  

    # 2.문서 생성
    document = row[['document_no', 'behavior', 'analysis', 'solution', 'behavior_emb', 'behavior_plus_analysis_emb']].to_dict()  

    # 3.문서 인덱싱
    response = client.index(index=index_name, body=document)
    print(f"Indexed document: {response['_id']}") 
