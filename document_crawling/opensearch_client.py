import os
import json
from opensearchpy import OpenSearch
from dotenv import load_dotenv

class OpenSearchClient:
    def __init__(self):
        load_dotenv()
        self.host = "192.168.0.152"
        self.OPENSEARCH_KEY = os.getenv("OPENSEARCH_KEY")
        self.client = self.connect_to_opensearch()

    def connect_to_opensearch(self):
        client = OpenSearch(
            hosts=[{'host': self.host, 'port': 9200}],
            http_auth=('admin', self.OPENSEARCH_KEY),
            use_ssl=True,
            verify_certs=False
        )
        return client

    def create_index(self, index_name='document_data'):
        mapping = {
            "settings": {
                "index": {
                    "knn": True  # k-NN 활성화
                }
            },
            "mappings": {
                "properties": {
                    "document_no": {"type": "integer"},
                    "title": {"type": "text"},
                    "url": {"type": "text"},
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
        if self.client.indices.exists(index=index_name):
            self.client.indices.delete(index=index_name)

        self.client.indices.create(index=index_name, body=mapping)
        print(f" '{index_name}' 인덱스 생성 완료.")

    def index_data(self, index_name, row):
        row['behavior_emb'] = json.loads(row['behavior_emb'])
        row['behavior_plus_analysis_emb'] = json.loads(row['behavior_plus_analysis_emb']) 
        row['behavior_emb'] = [float(x) for x in row['behavior_emb']]
        row['behavior_plus_analysis_emb'] = [float(x) for x in row['behavior_plus_analysis_emb']]  

        document = {
            'document_no': row['document_no'],
            'title': row['title'],
            'url': row['url'],
            'behavior_emb': row['behavior_emb'],
            'behavior_plus_analysis_emb': row['behavior_plus_analysis_emb']
        }

        response = self.client.index(index=index_name, body=document)
        print(f"Indexed document: {response['_id']}")