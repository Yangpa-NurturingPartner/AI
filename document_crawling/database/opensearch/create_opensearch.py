class CreateOpensearch:
    # 1. 인덱스 생성 
    def create_index(self,client, index_name):

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
                "behavior_analysis": {"type": "text"},
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
        print(f"'{index_name}' 인덱스 생성 완료.")