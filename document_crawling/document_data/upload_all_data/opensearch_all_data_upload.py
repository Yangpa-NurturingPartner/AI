import os
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from opensearchpy import OpenSearch
load_dotenv()
opensearch_password = os.getenv("opensearch_password")

# 1. OpenSearch에 연결
client = OpenSearch(
    hosts=[{'host': 'localhost', 'port': 9200}],  # localhost에서 도커 컨테이너가 실행 중이라고 가정
    http_auth=('admin', opensearch_password),  # OpenSearch의 사용자 이름과 비밀번호 (설정에 따라 다를 수 있음)
    use_ssl=True,
    verify_certs=False
)

# 2. 'video_data' 인덱스 생성
index_name = 'video_data'

# 인덱스 생성 시 설정할 매핑 정보(필드 타입)
mapping = {
    "settings": {
        "index": {
            "knn": True  # k-NN 기능 활성화
        }
    },
    "mappings": {
        "properties": {
            "video_no": {"type": "integer"},
            "prob_no": {"type": "integer"},
            "title": {"type": "text"},
            "url": {"type": "text"},
            "upload_date": {"type": "date"},
            "behavior": {"type": "text"},
            "analysis": {"type": "text"},
            "solution": {"type": "text"},
            "behavior_emb": {
                "type": "knn_vector",
                "dimension": 4096,
                "method": {
                    "engine": "nmslib",  # 검색 엔진 선택 (nmslib, faiss, lucene 등)
                    "name": "hnsw",  # 검색 알고리즘 선택 (hnsw, ivf 등)
                    "space_type": "l2",  # 거리 계산 방법 (L2: 유클리드 거리)
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
                    "engine": "nmslib",  # 검색 엔진 선택 (nmslib, faiss, lucene 등)
                    "name": "hnsw",  # 검색 알고리즘 선택 (hnsw, ivf 등)
                    "space_type": "l2",  # 거리 계산 방법 (L2: 유클리드 거리)
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

# 3. 데이터프레임 데이터 불러오기 (예시로 CSV 파일 사용)
# 실제로는 해당 데이터프레임이 이미 생성된 상태일 것
df = pd.read_csv("./final_data.csv")

# 4. 데이터프레임 데이터를 OpenSearch에 적재
def index_data(row):
    document = row.to_dict()  # 각 행을 사전으로 변환
    document['behavior_emb'] = np.random.rand(4096).tolist()  # 4096차원 벡터 랜덤 생성하여 behavior_emb 필드에 추가
    document['behavior_analysis_emb'] = np.random.rand(4096).tolist()
    response = client.index(index=index_name, body=document)
    print(response)

# 데이터프레임의 각 행을 OpenSearch에 적재
df.apply(index_data, axis=1)