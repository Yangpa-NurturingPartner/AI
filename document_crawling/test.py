import os
import psycopg2
import numpy as np
from openai import OpenAI
from dotenv import load_dotenv

# 1. .env 파일 열기
load_dotenv()

# 2. postgreSQL 연결
user, password, host = os.getenv("DB_USER"), os.getenv("DB_PASSWORD"), os.getenv("DB_HOST")

def connect_db():
    connection = psycopg2.connect(user=user, password=password, host=host, port=5432)
    return connection
 
# 3. 임베딩 api 키
embedding_api_key = os.getenv("UPSTAGE_API_KEY")

# 4. 임베딩 모델 사용
client = OpenAI(
    api_key= embedding_api_key,
    base_url="https://api.upstage.ai/v1/solar"
)
 
query_result = client.embeddings.create(
    model = "solar-embedding-1-large-query",
    input = "애가 화장실을 못가용"
).data[0].embedding
 
# 행동
document_result = client.embeddings.create(
    model = "solar-embedding-1-large-passage",
    input = "SOLAR 10.7B: Scaling Large Language Models with Simple yet Effective Depth Up-Scaling. DUS is simple yet effective in scaling up high performance LLMs from small ones. "
).data[0].embedding

# 행동 + 분석
document_result = client.embeddings.create(
    model = "solar-embedding-1-large-passage",
    input = "SOLAR 10.7B: Scaling Large Language Models with Simple yet Effective Depth Up-Scaling. DUS is simple yet effective in scaling up high performance LLMs from small ones. "
).data[0].embedding
 
