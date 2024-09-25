import os
import numpy as np
from openai import OpenAI
from dotenv import load_dotenv
load_dotenv()
UPSTAGE_API_KEY = os.getenv("UPSTAGE_API_KEY")
# API 클라이언트 설정
client = OpenAI(
    api_key=UPSTAGE_API_KEY,
    base_url="https://api.upstage.ai/v1/solar"
)

def update_behavior_embedding(connection):
    """Update behavior embeddings in the database using prob_no as the unique identifier."""
    cur = connection.cursor()
    select_query = "SELECT prob_no, behavior FROM data_video WHERE behavior IS NOT NULL;"
    cur.execute(select_query)
    rows = cur.fetchall()
    
    for row in rows:
        prob_no = row[0]  # prob_no는 유일값
        behavior_text = row[1]
        
        # 임베딩 생성 (리스트 형태로 반환)
        behavior_embedding = client.embeddings.create(
                                model="solar-embedding-1-large-passage",
                                input=behavior_text
                                ).data[0].embedding
        
        # 임베딩 업데이트 (prob_no 기준)
        update_query = "UPDATE data_video SET behavior_emb = %s WHERE prob_no = %s;"
        cur.execute(update_query, (behavior_embedding, prob_no))
    
    connection.commit()
    cur.close()

def update_behavior_analysis_embedding(connection):
    """Update behavior + analysis embeddings in the database using prob_no as the unique identifier."""
    cur = connection.cursor()
    select_query = "SELECT prob_no, behavior, analysis FROM data_video WHERE behavior IS NOT NULL AND analysis IS NOT NULL;"
    cur.execute(select_query)
    rows = cur.fetchall()
    
    for row in rows:
        prob_no = row[0]  # prob_no는 유일값
        behavior_text = row[1]
        analysis_text = row[2]
        
        # behavior와 analysis 합치기
        combined_text = behavior_text + " " + analysis_text
        
        # 임베딩 생성 (리스트 형태로 반환)
        behavior_analysis_embedding = client.embeddings.create(
                                        model="solar-embedding-1-large-passage",
                                        input=combined_text
                                        ).data[0].embedding
    
        # 임베딩 업데이트 (prob_no 기준)
        update_query = "UPDATE data_video SET behavior_analysis_emb = %s WHERE prob_no = %s;"
        cur.execute(update_query, (behavior_analysis_embedding, prob_no))
    
    connection.commit()
    cur.close()
