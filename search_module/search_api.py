import os
from time import time
from openai import OpenAI
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import List, Dict, Optional
from opensearchpy import OpenSearch
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from search_module.search_index import search_video, search_document, search_community, search_chat, generate_rag_response, search_video_document
load_dotenv()

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPSTAGE_API_KEY = os.getenv("UPSTAGE_API_KEY")
OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST")
OPENSEARCH_USER = os.getenv("OPENSEARCH_USER")
OPENSEARCH_PASSWORD = os.getenv("OPENSEARCH_PASSWORD")

embedding_client = OpenAI(
    api_key=UPSTAGE_API_KEY,
    base_url="https://api.upstage.ai/v1/solar"
)
opensearch_client = OpenSearch(
    hosts=[{"host": OPENSEARCH_HOST, "port": 9200}],
    http_auth=(OPENSEARCH_USER, OPENSEARCH_PASSWORD),
    use_ssl=True,
    verify_certs=False
)

class SearchQuery(BaseModel):
    query: str
    user_no: int = None
    search_size: int = 8
    top_k: int = 10

class ChatContent(BaseModel):
    messages: List[Dict[str, str]]
    search_size: int = 10
    top_k: int = 4

class ChatQnA(BaseModel):
    user_no : int
    session_id : str
    query : str
    answer : str

class CommunityContent(BaseModel):
    content: str
    board: int
    
@app.post("/embedded/chat/contents")
async def embedding_chat(content_data: ChatQnA):
    try:
        content = content_data.query + "\n" + content_data.answer

        embeddings = embedding_client.embeddings.create(
            model="solar-embedding-1-large-passage",
            input=content
        ).data[0].embedding
        
        document = {
            "user_no": content_data.user_no,
            "session_id": content_data.session_id,
            "content" : content,
            "content_emb" : embeddings
        }

        response = opensearch_client.index(
            index="data_chat",
            body=document
        )

        if response["result"] == "created":
            return {"status": 200, "message": ""}
        else:
            return {"status": 500, "message": "OpenSearch 저장 실패"}
    except Exception as e:
        return {"status": 500, "message": str(e)}

@app.post("/embedded/community/contents")
async def embedCommunity(content_data: CommunityContent):
    try:
        search_response = opensearch_client.search(
            index="data_community",
            body={
                "size": 1,
                "sort": [
                    {"community_no": "desc"}
                ],
                "_source": ["community_no"]
            }
        )
        hits = search_response['hits']['hits']
        if hits:
            highest_community_no = hits[0]['_source']['community_no']
        else:
            highest_community_no = 0

        new_community_no = highest_community_no + 1

        embedding_response = embedding_client.embeddings.create(
            model="solar-embedding-1-large-passage",
            input=content_data.content
        )
        embeddings = embedding_response.data[0].embedding

        document = {
            "content": content_data.content,
            "content_emb": embeddings,
            "board_no": content_data.board,
            "community_no": new_community_no
        }

        response = opensearch_client.index(
            index="data_community",
            body=document
        )

        if response["result"] == "created":
            return {"status": 200, "message": ""}
        else:
            return {"status": 500, "message": "OpenSearch 저장 실패"}
    except Exception as e:
        return {"status": 500, "message": str(e)}

@app.post("/search/chat")
async def RAG_chat(chat_content: ChatContent):
    query = chat_content.messages[-1]['content']
    results = search_video_document(query, chat_content.search_size)[:chat_content.top_k]
    rag_response = generate_rag_response(query, chat_content.messages, results)  # RAG 로직 호출
    return {"result": rag_response}


@app.post("/search/unified")
async def unified_search(search_query: SearchQuery):
    # 통합 검색에서는 모든 인덱스에서 검색 수행
    video_results = search_video(search_query.query, search_query.search_size)
    document_results = search_document(search_query.query, search_query.search_size)[:3]
    community_results = search_community(search_query.query, search_query.search_size)[:3]
    chat_results = search_chat(search_query.query, search_query.user_no, search_query.search_size)[:3]
    video_no_list = []
    seen = set()  # 중복 확인용 set
    for result in video_results:
        video_no = result[0]["video_no"]
        if video_no not in seen:  # set에서 중복 확인 (성능 O(1))
            video_no_list.append(int(video_no))
            seen.add(video_no)
        if len(video_no_list)==3: break

    document_no_list = [result[0]["document_no"] for result in document_results]
    board_no_list = [result[0]["board_no"] for result in community_results]
    session_ids = [result[0]["session_id"] for result in chat_results]
    # 검색 결과 반환
    return {
        "video_results_video_no": video_no_list,
        "document_results_document_no": document_no_list,
        "community_results_board_no": board_no_list,
        "chat_results_session_id": session_ids
    }

@app.post("/search/community")
async def community_search(search_query: SearchQuery):
    community_results = search_community(search_query.query, search_query.search_size)[:search_query.top_k]
    board_no_list = [result[0]["board_no"] for result in community_results]
    return {"board_no_list": board_no_list}

@app.post("/search/chat-history")
async def chat_history_search(search_query: SearchQuery):
    chat_results = search_chat(search_query.query, search_query.user_no, search_query.search_size)[:search_query.top_k]
    session_ids = [result[0]["session_id"] for result in chat_results]
    return {"session_ids": session_ids}
