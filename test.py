import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from openai import OpenAI
from opensearchpy import OpenSearch
from dotenv import load_dotenv
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
client = OpenAI(
    api_key=UPSTAGE_API_KEY,
    base_url="https://api.upstage.ai/v1/solar"
)
opensearch_client = OpenSearch(
    hosts=[{"host": OPENSEARCH_HOST, "port": 9200}],
    http_auth=(OPENSEARCH_USER, OPENSEARCH_PASSWORD),
    use_ssl=True,
    verify_certs=False
)
class CommunityContent(BaseModel):
    content: str
    board: int
    community: int
@app.post("/embedded/community/contents")
async def embedCommunity(content_data: CommunityContent):
    try:
        embedding_response = client.embeddings.create(
            model="solar-embedding-1-large-passage",
            input=content_data.content
        )
        embeddings = embedding_response.data[0].embedding
        document = {
            "title_emb": embeddings,
            "board_no": content_data.board,
            "community_no": content_data.community
        }
        response = opensearch_client.index(
            index="data_community",
            body=document
        )
        if response["result"] == "created":
            return {"status": 200, "why": ""}
        else:
            return {"status": 500, "why": "OpenSearch 저장 실패"}
    except Exception as e:
        return {"status": 500, "why": str(e)}