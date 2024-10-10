import os
import sys
from openai import OpenAI
from dotenv import load_dotenv
from opensearchpy import OpenSearch
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from hybrid_search.hybrid_search_opensearch import perform_searches, tmmcc_hybrid_search_with_results , perform_multi_searches

load_dotenv()
OPENSEARCH_KEY = os.getenv("OPENSEARCH_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
opensearch_client = OpenSearch(
    hosts=[{'host': "192.168.0.152", 'port': 9200}],
    http_auth=('admin', OPENSEARCH_KEY),
    use_ssl=True,
    verify_certs=False
)
openai_client = OpenAI(api_key=OPENAI_API_KEY)

def search_video_document(query, size, bm25_weight=0.2, vector_weight=0.8):
    index_names = ["data_video", "data_document"]
    text_field, vector_field = "behavior_analysis", "behavior_analysis_emb"
    meta_field = ['behavior', 'analysis', 'solution']
    bm25_result, vector_result = perform_multi_searches(query, index_names, text_field, vector_field, size, meta_field)
    tmmcc_results = tmmcc_hybrid_search_with_results(bm25_result, vector_result, bm25_weight=bm25_weight, vector_weight=vector_weight)
    return tmmcc_results

def search_video(query, size, bm25_weight=0.2, vector_weight=0.8):
    index_name, meta_field = 'data_video', ["video_no"]
    text_field, vector_field = "behavior_analysis", "behavior_analysis_emb"
    bm25_result, vector_result = perform_searches(query, index_name, text_field, vector_field, size, meta_field)
    tmmcc_results = tmmcc_hybrid_search_with_results(bm25_result, vector_result, bm25_weight=bm25_weight, vector_weight=vector_weight)
    return tmmcc_results
    
def search_document(query, size, bm25_weight=0.2, vector_weight=0.8):
    index_name, meta_field = 'data_document', ["document_no"]
    text_field, vector_field = "behavior_analysis", "behavior_analysis_emb"
    bm25_result, vector_result = perform_searches(query, index_name, text_field, vector_field, size, meta_field)
    tmmcc_results = tmmcc_hybrid_search_with_results(bm25_result, vector_result, bm25_weight=bm25_weight, vector_weight=vector_weight)
    return tmmcc_results
    
def search_community(query, size, bm25_weight=0.2, vector_weight=0.8):
    index_name, meta_field = 'data_community', ["board_no"]
    text_field, vector_field = "content", "content_emb"
    bm25_result, vector_result = perform_searches(query, index_name, text_field, vector_field, size, meta_field)
    tmmcc_results = tmmcc_hybrid_search_with_results(bm25_result, vector_result, bm25_weight=bm25_weight, vector_weight=vector_weight)
    return tmmcc_results

def search_chat(query, user_no, size, bm25_weight=0.2, vector_weight=0.8):
    index_name, meta_field = 'data_chat', ["session_id"]
    text_field, vector_field = "content", "content_emb"
    bm25_result, vector_result = perform_searches(query, index_name, text_field, vector_field, size, meta_field, field="chat", user_no=user_no)
    tmmcc_results = tmmcc_hybrid_search_with_results(bm25_result, vector_result, bm25_weight=bm25_weight, vector_weight=vector_weight)
    return tmmcc_results

def generate_rag_response(query, messages, results):
    MODEL_4o="gpt-4o"
    system_prompt = f"""
    당신은 육아 정보를 기반으로 육아 고민에 대한 답변을 하는 전문가입니다. 
    - 제공된 문맥에서 **사용자의 질문과 관련된 정보만 추출**하여 질문에 답을 하세요. 
    - 관련된 정보가 없을경우 도움이 될 수 있는 일반적인 조언을 제공하세요. 
    - 답변은 2000자를 넘기지 않도록 작성하세요. 
    - 답변은 문제행동, 분석, 해결방안으로 구분하지 말고, 자연스럽게 상담하듯이 작성하세요.
    - 답변은 '상황 설명'과 '솔루션'으로 구조화하여 작성하세요.
    - '상황 설명'과 '솔루션'뒤에는 \n을 사용하세요
    - 솔루션 내에서 번호를 매기고 \n\n을 사용하세요
    - '먼저', '또한', '마지막으로' 등의 표현은 사용하지 마세요.
    - 답변에서 줄바꿈과 번호 외의 **같은 마크다운 문자는 절대 사용하지 마세요.
    """
    content = ""
    for result in results:
        behavior = result[0]['behavior']
        analysis = result[0]['analysis']
        solution = result[0]['solution']
        content += f"아이의 문제행동 : {behavior}\n문제행동분석 : {analysis}\n해결방안 : {solution}\n\n"
    # print(content)
    messages.append({"role": "assistant", "content": f"검색 결과:\n{content}"})
    rag_messages = [{"role": "system", "content": system_prompt}] + messages

    response_4o = openai_client.chat.completions.create(
        model=MODEL_4o,
        messages=rag_messages,
        temperature=0,
    )
    respond = response_4o.choices[0].message.content
    results = [
        {"role":"user", "content":f"{query}"},
        {"role":"assistant", "content":f"{respond}"}
    ]
    return results