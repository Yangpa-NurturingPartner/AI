import os
from dotenv import load_dotenv
import openai
import psycopg2

class Properties:

    # 1. PostgreSQL 연결
    def sql(self):
        load_dotenv()
        user, password, host = os.getenv("DB_USER"), os.getenv("DB_PASSWORD"), os.getenv("DB_HOST")

        def connect_db():
            connection = psycopg2.connect(user=user, password=password, host=host, port=5432)
            return connection
        
        return connect_db
        
    # 2. OpenAPI, embedding API Key 연결
    def api_key(self):
        load_dotenv()
        OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
        embedding_api_key = os.getenv("UPSTAGE_API_KEY")

        openai.api_key = OPENAI_API_KEY
        client = openai.OpenAI(api_key=OPENAI_API_KEY)

        return embedding_api_key, client

    # 3. 모델 선택 및 설정
    def model(self):
        load_dotenv()
        model = "gpt-4o-mini"
        system_prompt = """
        넌 내용 요약 전문가야.
        이 정보에서는 한 아이의 문제 행동과 박사님의 문제 행동에 대한 분석, 그리고 해결방안을 다뤄.
        제공된 정보를 요약할 때는 "아이의" 각 문제행동의 구체적인 상황과 맥락을 고려하고, 문제행동에 대한 원인 분석 및 해결방안을 포함하여 요약해.

        규칙이 있어.
        1. 각 "아이"의 문제 행동은 문제의 원인, 배경, 맥락을 명확히 인식하여 개별적으로 자세히 작성해.
        2. 문제행동에 대한 분석은 최대한 자세히 요약해.
        3. 해결방안은 최대한 자세히 요약해.
        4. 없는 얘기는 작성하지 말고 요약만 해.
        5. 문제행동, 분석, 해결방안 중 없는 부분은 빈칸으로 냅둬.

        6. 답변은 다음 형식을 따라 :

        아이의 문제행동: 걷거나 뛰는 등 기본적인 신체 활동을 회피하고, 주로 누워있거나 앉아있는 시간을 많이 보냅니다.
        문제행동 분석: 중력을 다루는 능력이 부족하여 신체 활동에 대한 불안이 크기 때문입니다. 신체 활동에 대한 두려움이 있고 회피하려는 경향을 보입니다.
        해결방안:  아이의 신체적 활동을 늘리기 위해 재미있고 흥미로운 운동 프로그램을 도입할 수 있습니다. 예를 들어, 놀이를 통해 자연스럽게 운동을 하도록 유도하는 방법입니다. 또한, 물리치료나 운동치료를 통해 중력을 다루는 능력을 향상시키는 것이 필요합니다. 부모는 아이가 신체 활동을 즐길 수 있도록 긍정적인 피드백을 주고, 함께 활동하는 시간을 늘려야 합니다.
        """
        return model, system_prompt
    
    # 4. OpenSearch 연결
    def opensearch(self):
        load_dotenv()
        host = "192.168.0.152"
        OPENSEARCH_KEY = os.getenv("OPENSEARCH_KEY")

        client = OpenSearch(
            hosts=[{'host': host, 'port': 9200}],
            http_auth=('admin', OPENSEARCH_KEY),
            use_ssl=True,
            verify_certs=False
        )
        return client
    
    #5. 임베딩 모델 사용
    def embedding_model(self):
        load_dotenv()
        embedding_api_key = os.getenv("UPSTAGE_API_KEY")

        embedding_client = openai.OpenAI(
            api_key=embedding_api_key,
            base_url="https://api.upstage.ai/v1/solar"
        )
        return embedding_client
