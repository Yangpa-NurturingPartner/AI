from properties import Properties
import pandas as pd 
class SummaryAndEmbedding:

    def summary_and_embedding(self,results):

        # 0. 프로퍼티 설정 
        properties = Properties()

        # 1. OpenAPI 연결
        openai_client = properties.api_key()  # client 객체 반환하지 않음

        # 2. 모델 선택 및 설정
        gpt_model, system_prompt = properties.model()

        # 3. 임베딩 클라이언트
        embedding_client = properties.embedding_model()

        #ㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡ

        summaries = []
        embeddings = []

        for data in results:
            # data가 튜플인 경우 딕셔너리로 변환
            if isinstance(data, tuple):
                if len(data) == 4:
                    data = {
                        "id": data[0],
                        "제목": data[1],
                        "url": data[2],
                        "내용": data[3]
                    }
                else:
                    raise ValueError(f"data 튜플을 딕셔너리로 변환할 수 없습니다: {data}")

            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": data["내용"]}
            ]

            response = openai_client.chat.completions.create(
                model=gpt_model,
                messages=messages,
                temperature=0,
            )
            

            summary = response.choices[0].message.content

            if "아이의 문제행동:" in summary:
                behavior = summary.split("아이의 문제행동:")[1].split("문제행동 분석:")[0].strip()
                analysis = summary.split("문제행동 분석:")[1].split("해결방안:")[0].strip()
                solution = summary.split("해결방안:")[1].strip()
                behavior_analysis = behavior + "\n" + analysis

                summaries.append({
                    "title": data["제목"],
                    "behavior": behavior,
                    "analysis": analysis,
                    "solution": solution,
                    "behavior_analysis": behavior_analysis
                })

                # 임베딩 생성
                if behavior:
                    behavior_embedding = embedding_client.embeddings.create(
                        model="solar-embedding-1-large-passage",
                        input=behavior
                    ).data[0].embedding

                if behavior_analysis:
                    behavior_plus_analysis_embedding = embedding_client.embeddings.create(
                        model="solar-embedding-1-large-passage",
                        input=behavior_analysis
                    ).data[0].embedding

                embeddings.append({
                    "behavior_emb": behavior_embedding if behavior else None,
                    "behavior_analysis_emb": behavior_plus_analysis_embedding if behavior_plus_analysis else None
                })
            else:
                summaries.append({
                    "title": data["제목"],
                    "summary": summary.strip()
                })

        # summaries와 embeddings를 데이터 프레임으로 변환하여 CSV 파일로 저장
        summaries_df = pd.DataFrame(summaries)
        embeddings_df = pd.DataFrame(embeddings)
        summaries_df.to_csv('summaries.csv', index=False)
        embeddings_df.to_csv('embeddings.csv', index=False)

        return summaries, embeddings