from properties import Properties

class SummaryAndEmbedding:

    def summary_and_embedding(self,results):

        # 0. 프로퍼티 설정 
        properties = Properties()

        # 1. OpenAPI 연결
        properties.api_key()  # client 객체 반환하지 않음

        # 2. 모델 선택 및 설정
        model, system_prompt = properties.model()

        # 3. 임베딩 클라이언트 설정
        properties.embedding_model()

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

            response = client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=0,
            )

            summary = response.choices[0].message.content

            if "아이의 문제행동:" in summary:
                problem_behavior = summary.split("아이의 문제행동:")[1].split("문제행동 분석:")[0].strip()
                behavior_analysis = summary.split("문제행동 분석:")[1].split("해결방안:")[0].strip()
                solution = summary.split("해결방안:")[1].strip()
                behavior_plus_analysis = problem_behavior + "," + behavior_analysis

                summaries.append({
                    "제목": data["제목"],
                    "아이의 문제행동": problem_behavior,
                    "문제행동 분석": behavior_analysis,
                    "해결방안": solution,
                    "아이의 문제행동 + 문제행동 분석": behavior_plus_analysis
                })

                 # 임베딩 생성
                behavior_embedding = openai.Embedding.create(
                    model="solar-embedding-1-large-passage",  # 모델 이름 유지
                    input=problem_behavior
                ).data[0].embedding

                behavior_plus_analysis_embedding = openai.Embedding.create(
                    model="solar-embedding-1-large-passage",  # 모델 이름 유지
                    input=behavior_plus_analysis
                ).data[0].embedding

                embeddings.append({
                    "아이의 문제행동 임베딩": behavior_embedding,
                    "아이의 문제행동 + 문제행동 분석 임베딩": behavior_plus_analysis_embedding
                })
            else:
                summaries.append({
                    "제목": data["제목"],
                    "요약": summary.strip()
                })

        return summaries, embeddings