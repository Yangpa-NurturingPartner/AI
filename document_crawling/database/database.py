from properties import Properties

class Database:

    def __init__(self):
        self.properties_instance = Properties()

    # 1. postgre 연결 및 데이터 삽입
    def process_content(self, results):

        connection = self.properties_instance.sql()
        cursor = connection.cursor()

        insert_query = """
        INSERT INTO data_document(document_no, title, url)
        VALUES (%s, %s, %s)
        """
        
        for result in results:
            idx, title, url, _ = result  # 네 번째 값은 무시합니다.
            cursor.execute(insert_query, (idx, title, url))
        
        connection.commit()
        cursor.close()
        connection.close()


    # 2. opensearch 연결 및 데이터 삽입
    def process_and_upload_to_opensearch(self, summaries, embeddings):

        client = self.properties_instance.opensearch()

        for idx, (summary, embedding) in enumerate(zip(summaries, embeddings)):
            document = {
                "title": summary.get("제목"),
                "behavior": summary.get("아이의 문제행동"),
                "analysis": summary.get("문제행동 분석"),
                "solution": summary.get("해결방안"),
                "behavior_plus_analysis": summary.get("아이의 문제행동 + 문제행동 분석"),
                "behavior_emb": embedding.get("아이의 문제행동 임베딩"),
                "behavior_plus_analysis_emb": embedding.get("아이의 문제행동 + 문제행동 분석 임베딩")
            }

            response = client.index(
                index="document_data",  # 인덱스 이름을 적절히 변경하세요
                id=idx,
                body=document
            )

        # 인덱싱 결과를 로그로 출력
        print(f"문서 ID {idx} 인덱싱 응답: {response}")

        # 에러 처리 예제
        if response.get('result') != 'created':
            print(f"문서 ID {idx} 인덱싱 오류: {response}")

        return "데이터가 OpenSearch에 성공적으로 업로드되었습니다"
        







    