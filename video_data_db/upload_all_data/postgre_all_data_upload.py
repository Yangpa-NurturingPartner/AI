import pandas as pd
from video_data_db.postgres_module.upload_all import connect_db, create_table_columns, create_embedding_columns, create_pgvector_extension,alter_primary_key, upload_data_from_dataframe, close_connection
from video_data_db.postgres_module.embedding import update_behavior_embedding, update_behavior_analysis_embedding

def main():
    # 1. 데이터베이스 연결
    connection = connect_db()
    # create_pgvector_extension(connection)
    # alter_primary_key(connection)

    # 2. 테이블에 칼럼 추가
    create_table_columns(connection)
    create_embedding_columns(connection)


    # 3. 데이터프레임에서 데이터 업로드 (예시 DataFrame)
    df = pd.read_csv("./video_data_preprocessing/final_data.csv")
    upload_data_from_dataframe(connection, df)

    # 4. behavior 값에 대한 임베딩 업데이트
    update_behavior_embedding(connection)

    # 5. behavior + analysis 값에 대한 임베딩 업데이트
    update_behavior_analysis_embedding(connection)

    # 6. 데이터베이스 연결 종료
    close_connection(connection)

if __name__ == "__main__":
    main()
