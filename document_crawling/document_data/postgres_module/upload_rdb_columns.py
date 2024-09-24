import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
user, password, host = os.getenv("DB_USER"), os.getenv("DB_PASSWORD"), os.getenv("DB_HOST")

def connect_db():
    """Connect to PostgreSQL database."""
    connection = psycopg2.connect(user=user, password=password, host=host, port=5432)
    return connection

def filter_unique_videos(df):
    """Filter unique videos with only one title, url, and upload_date per video_no."""
    # Drop duplicate rows for video_no while keeping the first occurrence of title, url, and upload_date
    unique_videos_df = df[['video_no', 'title', 'url', 'upload_date']].drop_duplicates(subset=['video_no'])
    return unique_videos_df

def load_csv_to_dataframe(csv_file_path):
    """Load final_data.csv into a DataFrame."""
    return pd.read_csv(csv_file_path)

def merge_dataframes(df_csv, df_embeddings):
    """Merge final_data.csv with the embeddings DataFrame."""
    return pd.merge(df_csv, df_embeddings, on=['video_no', 'prob_no'], how='left')

def fetch_embeddings(connection):
    """Fetch behavior_emb and behavior_analysis_emb from PostgreSQL."""
    cur = connection.cursor()
    try:
        fetch_query = "SELECT video_no, prob_no, behavior_emb, behavior_analysis_emb FROM data_video;"
        cur.execute(fetch_query)
        rows = cur.fetchall()
        columns = ['video_no', 'prob_no', 'behavior_emb', 'behavior_analysis_emb']
        df_embeddings = pd.DataFrame(rows, columns=columns)
        return df_embeddings
    except psycopg2.Error as e:
        print(f"Error fetching data: {e}")
    finally:
        cur.close()

def drop_existing_table(connection):
    """Drop the existing data_video table."""
    cur = connection.cursor()
    try:
        drop_query = "DROP TABLE IF EXISTS data_video;"
        cur.execute(drop_query)
        connection.commit()
        print("Existing table dropped successfully.")
    except psycopg2.Error as e:
        print(f"Error dropping table: {e}")
        connection.rollback()
    finally:
        cur.close()

def create_new_table(connection):
    """Create the new data_video table with appropriate columns."""
    cur = connection.cursor()
    try:
        create_query = """
        CREATE TABLE data_video (
            video_no BIGINT PRIMARY KEY,
            title VARCHAR(255),
            url VARCHAR(255),
            upload_date DATE
        );
        """
        cur.execute(create_query)
        connection.commit()
        print("New table created successfully.")
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
        connection.rollback()
    finally:
        cur.close()

def upload_videos_to_db(connection, df_unique_videos):
    """Insert the unique video data into the PostgreSQL data_video table."""
    cur = connection.cursor()
    try:
        for index, row in df_unique_videos.iterrows():
            insert_query = """
            INSERT INTO data_video (video_no, title, url, upload_date)
            VALUES (%s, %s, %s, %s);
            """
            cur.execute(insert_query, (row['video_no'], row['title'], row['url'], row['upload_date']))
        connection.commit()
        print("Unique video data uploaded successfully.")
    except psycopg2.Error as e:
        print(f"Error uploading video data: {e}")
        connection.rollback()
    finally:
        cur.close()

def close_connection(connection):
    """Close the database connection."""
    connection.close()


