import os
import psycopg2
import numpy as np
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
user, password, host = os.getenv("DB_USER"), os.getenv("DB_PASSWORD"), os.getenv("DB_HOST")

def connect_db():
    """Connect to PostgreSQL database."""
    connection = psycopg2.connect(user=user, password=password, host=host, port=5432)
    return connection

def alter_primary_key(connection):
    """Remove the primary key from video_no and set prob_no as the new primary key."""
    cur = connection.cursor()
    try:
        # Step 1: Remove Primary Key from video_no
        cur.execute("ALTER TABLE data_video DROP CONSTRAINT IF EXISTS data_video_pkey;")
        
        # Step 2: Set prob_no as the new Primary Key
        cur.execute("ALTER TABLE data_video ADD CONSTRAINT data_video_pkey PRIMARY KEY (prob_no);")
        
        connection.commit()
        print("Primary Key altered successfully.")
    except psycopg2.Error as e:
        print(f"Error altering primary key: {e}")
        connection.rollback()
    finally:
        cur.close()

def create_pgvector_extension(connection):
    """Create pgvector extension in the PostgreSQL database."""
    cur = connection.cursor()
    try:
        # Try to create the pgvector extension
        create_extension_query = "CREATE EXTENSION IF NOT EXISTS vector;"
        cur.execute(create_extension_query)
        connection.commit()
        print("pgvector extension created successfully.")
    except psycopg2.Error as e:
        print(f"Error creating pgvector extension: {e}")
        connection.rollback()
    finally:
        cur.close()

def create_table_columns(connection):
    """Create columns for behavior, analysis, solution, etc."""
    cur = connection.cursor()
    alter_table_query = """
    ALTER TABLE data_video 
    ALTER COLUMN title TYPE varchar(255),
    ALTER COLUMN url TYPE varchar(255),
    ADD COLUMN IF NOT EXISTS video_no BIGINT,
    ADD COLUMN IF NOT EXISTS prob_no BIGINT,
    ADD COLUMN IF NOT EXISTS behavior TEXT,
    ADD COLUMN IF NOT EXISTS analysis TEXT,
    ADD COLUMN IF NOT EXISTS solution TEXT;
    """
    cur.execute(alter_table_query)
    connection.commit()
    cur.close()

def create_embedding_columns(connection):
    """Create vector columns for embeddings using pgvector."""
    cur = connection.cursor()
    alter_table_query_2 = """
    ALTER TABLE data_video 
    ADD COLUMN IF NOT EXISTS behavior_emb VECTOR(4096),
    ADD COLUMN IF NOT EXISTS behavior_analysis_emb VECTOR(4096);
    """
    cur.execute(alter_table_query_2)
    connection.commit()
    cur.close()

def upload_data_from_dataframe(connection, df):
    """Insert data from DataFrame into the database."""
    cur = connection.cursor()
    for index, row in df.iterrows():
        insert_query = """
        INSERT INTO data_video (video_no, title, url, upload_date, prob_no, behavior, analysis, solution)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (prob_no) DO NOTHING;
        """
        cur.execute(insert_query, (row['video_no'], row['title'], row['url'], row['upload_date'], 
                                   row['prob_no'], row['behavior'], row['analysis'], row['solution']))
    connection.commit()
    cur.close()

def close_connection(connection):
    """Close the database connection."""
    connection.close()
