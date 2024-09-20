import os
import psycopg2
import numpy as np
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

user, password, host = os.getenv("DB_USER"), os.getenv("DB_PASSWORD"), os.getenv("DB_HOST")

def connect_db():
    connection = psycopg2.connect(user=user, password=password, host=host, port=5432)
    return connection

def create_table_columns(connection):
    try:
        cur = connection.cursor()
        alter_table_query = """
        ALTER TABLE data_document
            ADD COLUMN IF NOT EXISTS behavior TEXT,
            ADD COLUMN IF NOT EXISTS analysis TEXT,
            ADD COLUMN IF NOT EXISTS solution TEXT,
            ADD COLUMN IF NOT EXISTS behavior_emb VECTOR(4096),
            ADD COLUMN IF NOT EXISTS behavior_analysis_emb VECTOR(4096);
        """
        cur.execute(alter_table_query)
        connection.commit()
    except Exception as e:
        print(f"Error executing query: {e}")
    finally:
        cur.close()


def close_connection(connection):
    """Close the database connection."""
    connection.close()
