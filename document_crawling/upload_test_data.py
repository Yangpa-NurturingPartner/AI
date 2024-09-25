import json
import numpy as np
from opensearchpy import OpenSearch
from properties import Properties
from database.opensearch import create_opensearch

def main():
    # 1. 오픈서치 연결
    properties_instance = Properties()
    client = properties_instance.opensearch()

    # 2. 인덱스 삭제
    index_name = 'document_data'
    delete_index(client, index_name)


def delete_index(client, index_name):
    if client.indices.exists(index=index_name):
        response = client.indices.delete(index=index_name)
        print(f"Index '{index_name}' deleted: {response['acknowledged']}")

        

    

if __name__ == "__main__":
    main()