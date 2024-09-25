import json
import numpy as np
import pandas as pd
from opensearchpy import OpenSearch
from dotenv import load_dotenv
from properties import Properties
import create_opensearch

# 1.오픈서치 연결
properties_instance = Properties()
client = properties_instance.opensearch()

# 2. 인덱스 생성
index_name = 'document_data'
create_opensearch.CreateOpensearch.create_index(client, index_name)

# 3. 데이터 업로드
