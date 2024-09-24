import pandas as pd
from opensearch_module.upload_vdb_fields import connect_to_opensearch, create_index, index_data

def main():
    # Step 1: Connect to OpenSearch
    client = connect_to_opensearch()
    
    # Step 2: Create the index
    index_name = 'document_data'
    create_index(client, index_name)
    
    # Step 4: Ensure the necessary columns exist
    required_columns = ['video_no', 'prob_no', 'behavior', 'analysis', 'solution', 'behavior_emb', 'behavior_analysis_emb']
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"Missing one or more required columns: {required_columns}")

if __name__ == "__main__":
    main()
