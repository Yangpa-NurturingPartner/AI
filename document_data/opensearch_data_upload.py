import pandas as pd
from opensearch_module.upload_vdb_fields import connect_to_opensearch, create_index, index_data

def main():
    # Step 1: Connect to OpenSearch
    client = connect_to_opensearch()
    
    # Step 2: Create the index
    index_name = 'data_video'
    create_index(client, index_name)
    
    # Step 3: Load the data from CSV
    csv_file_path = "./video_data_db/final_data.csv"  # Update this to your actual path
    df = pd.read_csv(csv_file_path)
    
    # Step 4: Ensure the necessary columns exist
    required_columns = ['video_no', 'prob_no', 'behavior', 'analysis', 'solution', 'behavior_emb', 'behavior_analysis_emb']
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"Missing one or more required columns: {required_columns}")
    
    # Step 5: Index the data into OpenSearch
    df.apply(lambda row: index_data(client, index_name, row), axis=1)

if __name__ == "__main__":
    main()
