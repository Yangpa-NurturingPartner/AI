from postgres_module.upload_rdb_columns import connect_db, filter_unique_videos, load_csv_to_dataframe, merge_dataframes, fetch_embeddings, drop_existing_table, create_new_table, upload_videos_to_db, close_connection

def main():
    connection = connect_db()
     # Step 2: Load final_data.csv into a DataFrame
    csv_file_path = "./video_data_db/final_data.csv"  # Update the path to your CSV file
    df_merged = load_csv_to_dataframe(csv_file_path)
    # df_embeddings = fetch_embeddings(connection)
    # df_merged = merge_dataframes(df_csv, df_embeddings)
    # df_merged.to_csv(csv_file_path)

    # Step 5: Filter unique videos (one row per video_no for title, url, upload_date)
    df_unique_videos = filter_unique_videos(df_merged)
    # Step 6: Drop the existing table
    drop_existing_table(connection)
    # Step 7: Create the new data_video table with the updated schema
    create_new_table(connection)
    # Step 8: Upload the unique video data to the new table
    upload_videos_to_db(connection, df_unique_videos)
    # Step 9: Close the connection
    close_connection(connection)

if __name__=="__main__":
    main()