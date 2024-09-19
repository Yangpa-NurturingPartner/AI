from openai import OpenAI 
import os
import time
import json
import pandas as pd
from dotenv import load_dotenv
load_dotenv()
openai_api_key = os.getenv("OPENAI_API_KEY")

MODEL="gpt-4o"
client = OpenAI(api_key=openai_api_key)

def request_batch(file_name):
    
    batch_input_file = client.files.create(
    file=open(file_name, "rb"),
    purpose="batch"
    )

    batch_job = client.batches.create(
        input_file_id=batch_input_file.id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata={
        "description": "nightly eval job"
        }
    )

    return batch_job

if __name__=="__main__":
    forder_path = "./video_data_preprocessing/batch_inputs"
    for batch_file_idx, file_name in enumerate(sorted(os.listdir(forder_path), key=lambda x: os.path.splitext(x)[0])):
        if batch_file_idx == 0: continue
        batch_job = request_batch(forder_path + "/" + file_name)

        while True:
            batch_job  = client.batches.retrieve(batch_job.id)
            if batch_job.status=="completed": break
            time.sleep(10)
        print(f"{file_name} batch job completed!")

        result_file_id = batch_job.output_file_id
        result = client.files.content(result_file_id).content
        result_file_name = f"./video_data_preprocessing/batch_results/batch_result_{batch_file_idx}.jsonl"
        with open(result_file_name, 'wb') as file:
            file.write(result)

        video_data = pd.read_csv("./summarized_video_data.csv")
        with open(result_file_name, 'r', encoding='utf-8') as file:
            for line in file:
                data = json.loads(line)
                idx = data['custom_id']
                content = data['response']['body']['choices'][0]['message']['content']
                video_data.at[int(idx), 'response'] = content
        video_data.to_csv("./summarized_video_data.csv", index=False, encoding='utf-8-sig')

# print(response.choices[0].message.content)
# content = data['response']['body']['choices'][0]['message']['content']

