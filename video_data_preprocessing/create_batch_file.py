import os
import json
import pandas as pd
from copy import deepcopy
from openai import OpenAI
from pytubefix import YouTube
from dotenv import load_dotenv 
from moviepy.editor import VideoFileClip
load_dotenv()
openai_api_key = os.getenv("OPENAI_API_KEY")
data = pd.read_csv("filtered_playlist_video_data.csv")
client = OpenAI(api_key=openai_api_key)

system_prompt = """
당신은 영상 대본 요약 전문가입니다.
이 영상에서는 한 아이의 다양한 문제 행동과 박사님의 문제 행동에 대한 분석, 그리고 해결방안을 다룹니다. 
제공된 대본을 요약할 때는 "아이의" 각 문제행동의 구체적인 상황과 맥락을 고려하고, 문제행동에 대한 원인 분석 및 해결방안을 포함하여 요약하세요. 

첫째, 각 "아이"의 문제 행동은 문제의 원인, 배경, 맥락을 명확히 인식하여 개별적으로 자세히 작성하세요
둘째, 문제행동에 대한 분석은 최대한 자세히 요약하세요
셋쨰, 해결방안은 최대한 자세히 요약하세요

답변은 다음 형식을 따르세요:

1. 아이의 문제행동: 걷거나 뛰는 등 기본적인 신체 활동을 회피하고, 주로 누워있거나 앉아있는 시간을 많이 보냅니다.
   문제행동 분석: 중력을 다루는 능력이 부족하여 신체 활동에 대한 불안이 크기 때문입니다. 신체 활동에 대한 두려움이 있고 회피하려는 경향을 보입니다.
   해결방안:  아이의 신체적 활동을 늘리기 위해 재미있고 흥미로운 운동 프로그램을 도입할 수 있습니다. 예를 들어, 놀이를 통해 자연스럽게 운동을 하도록 유도하는 방법입니다. 또한, 물리치료나 운동치료를 통해 중력을 다루는 능력을 향상시키는 것이 필요합니다. 부모는 아이가 신체 활동을 즐길 수 있도록 긍정적인 피드백을 주고, 함께 활동하는 시간을 늘려야 합니다.

2. 아이의 문제행동:
   문제행동 분석 :
   해결방안:
"""

template = {
        "custom_id": None,
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": "gpt-4o",
            "temperature": 0,
            "messages": [
                {
                    "role": "system",
                    "content": system_prompt
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": None}  # 여기 text 부분은 빈 상태로 유지
                        ]
                }
            ],
        }
    }


def get_audio_file():
    for index, row in data.iterrows():
        url = row['url']
        video_no = row['video_no']

        # 이미 오디오 파일이 있는 경우 스킵
        if os.path.exists(f'./Audios/{video_no}.mp3'):
            print(f"Audio file './Audios/{video_no}.mp3' already exists, skipping download and extraction.")
            continue  # 이미 파일이 있으면 스킵
        
        # Download the video
        yt = YouTube(url)
        stream = yt.streams.first()
        stream.download(output_path="./Videos", filename=f'{video_no}.mp4')

        # Extract the audio
        video = VideoFileClip(f'./Videos/{video_no}.mp4')
        video.audio.write_audiofile(f'./Audios/{video_no}.mp3', bitrate="32k")
        video.audio.close()
        video.close()

        # Delete the downloaded video file
        os.remove(f'./Videos/{video_no}.mp4')

def make_transcription():
    folder_path = './Audios'
    df = pd.DataFrame(columns=['video_no', 'transcription_text'])
    for file_name in sorted(os.listdir(folder_path), key=lambda x: int(os.path.splitext(x)[0])):
        audio_path = folder_path + "/" + file_name
        print(f"{file_name} transcription started!")
        transcription = client.audio.transcriptions.create(
            model="whisper-1",
            file=open(audio_path, "rb"),
            )
        print(f"{file_name} transcription completed!")
        video_no = os.path.splitext(file_name)[0]
        new_row = {'video_no': video_no, 'transcription_text': transcription.text}
        df.loc[video_no] = new_row
        df.to_csv("./transcriptions.csv", index=False)

if __name__=="__main__":
    # get_audio_file()
    # make_transcription()
    transcriptions = pd.read_csv("./transcriptions.csv")
    batches = []
    for index, row in data.iterrows():
        temp = deepcopy(template)
        temp['custom_id'] = f"{row['video_no']}"
        temp['body']['messages'][1]['content'][0]['text'] = f"영상 대본은 : {transcriptions.loc[transcriptions['video_no']==row['video_no']]['transcription_text'].values[0]}"
        batches.append(temp)
    with open("./video_data_preprocessing/batch_input.jsonl", "w") as file:
        for item in batches:
            json_string = json.dumps(item)
            file.write(json_string + "\n")
