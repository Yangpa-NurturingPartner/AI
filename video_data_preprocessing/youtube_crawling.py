# 먼저 재생목록에서 30분 넘어가는 영상들만 필터링 + 챕터에서 회차수가 2회 이하인 영상들만 필터링
# 오은영쌤 재생목록 안의 영상들의 제목, url, 업로드날짜 추출
from pytube import YouTube
from moviepy.editor import VideoFileClip
import pandas as pd
from googleapiclient.discovery import build
import re
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_SERVICE_VERSION = 'v3'

youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_SERVICE_VERSION, developerKey=YOUTUBE_API_KEY)

def get_videos_from_playlist(playlist_id):
    videos = []
    next_page_token = None
    while True:
        try:
            playlist_items_response = youtube.playlistItems().list(
                part='snippet,contentDetails',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()

            for item in playlist_items_response['items']:
                video_id = item['snippet']['resourceId']['videoId']
                title = item['snippet']['title']
                upload_date = item['snippet']['publishedAt']
                videos.append((video_id, title, upload_date))

            next_page_token = playlist_items_response.get('nextPageToken')
            if not next_page_token:
                break

        except Exception as e:
            print(f"An error occurred: {e}")
            break

    return videos

def get_video_details(video_id):
    try:
        video_response = youtube.videos().list(
            part='snippet,contentDetails',
            id=video_id
        ).execute()

        item = video_response['items'][0]
        duration = item['contentDetails']['duration']
        description = item['snippet']['description']
        
        # Parse duration
        duration_obj = parse_duration(duration)
                
        return duration_obj, description
    except Exception as e:
        return None, "No description found"

def parse_duration(duration):
    match = re.match(r'PT(\d+H)?(\d+M)?(\d+S)?', duration)
    if not match:
        return timedelta()
    
    hours = int(match.group(1)[:-1]) if match.group(1) else 0
    minutes = int(match.group(2)[:-1]) if match.group(2) else 0
    seconds = int(match.group(3)[:-1]) if match.group(3) else 0
    
    return timedelta(hours=hours, minutes=minutes, seconds=seconds)

def count_episodes(chapters):
    episode_count = len(re.findall(r'\d+회', chapters))
    return episode_count

def main(playlist_id):
    videos = get_videos_from_playlist(playlist_id)
    video_data = []

    for video_id, title, upload_date in videos:
        duration, chapters = get_video_details(video_id)
        
        if duration and duration >= timedelta(minutes=30):
            episode_count = count_episodes(chapters)
            
            if episode_count <= 2:
                youtube_url = f"https://www.youtube.com/watch?v={video_id}"
                upload_date = datetime.strptime(upload_date, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d")
                
                video_data.append({
                    "title": title,
                    "url": youtube_url,
                    "upload_date": upload_date
                })

    df = pd.DataFrame(video_data)
    print(len(df))  # 결과 출력
    df.to_csv("./filtered_playlist_video_data.csv", index=False, encoding='utf-8-sig')  # CSV 파일로 저장


if __name__=="__main__":
    # 특정 재생목록 ID로 테스트 실행
    playlist_id = 'PLl9GPcxBUXInqV6GfDJqs1VYQWxquNZ8u'  # 원하는 재생목록 ID로 변경
    main(playlist_id)
