import trafilatura
import re

def crawl_data(url):

    downloaded = trafilatura.fetch_url(url)
    if downloaded:
        crawling_data = trafilatura.extract(downloaded)
        if crawling_data:
            crawling_data = re.sub(r'댓글목록.*?등록된 댓글이 없습니다\.', '', crawling_data, flags=re.DOTALL)
            question_parts = crawling_data.split("페이지 정보", 1)
            question = question_parts[0].strip() if len(question_parts) > 1 else ""
            small_titles = re.findall(r'●\s*(.*?)(?=\n●|\Z)', crawling_data, re.DOTALL)
            answers = re.findall(r'▷\s*(.*?)(?=\n▷|\Z)', crawling_data, re.DOTALL)
            return question, small_titles, answers
    return None, [], []



