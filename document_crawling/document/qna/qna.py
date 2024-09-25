import trafilatura
import re

class QnACrawler:
   
    def crawl_data(self, url):
        downloaded = trafilatura.fetch_url(url)
        
        if downloaded:
            crawling_data = trafilatura.extract(downloaded)
            if crawling_data:
                crawling_data = re.sub(r'댓글목록.*?등록된 댓글이 없습니다\.', '', crawling_data, flags=re.DOTALL)
                question_parts = crawling_data.split("페이지 정보", 1)
                question = question_parts[0].strip() if len(question_parts) > 1 else ""
                small_titles = re.findall(r'●\s*(.*?)(?=\n●|\Z)', crawling_data, re.DOTALL)
                answers = re.findall(r'▷\s*(.*?)(?=\n▷|\Z)', crawling_data, re.DOTALL)

                title = question
                # small_titles와 answers를 문자열로 변환하여 연결
                small_titles_str = ','.join(small_titles)
                answers_str = ','.join(answers)
                content = small_titles_str + "," + answers_str

                return title, content
            
        return None, "", ""