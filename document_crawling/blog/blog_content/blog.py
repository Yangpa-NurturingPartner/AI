import blog_link
import blog_content

def main():
    # 모든 블로그 링크를 가져옵니다
    blog_links = blog_link.BlogLink().get_link()

    for url in blog_links:
        title, content = blog_content.BlogContent().get_content(url)

        if title and content:
            print("제목:", title)
            print("내용:", content)
            print("-------------------")  # 각 기사 사이에 구분선을 추가합니다

if __name__ == "__main__":
    main()