from link_crawling import LinkCrawling
from content_crawling import ContentCrawling

def main():
    # 링크 크롤링
    linkcrawler = LinkCrawling()
    links = linkcrawler.link()

    # 콘텐츠 크롤링
    contentcrawler = ContentCrawling()
    
    for link in links:

        content = contentcrawler.content(link)

        print(f"URL: {link}")
        print(content)
        print("-" * 50)

if __name__ == "__main__":
    main()