from data_processing.crawler.crawler import Crawler

if __name__ == "__main__":
    crawler = Crawler()
    qna_results = crawler.crawl_qna()
    print(qna_results)
