from crawler.crawler import Crawler

if __name__ == "__main__":
    crawler = Crawler()
    make_up_results = crawler.crawl_make_up()
    print(make_up_results)
