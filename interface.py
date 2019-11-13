from scrapy.utils.project import get_project_settings

from indexer import *
import scrapy
from scrapy.crawler import CrawlerProcess
from mir.mir.spiders.scrapper import ScholarSpider


def scrapper(count=None,urls=None):
    settings = get_project_settings()
    settings.update({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
        'FEED_FORMAT': 'json',
        'FEED_URI': 'crawling.json',

    })
    process = CrawlerProcess(settings)
    if count and urls:
        process.crawl(ScholarSpider, count=count, urls=urls)
    elif count and not urls:
        process.crawl(ScholarSpider, count=count)
    elif not count and urls:
        process.crawl(ScholarSpider, urls=urls)
    else:
        process.crawl(ScholarSpider)
    process.start()


def indexing(crawling_path="crawling.json", elastic_url="localhost:9200"):
    elas = ElasticDao(elastic_url)
    elas.insert_scholars(crawling_path)


def delete_index(elastic_url="localhost:9200"):
    elas = ElasticDao(elastic_url)
    elas.delete()


def set_page_rank(elastic_url="localhost:9200", alpha=0.85):
    elas = ElasticDao(elastic_url)
    elas.set_page_ranks(alpha)


def search(elastic_url="localhost:9200", title="", title_score=0, abstract="",
           abstract_score=0, date=2010, date_score=0, page_rank_score=0, size=10):
    elas = ElasticDao(elastic_url)
    return pprint(elas.search(title, title_score, abstract,
                              abstract_score, date, date_score, page_rank_score, size))


def HITS(elastic_url="localhost:9200", n=10):
    elas = ElasticDao(elastic_url)
    return pprint(elas.HITS_authors(n))


def get_all_docs(elastic_url="localhost:9200"):
    elas = ElasticDao(elastic_url)
    return elas.get_all()


def get_doc(id, elastic_url="localhost:9200"):
    elas = ElasticDao(elastic_url)
    return elas.get_scholar(id)

def get_urls():
    urls = input("enter start urls with ',' between them or press enter to crawl with default urls ")
    if urls:
        return urls.split(",")
    return None

if __name__ == '__main__':
    try:
        while True:
            input_str = "\n\n1.crawl scholars\n" \
                    "2.indexing\n" \
                    "3.calculate page ranks\n" \
                    "4.search in docs\n" \
                    "5.sort authors by HITS algorithms\n\n" \
                    "Enter number of task to execute:\n"
            task = int(input(input_str))
            if task == 1:
                count = input("Enter number of docs that you want to crawl or press enter to crawl with default number:")
                if count:
                    count =int(count)
                else:
                    count = None
                urls = get_urls()
                scrapper(count, urls)
            elif task == 2:
                new_input_str = "\n1.create index and insert paper\n" \
                            "2.delete index\n" \
                                "3.get all papers\n" \
                                "4.get paper by id\n" \
                            "\nenter number of task to excute: "
                new_task = int(input(new_input_str))
                if new_task == 1:
                    elastic_url = input("enter elastic url:port or press enter for default value")
                    json_path = input("enter path of crawling json output file or press enter for default value")
                    if not elastic_url and not json_path:
                        indexing()
                    elif elastic_url and not json_path:
                        indexing(elastic_url=elastic_url)
                    elif not elastic_url and json_path:
                        indexing(json_path)
                    elif elastic_url and json_path:
                        indexing(json_path, elastic_url)
                if new_task == 2:
                    elastic_url = input("enter elastic url:port or press enter for default value")
                    if elastic_url:
                        delete_index(elastic_url)
                    else:
                        delete_index()
                if new_task == 3:
                    elastic_url = input("enter elastic url:port or press enter for default value")
                    if elastic_url:
                        pprint(get_all_docs(elastic_url))
                    else:
                        pprint(get_all_docs())
                if new_task == 4:
                    elastic_url = input("enter elastic url:port or press enter for default value")
                    doc_id = input("enter paper id")
                    if elastic_url:
                        pprint(get_doc(doc_id, elastic_url))
                    else:
                        pprint(get_doc(doc_id))
            elif task == 3:
                elastic_url = input("enter elastic url:port or press enter for default value")
                alpha = input("enter alpha for page rank")
                if elastic_url and alpha:
                    set_page_rank(elastic_url,eval(alpha))
                elif not elastic_url and alpha:
                    set_page_rank(alpha=eval(alpha))
                elif elastic_url and not alpha:
                    set_page_rank(elastic_url)
                else:
                    set_page_rank()
            elif task == 4:
                elastic_url = input("enter elastic url:port or press enter for default value")
                if not elastic_url:
                    elastic_url = "localhost:9200"
                title = input("title query:(press enter for skip this)")
                title_score = input("title query weight:(press enter for skip this)")
                if not title:
                    title = ""
                    title_score = 0
                if title_score:
                    title_score = int(title_score)
                else:
                    title_score = 1
                abstract = input("abstract query:(press enter for skip this)")
                abstract_score = input("abstract query weight:(press enter for skip this)")
                if not abstract:
                    abstract = ""
                    abstract_score = 0
                if abstract_score:
                    abstract_score = int(abstract_score)
                else:
                    abstract_score = 1
                date = input("date :(press enter for skip this)")
                date_score = input("date weight:(press enter for skip this)")
                if not date:
                    date = 2010
                    date_score = 0
                if date_score:
                    date_score = int(date_score)
                else:
                    date_score = 1
                page_rank_score = input("page rank weight:(press 0(or enter) or 1 )")
                if page_rank_score:
                    page_rank_score = int(page_rank_score)
                else:
                    page_rank_score = 0
                size = input("size of results:(press enter for default value)")
                if not size:
                    size = 10
                else:
                    size = int(size)
                search(elastic_url, title, title_score, abstract, abstract_score, date,
                       date_score, page_rank_score, size)
            elif task == 5:
                elastic_url = input("enter elastic url:port or press enter for default value")
                n = input("enter number of results:(press enter for default)")
                if not n and not elastic_url:
                    HITS()
                elif not n and elastic_url:
                    HITS(elastic_url)
                elif n and not elastic_url:
                    HITS(n=int(n))
                else:
                    HITS(elastic_url, int(n))
    except KeyboardInterrupt:
        print('interrupted!')