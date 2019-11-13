import scrapy
import logging
from scrapy.utils.markup import remove_tags


class ScholarSpider(scrapy.Spider):
    name = "scholar"
    prefix_url = 'https://www.semanticscholar.org'
    start_urls = [
        'https://www.semanticscholar.org/paper/Coordinated-actor-model-of-self-adaptive-traffic-Bagheri-Sirjani/45ee43eb193409c96107c5aa76e8668a62312ee8',
        'https://www.semanticscholar.org/paper/Automatic-Access-Control-Based-on-Face-and-Hand-in-Jahromi-Bonderup/2199cb39adbf22b2161cd4f65662e4a152885bae',
        'https://www.semanticscholar.org/paper/Fair-Allocation-of-Indivisible-Goods%3A-Improvements-Ghodsi-Hajiaghayi/03d557598397d14727803987982c749fbfe1704b',
        'https://www.semanticscholar.org/paper/Restoring-highly-corrupted-images-by-impulse-noise-Taherkhani-Jamzad/637cf5540c0fb1492d94292bf965b2c404e42fb4',
        'https://www.semanticscholar.org/paper/Domino-Temporal-Data-Prefetcher-Bakhshalipour-Lotfi-Kamran/665c0dde22c2f8598869d690d59c9b6d84b07c01',
        'https://www.semanticscholar.org/paper/Deep-Private-Feature-Extraction-Ossia-Taheri/3355aff37b5e4ba40fc689119fb48d403be288be',

    ]

    def __init__(self, count=200,urls=None, *args, **kwargs):
        super(ScholarSpider, self).__init__(*args, **kwargs)
        self.count = int(count)
        if urls:
            self.start_urls = urls

    def parse(self, response):
        if self.count > 0:
            self.count -= 1
            paper_id = response.url[response.url.index('paper/')+len("paper/"):]
            paper_title = remove_tags(response.css("title").extract_first())
            paper_desc = remove_tags(response.css("meta[name='description']::attr(content)").extract_first())
            paper_authors = [remove_tags(auth) for auth in response.css("meta[name='citation_author']::attr(content)").extract()]
            paper_publish_year = remove_tags(response.css("meta[name='citation_publication_date']::attr(content)").extract_first())
            paper_refs = [ref[ref.index('paper/')+len("paper/"):] for ref in response.css('#references .citation .result-meta > a::attr(href)').extract() if ref.startswith("/paper/")]
            ref_len = response.css("#references > div.card-content > div > div.citation-pagination.flex-row-vcenter > ul > li > a").extract()
            output = {}
            extra_output = {}
            output['type'] = "paper"
            output['id'] = paper_id
            output['abstract'] = paper_desc
            output['title'] = paper_title
            output['authors'] = paper_authors
            output['date'] = paper_publish_year
            output['references'] = paper_refs

            if len(ref_len) > 3:
                print("extra")
                yield scrapy.Request(response.url+"?citedPapersOffset=10", self.parse2)




            print(self.count)
            print(output)
            yield output

            print("-----------------------------\n\n")

            if len(paper_refs) > 5:
                new_papers = paper_refs[0:5]
            else:
                new_papers = paper_refs
            for url in new_papers:
                yield scrapy.Request(self.prefix_url+"/paper/"+url, self.parse)

    def parse2(self, response):
        extra_output = {}
        extra_output['type'] = "extra-references"
        extra_output['id'] = response.url[response.url.index('paper/')+len("paper/"):response.url.index("?")]
        extra_output['references'] = [ref[ref.index('paper/')+len("paper/"):] for ref in response.css('#references .citation .result-meta > a::attr(href)').extract() if ref.startswith("/paper/")]
        yield extra_output