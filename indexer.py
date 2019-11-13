import json
import operator
from pprint import pprint
from typing import List
import math

import numpy as np
from elasticsearch import Elasticsearch
from elasticsearch import helpers

class ElasticDao:
    name = "paper_index"

    def __init__(self, host="localhost:9200"):
        self.es = Elasticsearch(hosts=[host])
        print("initialize elastic search")
        if not self.es.indices.exists(index=self.name):
            self.es.indices.create(index=self.name)

    def delete(self):
        res = self.es.indices.delete(self.name)
        print("index deleted")
        return res

    def insert(self, scholar:dict):
        print("insert scholar whit id :"+scholar['id'])
        return self.es.index(index=self.name, doc_type='paper', body={"paper":scholar}, id=scholar['id'])


    def search(self, title="", title_score=0, abstract="", abstract_score=0, date=None, date_score=0, page_rank_score=1,size=10):

        query = {
                  "query": {
                     "bool": {
                       "should": [
                        {"match": {
                            "paper.title":  {
                              "query": title,
                              "boost": title_score
                        }}},
                        {"match": {
                            "paper.abstract":  {
                              "query": abstract,
                              "boost": abstract_score
                        }}},
                        {"range": {
                              "paper.date": {
                                  "gte": date,
                                  "boost": date_score
                            }}},
                     ]
                   }
                  }
                }
        if page_rank_score:
            query['sort'] = [{"paper.page_rank": {"order": "desc"}},
                             "_score",

                             ]

        res = self.es.search(index=self.name, body=query,size=size)
        scholars = []
        for hit in res['hits']['hits']:
            scholars.append({"paper":hit["_source"]['paper'],"score":hit['_score']})
        return scholars


    def get_scholar(self,id):
        return self.es.search(index=self.name, body={"query": {"match": {'_id': id}}})['hits']['hits'][0]['_source']['paper']

    def get_scholars_by_ids(self, ids):
        query = {"ids": ids}
        return [sc['_source']['paper'] for sc in self.es.mget(index=self.name, body=query,doc_type='paper')['docs']]

    def get_all(self):
        scholars = []
        res0 = self.es.search(index=self.name, body={"query": {"match_all": {}}})
        res = self.es.search(index=self.name, size=int(res0['hits']['total']), body={"query": {"match_all": {}}})
        for hit in res['hits']['hits']:
            scholars.append(hit["_source"]['paper'])
        return scholars

    def insert_scholars(self, path="/windows/emad/daneshga/MIR/2018/p3/crawling.json"):
        bulk_inserts = []
        with open(path, 'r', encoding="utf-8") as f:
            scholars = json.load(f)
        origin_scholars = [sc for sc in scholars if sc['type'] == 'paper']
        extra_scholars = [sc for sc in scholars if sc['type'] == 'extra-references']
        for scholar in extra_scholars:
                old_scholar = [sc for sc in origin_scholars if sc['id'] == scholar['id']][0]
                old_scholar['references'].extend(scholar['references'])
        for scholar in origin_scholars:
            #self.insert(scholar)
            del scholar['type']
            action = {
                    "_index": self.name,
                    "_type": "paper",
                    "_id": scholar['id'],
                    "_source": {
                        "paper": scholar
                     },
                    }
            bulk_inserts.append(action)
        helpers.bulk(self.es, bulk_inserts)
        print("bulk insert scholars")



    def update(self, id, body):
        print("update scholar  whit id :" + id)
        return self.es.update(index=self.name, doc_type='paper', body=body, id=id, refresh='wait_for')

    def add_page_rank(self, id, page_rank):
        query = {'doc': {"paper": {'page_rank': page_rank}}}
        return self.update(id, query)

    def get_id(self, url):
        return url[url.index('paper/')+len("paper/"):]

    def set_page_ranks(self, d=0.85, eps=1.0e-8):
        scholars = self.get_all()
        scholar_ids = [sc['id'] for sc in scholars]
        N = len(scholars)
        M = np.zeros((N, N))
        for index, scholar in enumerate(scholars):
            refs = [ref for ref in scholar['references']]
            new_refs = [ref for ref in refs if ref in scholar_ids]
            if not new_refs:
                for ref_index in range(N):
                    M[ref_index][index] = 1 / N
            for ref in new_refs:
                M[scholar_ids.index(ref)][index] = 1 / len(new_refs)

        v1 = v = np.random.rand(N, 1)
        v = v / np.linalg.norm(v, 1)
        M_hat = (d * M) + (((1 - d) / N) * np.ones((N, N), dtype=np.float32))
        while True:
            v1 = np.dot(M_hat, v)
            dist = np.linalg.norm(v - v1, 2)
            if dist < eps:
                break
            else:
                v = v1

        bulk_update = []
        for index, id in enumerate(scholar_ids):
            #self.add_page_rank(id, v[index][0])
            action = {
                '_op_type': 'update',
                '_index': self.name,
                '_type': 'paper',
                '_id': id,
                'doc': {"paper": {'page_rank': v[index][0]}}
            }
            bulk_update.append(action)
        helpers.bulk(self.es, bulk_update)
        print("bulk update page ranks")

    def HITS_authors(self, n=10):
        authors = []
        scholars = self.get_all()
        scholar_ids = [sc['id'] for sc in scholars]
        for sc in scholars:
            refs_id = [ref for ref in sc['references'] if ref in scholar_ids]
            if not refs_id:
                continue
            refs = self.get_scholars_by_ids(refs_id)
            for sc_author in sc['authors']:
                author = self.find_author(sc_author, authors)
                for ref in refs:
                    for ref_author in ref['authors']:
                        out_ref_author = self.find_author(ref_author, authors)
                        author.out_links.add(out_ref_author)
                        out_ref_author.in_links.add(author)


        steps = 5
        for step in range(steps):
            norm = 0
            for author in authors:
                author.auth = 0
                for in_author in author.in_links:
                    author.auth += in_author.hub
                norm += math.pow(author.auth, 2)
            norm = math.sqrt(norm)
            for author in authors:
                author.auth /= norm
            norm = 0
            for author in authors:
                author.hub = 0
                for out_author in author.out_links:
                    author.hub += out_author.auth
                    norm += math.pow(author.hub, 2)
                norm = math.sqrt(norm)
            for author in authors:
                author.hub /= norm

        sorted_authors = sorted(authors, key=lambda x: x.auth, reverse=True)
        return sorted_authors[0:n]

    def find_author(self, name, authors):
        for author in authors:
            if name == author.name:
                return author
        author = Author(name)
        authors.append(author)
        return author



class Author:

    def __init__(self, name):
        self.name = name
        self.out_links = set()
        self.in_links = set()
        self.auth = 1
        self.hub = 1

    def __str__(self):
        return self.name + " : " + str(self.auth)

    def __repr__(self):
        return self.name + " : " + str(self.auth)

if __name__ == '__main__':
    print(len(ElasticDao().get_all()))







