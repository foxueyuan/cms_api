# -*- coding: utf-8 -*-

import os

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, scan


cur_dir = os.getcwd()


def query_generation(inputs):
    """
    [{"match_phrase": {"question": phrase}} for phrase in text_main_content]
    :param inputs:
    :return:
    """
    _query = {
        'query': {
            'bool': {
                'should': inputs
            }
        }
    }

    '''# multi-match多级查询例子
    {
        "multi_match": {
            "query": {
                "bool": {
                    "should": inputs
                }
            }
        }
    }
    '''

    '''# bool布尔查询例子
    {
        "bool": {
            "must":     { "match": { "title": "how to make millions" }},
            "must_not": { "match": { "tag":   "spam" }},
            "should": [
                { "match": { "tag": "starred" }},
                { "range": { "date": { "gte": "2014-01-01" }}}
            ]
        }
    }
    '''

    return _query


class ElasticSearchClient(object):
    @staticmethod
    def get_es_servers():
        es_servers = [{
            "host": "106.14.107.172",
            "port": "9200"
        }]
        es_client = Elasticsearch(hosts=es_servers)
        return es_client


class LoadElasticSearch(object):  # 在ES中加载、批量插入数据
    def __init__(self):
        self.index = 'fo-law-index'
        self.doc_type = 'test-type'
        self.es_client = ElasticSearchClient.get_es_servers()
        self.set_mapping()

    def set_mapping(self):
        mapping = {
            self.doc_type: {
                    'question': {
                        'type': 'string'
                    },
                    'answer': {
                        'type': 'string'
                    }
                }
            }

        if not self.es_client.indices.exists(index=self.index):
            self.es_client.indices.create(index=self.index, body=mapping, ignore=400)
            self.es_client.indices.put_mapping(index=self.index, doc_type=self.doc_type, body=mapping)

    def add_data(self, row_obj):
        """
        单条插入ES
        """
        _id = row_obj.get('_id', 1)
        row_obj.pop('_id')
        self.es_client.index(index=self.index, doc_type=self.doc_type, body=row_obj, id=_id)

    def query_data(self,_id):
        """
        查询数据
        :return:
        """
        return_data = self.es_client.get(index=self.index, doc_type=self.doc_type, id=_id)
        response = {
            "_id": return_data['_id'],
            "answer": return_data['_source']['answer'],
            "question": return_data['_source']['question']
        }
        return response

    def add_data_bulk(self, row_obj_list):
        """
        批量插入ES
        """
        load_data = []
        i = 1
        bulk_num = 10000
        for row_obj in row_obj_list:
            action = {
                '_index': self.index,
                '_type': self.doc_type,
                '_source': {
                    'question': row_obj.get('question', None),
                    'answer': row_obj.get('answer', None),
                }
            }
            load_data.append(action)
            i += 1
            # 批量处理
            if len(load_data) == bulk_num:
                print('插入', int(i / bulk_num), '批数据')
                success, failed = bulk(self.es_client, load_data, index=self.index, raise_on_error=True)
                del load_data[0:len(load_data)]
                print(success, failed)

        if len(load_data) > 0:
            success, failed = bulk(self.es_client, load_data, index=self.index, raise_on_error=True)
            del load_data[0:len(load_data)]
            print(success, failed)

    def update_question(self, _id=None, updates={}):
        return_result = self.es_client.update(index=self.index, doc_type=self.doc_type, id=_id, body=updates, refresh=True,
                                  retry_on_conflict=2)
        if return_result['result'] == 'updated':
            return True
        return False

    def get_laws_by_page(self, inputs=[], scroll_id=None):
        if not scroll_id:
            if inputs:
                my_query = query_generation(inputs)
            else:
                my_query = {"query": {"match_all": {}}}
            init_es = self.es_client.search(
                doc_type=self.doc_type, index=self.index,
                body=my_query,
                scroll=u'10m',
                size=50
            )
            scroll_id = init_es['_scroll_id']
        return_result = self.es_client.scroll(scroll_id=scroll_id,
                                  body={"scroll": u'10m'},
                                  )
        total = return_result['hits']['total']
        questions = return_result['hits']['hits']
        response = {}
        question_list = []
        response['total'] = total
        response['scroll_id'] = scroll_id
        response['questions'] = question_list
        for question in questions:
            source = question['_source']
            question_item = {
                "_id": question['_id'],
                "question": source['question'],
                "answer": source['answer']
            }
            question_list.append(question_item)
        return response

    def get_records(self, inputs=[]):
        """
        查询所有数据
        """
        if inputs:
            my_query = query_generation(inputs)
        else:
            my_query = {"query": {"match_all": {}}}
        return_result = scan(self.es_client, query=my_query, index=self.index,
                             doc_type=self.doc_type)
        response = {}
        question_list = []
        total = 0
        response['questions'] = question_list
        for question in return_result:
            source = question['_source']
            try:
                question_item = {
                    "_id": question['_id'],
                    "question": source['question'],
                    "answer": source['answer']
                }
                question_list.append(question_item)
                total += 1
            except:
                pass
        response['total'] = total
        return response


if __name__ == '__main__':
    from datetime import datetime
    import csv
    # es = ElasticSearchClient.get_es_servers()
    # es.index(index='fo-law-index', doc_type='test-type', body={'any': 'data', 'timestamp': datetime.now()})
    load_es = LoadElasticSearch()
    result = load_es.get_records([
                { "match_phrase": { "question": "劳动法" }},{ "match_phrase": { "question": "婚姻法" }},{ "match_phrase": { "answer": "中国" }}
            ])
    print(result['total'])
    #[{"match_phrase": {"question": phrase}} for phrase in text_main_content]
    # qa_list = []
    # with open('/Users/shihujie/Desktop/law.txt', 'rt', encoding='utf-8') as txt:
    #     lines = txt.readlines()
    #     for record in lines:
    #         try:
    #             record_items = record.split('\t', 2)
    #             questions = record_items[0].split('####||')
    #             answer = record_items[1].strip()
    #             for question in questions:
    #                 qa_list.append({'question': question, 'answer': answer})
    #         except:
    #             pass
    # load_es.add_data_bulk(qa_list)
