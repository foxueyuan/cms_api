# -*- coding: utf-8 -*-

import os

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, scan
from datetime import datetime
import time
from sync_es import SyncES

cur_dir = os.getcwd()

sync_es = SyncES()


def _format_data(db,remote_sync_data, row_obj):
    data_quality = True
    topic = row_obj.get('topic', None)
    if db == 'qa':
        if topic:
            if isinstance(topic, str):
                topic = [topic]
        else:
            data_quality = False
    question = row_obj.get('question', None)
    if question:
        if isinstance(question, str):
            question = [question]
    else:
        data_quality = False
    title = row_obj.get('title', None)
    if not title:
        data_quality = False
    answer = row_obj.get('answer', None)
    if not answer:
        data_quality = False
    if data_quality:
        sync_data = {
            'topic': topic,
            'question': question,
            'title': title,
            'answer': answer
        }
        remote_sync_data.append(sync_data)


def _remote_delete_sync(intent,title):
    return_msg = {
        "deleted": False,
        "msg": "failed to delete"
    }
    status, result = sync_es.get_by_title(intent, title)
    if status and status == 200:
        if result['result']:
            delete_status = True
            if result['result']:
                status,reuslt = sync_es.delete(intent,result['result']['_id'])
                if status and status != 200:
                    delete_status = False
            if delete_status:
                return_msg['deleted'] = True
                return_msg['msg'] = "success"
                return return_msg
            else:
                return_msg['deleted'] = False
                return_msg['msg'] = "failed to delete"
                return return_msg
        else:
            return_msg['deleted'] = True
            return_msg['msg'] = "no related data in train set"
            return return_msg
    else:
        return return_msg


def _remote_update_sync(intent, updates):
    updates = updates['doc']
    return_msg = {
        "updated": False,
        "msg": "failed to update"
    }
    status, result = sync_es.get_by_title(intent, updates['title'])
    if status and status == 200:
        if result['result']:
            if 'topic' in updates:
                topic = updates['topic']
                if isinstance(topic, str):
                    updates['topic'] = [topic]
            if 'question' in updates:
                question = updates['question']
                if isinstance(question, str):
                    updates['question'] = [question]
            status, result = sync_es.update_title(intent, updates['title'], updates)
            if status and status == 200:
                return_msg['updated'] = True
                return_msg['msg'] = "success"
                return return_msg
            else:
                return return_msg
        else:
            return_msg['updated'] = "new"
            return_msg['msg'] = "not exist in train set"
            return return_msg

    else:
        return return_msg


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


class QuestionElasticSearch(object):  # 在ES中加载、批量插入数据
    def __init__(self):
        self.index = 'fo-qa-index'
        self.doc_type = 'test-type'
        self.es_client = ElasticSearchClient.get_es_servers()
        self.set_mapping()
        self.remote_db = 'qa'

    def set_mapping(self):
        mapping = {
            self.doc_type: {
                    'topic': {
                        'type': 'string'
                    },
                    'title': {
                        'type': 'string'
                    },
                    'question': {
                        'type': 'string'
                    },
                    'answer': {
                        'type': 'string'
                    },
                    'updated': {
                        'type': 'date',
                        'format': 'yyyy-MM-dd HH:mm:ss'
                    },
                    'updatedAt': {
                        'type': 'date',
                        'format': 'epoch_second'
                    }
                }
            }

        if not self.es_client.indices.exists(index=self.index):
            self.es_client.indices.create(index=self.index, body=mapping, ignore=400)
            self.es_client.indices.put_mapping(index=self.index, doc_type=self.doc_type, body=mapping)

    def update_mapping(self):
        mapping = {
            self.doc_type:{
                "properties": {
                    'topic': {
                        'type': 'string'
                    },
                    'title': {
                        'type': 'string'
                    },
                    'question': {
                        'type': 'string'
                    },
                    'answer': {
                        'type': 'string'
                    },
                    'updatedAt': {
                        'type': 'date',
                        'format': 'epoch_second'
                    }
                }
            }
        }
        self.es_client.indices.put_mapping(index=self.index, doc_type=self.doc_type, body=mapping)
        print("success")

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
        source = return_data['_source']
        response = {
            "_id": return_data['_id'],
            "topic": source.get('topic',None),
            "title": source.get('title',None),
            "answer": source.get('answer',None),
            "question": source.get('question',None)
        }
        return response

    def add_data_bulk(self, row_obj_list):
        """
        批量插入ES
        """
        current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        now = int(time.time())
        load_data = []
        remote_sync_data = []
        i = 1
        bulk_num = 10000
        for row_obj in row_obj_list:
            action = {
                '_index': self.index,
                '_type': self.doc_type,
                '_source': {
                    'topic': row_obj.get('topic', None),
                    'title': row_obj.get('title', None),
                    'question': row_obj.get('question', None),
                    'answer': row_obj.get('answer', None),
                    'updated': row_obj.get('updated', current_time),
                    'updatedAt': row_obj.get('updatedAt', now),
                }
            }
            load_data.append(action)

            _format_data(self.remote_db,remote_sync_data, row_obj)
            i += 1
            # 批量处理
            if len(load_data) == bulk_num:
                print('插入', int(i / bulk_num), '批数据')
                success, failed = bulk(self.es_client, load_data, index=self.index, raise_on_error=True)
                status,result = sync_es.post(self.remote_db,remote_sync_data)
                del load_data[0:len(load_data)]
                print(success, failed)
                print(status)

        if len(load_data) > 0:
            success, failed = bulk(self.es_client, load_data, index=self.index, raise_on_error=True)
            status, result = sync_es.post(self.remote_db, remote_sync_data)
            del load_data[0:len(load_data)]
            print(success, failed)
            print(status)

    def update_question(self, _id=None, updates={}):
        return_msg = {
            "updated": False,
            "msg": "failed to update"
        }
        if 'title' in updates:
            return_msg = _remote_update_sync(self.remote_db,updates)
        else:
            return_result = self.query_data(_id)
            if 'title' in return_result:
                updates['title'] = return_result['title']
                return_msg = _remote_update_sync(self.remote_db,updates)
            else:
                return_msg['updated'] = False
                return_msg['msg'] = "don't have data qualified data, miss title info for id {}".format(_id)
                return return_msg

        if return_msg and return_msg['updated'] == 'new':
            return_result = self.query_data(_id)
            sync_data = []
            _format_data(self.remote_db,sync_data, return_result)
            status,result = sync_es.post(self.remote_db,sync_data)
            if status and status == 200:
                return_msg['updated'] = True
                return_msg['msg'] = "success"
            else:
                return_msg['updated'] = False
                return_msg['msg'] = "fail to insert new data to train set"

        if return_msg and return_msg['updated']:
            return_result = self.es_client.update(index=self.index, doc_type=self.doc_type, id=_id, body=updates, refresh=True,
                                      retry_on_conflict=2)
            if return_result['result'] == 'updated':
                return_msg['updated'] = True
                return_msg['msg'] = "success"
                return return_msg
            else:
                return_msg['updated'] = False
                return_msg['msg'] = "failed to update"
                return return_msg
        else:
            return return_msg

    def delete_question(self, _id):
        return_msg = {
            "deleted": False,
            "msg": "failed to delete"
        }
        return_result = self.query_data(_id)
        if 'title' in return_result:
            title = return_result['title']
            return_msg = _remote_delete_sync(self.remote_db,title)
            print(return_msg)
        else:
            return_msg['deleted'] = False
            return_msg['msg'] = "don't have data in training set, please add data firstly"
            return return_msg
        if return_msg and return_msg['deleted']:
            status = self.es_client.delete(index=self.index, doc_type=self.doc_type, id=_id)
            if status:
                return return_msg
            else:
                return_msg['deleted'] = False
                return_msg['msg'] = "failed to delete"
                return return_msg
        else:
            return return_msg

    def get_questions_by_page(self, inputs=[], scroll_id=None):
        if not scroll_id:
            if inputs:
                my_query = query_generation(inputs)
            else:
                my_query = {"query": {"match_all": {}}}
            return_result = self.es_client.search(
                doc_type=self.doc_type, index=self.index,
                body=my_query,
                scroll=u'10m',
                size=20
            )
            _scroll_id = return_result['_scroll_id']
        if scroll_id:
            return_result = self.es_client.scroll(scroll_id=scroll_id,
                                      body={"scroll": u'10m'})
            _scroll_id = scroll_id
            if not return_result['hits']['hits']:
                self.es_client.clear_scroll(scroll_id=scroll_id)
                _scroll_id = ""

        total = return_result['hits']['total']
        questions = return_result['hits']['hits']
        response = {}
        question_list = []
        response['total'] = total
        response['scroll_id'] = _scroll_id
        response['questions'] = question_list
        for question in questions:
            source = question['_source']
            try:
                question_item = {
                    "_id": question['_id'],
                    "topic": source['topic'],
                    "title": source['title'],
                    "question": source['question'],
                    "answer": source['answer']
                }
                question_list.append(question_item)
            except:
                pass
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
                    "topic": source.get('topic',None),
                    "title": source.get('title',None),
                    "question": source.get('question',None),
                    "answer": source.get('answer',None),
                }
                question_list.append(question_item)
                total += 1
            except:
                pass
        response['total'] = total
        return response

    def _get_updated_records(self):
        """
        查询所有数据
        """
        my_query = {
                "query": {
                    "exists" : { "field" : "updated" }
                }
            }
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
                    "topic": source['topic'],
                    "title": source['title'],
                    "question": source['question'],
                    "answer": source['answer'],
                    "updated": source['updated']
                }
                question_list.append(question_item)
                total += 1
            except:
                pass
        response['total'] = total
        return response


class TopicElasticSearch(object):  # 在ES中加载、批量插入数据
    def __init__(self):
        self.index = 'fo-topic-index'
        self.doc_type = 'test-type'
        self.es_client = ElasticSearchClient.get_es_servers()
        self.set_mapping()

    def set_mapping(self):
        mapping = {
            self.doc_type: {
                    'topic': {
                        'type': 'string'
                    },
                    'updated': {
                        'type': 'date',
                        'format': 'yyyy-MM-dd HH:mm:ss'
                    }
                }
            }

        if not self.es_client.indices.exists(index=self.index):
            self.es_client.indices.create(index=self.index, body=mapping, ignore=400)
            self.es_client.indices.put_mapping(index=self.index, doc_type=self.doc_type, body=mapping)

    def update_mapping(self):
        mapping = {
            self.doc_type:{
                "properties": {
                    'topic': {
                        'type': 'string'
                    },
                    'updated': {
                        'type': 'date',
                        'format': 'yyyy-MM-dd HH:mm:ss'
                    }
                }
            }
        }
        self.es_client.indices.put_mapping(index=self.index, doc_type=self.doc_type, body=mapping)
        print("success")

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
            "topic": return_data['_source']['topic']
        }
        return response

    def add_data_bulk(self, row_obj_list):
        """
        批量插入ES
        """
        current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        load_data = []
        i = 1
        bulk_num = 10000
        for row_obj in row_obj_list:
            action = {
                '_index': self.index,
                '_type': self.doc_type,
                '_source': {
                    'topic': row_obj.get('topic', None),
                    'updated': row_obj.get('updated', current_time),
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

    def update_topic(self, _id=None, updates={}):
        return_result = self.es_client.update(index=self.index, doc_type=self.doc_type, id=_id, body=updates, refresh=True,
                                  retry_on_conflict=2)
        if return_result['result'] == 'updated':
            return True
        return False

    def delete_topic(self, _id):
        return self.es_client.delete(index=self.index, doc_type=self.doc_type, id=_id)

    def exist_topic(self, topics=[]):
        return_msg = {
            "exist": True,
            "msg": "I'm here!"
        }
        if topics:
            my_topics = {item['topic']:1 for item in self.get_topics()}
            for topic in topics:
                if topic not in my_topics:
                    return_msg['exist'] = False
                    return_msg['msg'] = "Topic {} is not exist in topics, please add!".format(topic)
                    return return_msg
            return return_msg
        else:
            return_msg['exist'] = False
            return_msg['msg'] = "Miss topic in question items!"
            return return_msg

    def get_topics(self):
        """
        查询所有数据
        """
        my_query = {"query": {"match_all": {}}}
        return_result = scan(self.es_client, query=my_query, index=self.index,
                             doc_type=self.doc_type)
        topic_list = []
        for question in return_result:
            source = question['_source']
            try:
                question_item = {
                    "_id": question['_id'],
                    "topic": source['topic'],
                }
                topic_list.append(question_item)
            except:
                pass
        return topic_list


class LawElasticSearch(object):  # 在ES中加载、批量插入数据
    def __init__(self):
        self.index = 'fo-law-index'
        self.doc_type = 'test-type'
        self.es_client = ElasticSearchClient.get_es_servers()
        self.set_mapping()
        self.remote_db = 'kg'

    def set_mapping(self):
        mapping = {
            self.doc_type: {
                    'title': {
                        'type': 'string'
                    },
                    'question': {
                        'type': 'string'
                    },
                    'answer': {
                        'type': 'string'
                    },
                    'updated': {
                        'type': 'date',
                        'format': 'yyyy-MM-dd HH:mm:ss'
                    },
                    'updatedAt': {
                        'type': 'date',
                        'format': 'epoch_second'
                    }
                }
            }

        if not self.es_client.indices.exists(index=self.index):
            self.es_client.indices.create(index=self.index, body=mapping, ignore=400)
            self.es_client.indices.put_mapping(index=self.index, doc_type=self.doc_type, body=mapping)

    def update_mapping(self):
        mapping = {
            self.doc_type:{
                "properties": {
                    'title': {
                        'type': 'string'
                    },
                    'question': {
                        'type': 'string'
                    },
                    'answer': {
                        'type': 'string'
                    },
                    'updatedAt': {
                        'type': 'date',
                        'format': 'epoch_second'
                    }
                }
            }
        }
        self.es_client.indices.put_mapping(index=self.index, doc_type=self.doc_type, body=mapping)
        print("success")

    def add_data(self, row_obj):
        """
        单条插入ES
        """
        _id = row_obj.get('_id', 1)
        row_obj.pop('_id')
        self.es_client.index(index=self.index, doc_type=self.doc_type, body=row_obj, id=_id)

    def query_data(self, _id):
        """
        查询数据
        :return:
        """
        return_data = self.es_client.get(index=self.index, doc_type=self.doc_type, id=_id)
        source = return_data['_source']
        questions = source.get("question", None)
        if questions:
            if not isinstance(questions, list):
                if "####||" in questions:
                    source['question'] = questions.split("####||")
                else:
                    source['question'] = [questions]

        response = {
            "_id": return_data['_id'],
            "title": source['title'],
            "answer": source['answer'],
            "question": source['question']
        }
        return response

    def add_data_bulk(self, row_obj_list):
        """
        批量插入ES
        """
        database="kg"
        load_data = []
        remote_sync_data = []
        current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        now = int(time.time())
        i = 1
        bulk_num = 10000
        for row_obj in row_obj_list:
            action = {
                '_index': self.index,
                '_type': self.doc_type,
                '_source': {
                    'title': row_obj.get('title', ""),
                    'question': row_obj.get('question', None),
                    'answer': row_obj.get('answer', None),
                    'updated': row_obj.get('updated', current_time),
                    'updatedAt': row_obj.get('updatedAt', now),
                }
            }
            load_data.append(action)
            _format_data(database,remote_sync_data, row_obj)
            i += 1
            # 批量处理
            if len(load_data) == bulk_num:
                print('插入', int(i / bulk_num), '批数据')
                success, failed = bulk(self.es_client, load_data, index=self.index, raise_on_error=True)
                status,result = sync_es.post(database,remote_sync_data)
                del load_data[0:len(load_data)]
                print(success, failed)
                print(status)

        if len(load_data) > 0:
            success, failed = bulk(self.es_client, load_data, index=self.index, raise_on_error=True)
            status, result = sync_es.post(database, remote_sync_data)
            del load_data[0:len(load_data)]
            print(success, failed)
            print(status)

    def delete_question(self, _id):
        return_msg = {
            "deleted": False,
            "msg": "failed to delete"
        }
        return_result = self.query_data(_id)
        if 'title' in return_result:
            title = return_result['title']
            return_msg = _remote_delete_sync(self.remote_db, title)
            print(return_msg)
        else:
            return_msg['deleted'] = False
            return_msg['msg'] = "don't have data in training set, please add data firstly"
            return return_msg
        if return_msg and return_msg['deleted']:
            status = self.es_client.delete(index=self.index, doc_type=self.doc_type, id=_id)
            if status:
                return return_msg
            else:
                return_msg['deleted'] = False
                return_msg['msg'] = "failed to delete"
                return return_msg
        else:
            return return_msg

    def update_question(self, _id=None, updates={}):
        return_msg = {
            "updated": False,
            "msg": "failed to update"
        }
        if 'title' in updates:
            return_msg = _remote_update_sync(self.remote_db, updates)
        else:
            return_result = self.query_data(_id)
            if 'title' in return_result:
                updates['title'] = return_result['title']
                return_msg = _remote_update_sync(self.remote_db, updates)
            else:
                return_msg['updated'] = False
                return_msg['msg'] = "don't have data qualified data, miss title info for id {}".format(_id)
                return return_msg

        if return_msg and return_msg['updated'] == 'new':
            return_result = self.query_data(_id)
            sync_data = []
            _format_data(self.remote_db, sync_data, return_result)
            status, result = sync_es.post(self.remote_db, sync_data)
            if status and status == 200:
                return_msg['updated'] = True
                return_msg['msg'] = "success"
            else:
                return_msg['updated'] = False
                return_msg['msg'] = "fail to insert new data to train set"

        if return_msg and return_msg['updated']:
            return_result = self.es_client.update(index=self.index, doc_type=self.doc_type, id=_id, body=updates,
                                                  refresh=True,
                                                  retry_on_conflict=2)
            if return_result['result'] == 'updated':
                return_msg['updated'] = True
                return_msg['msg'] = "success"
                return return_msg
            else:
                return_msg['updated'] = False
                return_msg['msg'] = "failed to update"
                return return_msg
        else:
            return return_msg

    def get_laws_by_page(self, inputs=[], scroll_id=None):
        if not scroll_id:
            if inputs:
                my_query = query_generation(inputs)
            else:
                my_query = {"query": {"match_all": {}}}
            return_result = self.es_client.search(
                doc_type=self.doc_type, index=self.index,
                body=my_query,
                scroll=u'5m',
                size=20
            )
            _scroll_id = return_result['_scroll_id']
        if scroll_id:
            return_result = self.es_client.scroll(scroll_id=scroll_id,
                                      body={"scroll": u'5m'})
            _scroll_id = scroll_id
            if not return_result['hits']['hits']:
                self.es_client.clear_scroll(scroll_id=scroll_id)
                _scroll_id = ""

        total = return_result['hits']['total']
        questions = return_result['hits']['hits']
        response = {}
        question_list = []
        response['total'] = total
        response['scroll_id'] = _scroll_id
        response['questions'] = question_list
        for question in questions:
            source = question['_source']
            try:
                questions = source.get("question",None)
                if questions:
                    if not isinstance(questions,list):
                        if "####||" in questions:
                            source['question'] = questions.split("####||")
                        else:
                            source['question'] = [questions]
                question_item = {
                    "_id": question['_id'],
                    "title": source['title'],
                    "question": source['question'],
                    "answer": source['answer']
                }
                question_list.append(question_item)
            except:
                pass

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
                    "title": source.get('title',None),
                    "question": source.get('question',None),
                    "answer": source.get('answer',None)
                }
                question_list.append(question_item)
                total += 1
            except:
                pass
        response['total'] = total
        return response

    def _get_updated_records(self, inputs=[]):
        """
        查询所有数据
        """
        my_query = {
                "query": {
                    "exists": {"field": "updated"}
                }
            }
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
                    "question": source.get('question',None),
                    "answer": source.get('answer',None),
                    "title":source.get('title',None),
                    "updated": source.get('updated',None)
                }
                if 'title' not in source:
                    question_item['title'] = source['question'].split("####||")[0]
                else:
                    question_item['title'] = source['title']

                question_list.append(question_item)
                total += 1
            except:
                pass
        response['total'] = total
        return response


def update_exsit_records(index_type):
    if index_type and index_type == "qa":
        load_es = QuestionElasticSearch()
    elif index_type and index_type == "law":
        load_es = LawElasticSearch()
    result = load_es._get_updated_records()
    for question in result['questions']:
        updated = datetime.strptime(question['updated'], '%Y-%m-%d %H:%M:%S')
        load_es.update_question(question['_id'], {"doc": {"title":question["title"],"updatedAt": int(updated.timestamp())}})


if __name__ == '__main__':
    # from datetime import datetime
    import csv
    # es = ElasticSearchClient.get_es_servers()
    # es.index(index='fo-topic-index', doc_type='test-type', body={'any': 'data', 'timestamp': datetime.now()})
    load_es = LawElasticSearch()
    result = load_es._get_updated_records()
    for question in result['questions']:
        # updated = datetime.strptime(question['updated'], '%Y-%m-%d %H:%M:%S')
        load_es.update_question(question['_id'],{"doc": {"title":question["title"]}})
    # updated = datetime.strptime(result['updated'],'%Y-%m-%d %H:%M:%S')
    # print(int(updated.timestamp()))
    # xx = load_es.get_records()
    # print(len(xx['questions']))
    # # result = load_es.get_records([
    # #             { "match_phrase": { "question": "劳动法" }},{ "match_phrase": { "question": "婚姻法" }},{ "match_phrase": { "answer": "中国" }}
    # #         ])
    # # print(result['total'])
    # #[{"match_phrase": {"question": phrase}} for phrase in text_main_content]
    # qa_list = []
    # with open('/Users/shihujie/Desktop/law.txt', 'rt', encoding='utf-8') as txt:
    #     lines = txt.readlines()
    #     for record in lines:
    #         try:
    #             record_items = record.split('\t', 2)
    #             questions = record_items[0].strip()
    #             answer = record_items[1].strip()
    #             qa_list.append({'question': questions, 'answer': answer})
    #         except:
    #             pass
    # load_es.add_data_bulk(qa_list)
