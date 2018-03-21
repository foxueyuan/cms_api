import asyncio
from aioelasticsearch import Elasticsearch
from aioelasticsearch.helpers import Scan
import re

from elasticsearch import Elasticsearch as ES

es = ES(hosts=["106.14.107.172"])
my_index = "lawquestion"
my_doc_type = "lawquestion"


def update_question(_id=None, updates={}):
    return_result = es.update(index=my_index, doc_type=my_doc_type, id=_id, body=updates, refresh=True,
                              retry_on_conflict=2)
    if return_result['result'] == 'updated':
        return True
    return False


def get_record_by_id(_id=None):
    doc = es.get(index=my_index, doc_type=my_doc_type, id=_id)
    source = doc['_source']
    question_item = {
        "_id": doc['_id'],
        "title": source['title'],
        "question": source['question'].replace('\n', ''),
        "answers": source['answers'].split("|"),
        "tag": re.sub(r'[\[\]]', '', source['tag'])
    }
    print(question_item)


def get_fresh_tag(source):
    if source.get('updated_tag'):
        return source['updated_tag']
    else:
        return re.sub(r'[\[\]]', '', source['tag'])


async def get_questions(tag=None,reviewed=None):
    async with Elasticsearch(hosts="106.14.107.172") as es:
        my_query = {}
        if tag:
            my_query = {"query": {
                "bool": {
                  "must": [
                    { "match": { "tag":tag}}
                  ]
                }
              }}
        question_list = []
        async with Scan(
                es,
                index='lawquestion',
                doc_type='lawquestion',
                query=my_query,
        ) as scan:
            async for doc in scan:
                source = doc['_source']
                question_item = {
                    "_id": doc['_id'],
                    "title": source['title'],
                    "question": source['question'].replace('\n', ''),
                    "answers": source['answers'].split("|"),
                    "tag": re.sub(r'[\[\]]', '', source['tag'])
                }
                if reviewed:
                    if source.get('reviewed'):
                        fresh_tag = get_fresh_tag(source)
                        reviewed_item = {
                            "_id": doc['_id'],
                            "title": source['title'],
                            "question": source['question'].replace('\n', ''),
                            "answers": source['final_answer'],
                            "tag": fresh_tag
                        }
                        question_list.append(reviewed_item)
                else:
                    if source.get('reviewed'):
                        pass
                    else:
                        question_list.append(question_item)
        return question_list


def get_all_questions(tag=None,reviewed=None):
    loop = asyncio.get_event_loop()
    question_list = loop.run_until_complete(get_questions(tag,reviewed))
    return question_list


def get_tags():
    question_list = get_all_questions()
    my_set = set()
    for question in question_list:
        if question['tag'] == 'tag':
            pass
        else:
            my_set.add(question['tag'])
    return list(my_set)


if __name__ == '__main__':
    # changes = {
    #     "doc" : {
    #         "final_answer": "根据合同约定处理，发生争议不能协商解决，可以申请劳动仲裁。",
    #         "reviewed": True,
    #         "updated_tag": "劳动纠纷"
    #     }
    # }
    changes = {'doc': {'final_answer': '根据合同约定处理，发生争议不能协商解决，可以申请劳动仲裁。', 'reviewed': True, 'updated_tag': '劳动纠纷'}}
    print(update_question('AWIyuqeG7YT7ZlJSzFmw', changes))
    # print(get_tags())
    # print(get_all_questions(reviewed=True))
    # print(len(get_all_questions(tag="刑事辩护")))