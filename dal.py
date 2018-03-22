import asyncio
from aioelasticsearch import Elasticsearch
from aioelasticsearch.helpers import Scan
import re

from elasticsearch import Elasticsearch as ES

es = ES(hosts=["106.14.107.172"])
my_index = "lawquestion"
my_doc_type = "lawquestion"


def get_records_by_page(tag=None, reviewed=None, scroll_id=None):
    if not scroll_id:
        my_body = {"query": {"match_all": {}}}
        if tag:
            my_body = {"query": {"match": {"tag": tag}}}
        if reviewed and reviewed == 'true':
            if tag:
                my_body = {
                    "query": {
                        "bool": {
                            "must": [
                                {"match": {"reviewed": True}}
                            ],
                            "should": [
                                {"match": {"updated_tag": tag}},
                                {"match": {"tag": tag}},
                            ]
                        }
                    }
                }
            else:
                my_body = {
                    "query": {
                        "bool": {
                            "must": [
                                {"match": {"reviewed": True}}
                            ]
                        }
                    }
                }
        init_es = es.search(
            body=my_body,
            scroll=u'30m',
            size=20
        )
        scroll_id = init_es['_scroll_id']
    return_result = es.scroll(scroll_id=scroll_id,
                              body={"scroll": u'30m'},
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
        if reviewed and reviewed == 'true':
            if source.get('reviewed') and source['reviewed']:
                fresh_tag = get_fresh_tag(source)
                reviewed_item = {
                    "_id": question['_id'],
                    "title": source['title'],
                    "question": source['question'].replace('\n', ''),
                    "answers": source['final_answer'],
                    "tag": fresh_tag
                }
                question_list.append(reviewed_item)
        else:
            question_item = {
                "_id": question['_id'],
                "title": source['title'],
                "question": source['question'].replace('\n', ''),
                "answers": source['answers'].split("|"),
                "tag": re.sub(r'[\[\]]', '', source['tag'])
            }
            if source.get('reviewed'):
                pass
            else:
                question_list.append(question_item)
    return response


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


async def get_tag_set():
    async with Elasticsearch(hosts="106.14.107.172") as es:
        my_set = set()
        async with Scan(
                es,
                index='lawquestion',
                doc_type='lawquestion',
                query={},
        ) as scan:
            async for doc in scan:
                source = doc['_source']
                if source['tag'] == 'tag':
                    pass
                else:
                    my_set.add(re.sub(r'[\[\]]', '', source['tag']))
        return my_set


def get_tags():
    loop = asyncio.get_event_loop()
    my_set = loop.run_until_complete(get_tag_set())
    return list(my_set)


if __name__ == '__main__':
    # changes = {
    #     "doc" : {
    #         "final_answer": "根据合同约定处理，发生争议不能协商解决，可以申请劳动仲裁。",
    #         "reviewed": True,
    #         "updated_tag": "劳动纠纷"
    #     }
    # }
    # changes = {'doc': {'final_answer': '根据合同约定处理，发生争议不能协商解决，可以申请劳动仲裁。', 'reviewed': True, 'updated_tag': '劳动纠纷'}}
    # print(update_question('AWIyuqeG7YT7ZlJSzFmw', changes))
    # print(get_tags())
    # print(get_all_questions(reviewed=True))
    # print(len(get_all_questions(tag="刑事辩护")))
    # sid = get_records_by_page(tag="刑事辩护")
    # for item in sid['questions']:
    #     print(item)
    #
    # mm = get_records_by_page(tag="刑事辩护",scroll_id=sid['scroll_id'])
    # for item in mm['questions']:
    #     print(item)
    print(get_tags())
