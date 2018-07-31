# import asyncio
# from aioelasticsearch import Elasticsearch
# from aioelasticsearch.helpers import Scan
# import re
#
# from elasticsearch import Elasticsearch as ES
#
# IP = "106.14.107.172"
# es = ES(hosts=[IP])
# my_index = "fo-qa-index"
# my_doc_type = "test-type"
#
#
# def get_records_by_page(topic=None, scroll_id=None):
#     if not scroll_id:
#         my_body = {"query": {"match_all": {}}}
#         if topic:
#             my_body = {"query": {"match": {"topic": topic}}}
#         init_es = es.search(
#             index=my_index, doc_type=my_doc_type,
#             body=my_body,
#             scroll=u'30m',
#             size=20
#         )
#         scroll_id = init_es['_scroll_id']
#     return_result = es.scroll(scroll_id=scroll_id,
#                               body={"scroll": u'30m'},
#                               )
#     total = return_result['hits']['total']
#     questions = return_result['hits']['hits']
#     response = {}
#     question_list = []
#     response['total'] = total
#     response['scroll_id'] = scroll_id
#     response['questions'] = question_list
#     for question in questions:
#         source = question['_source']
#         try:
#             question_item = {
#                 "_id": question['_id'],
#                 "topic": re.sub(r'[\[\]]', '', source['topic']),
#                 "title": source['title'],
#                 "question": source['question'].replace('\n', ''),
#                 "answer": source['answer']
#             }
#             question_list.append(question_item)
#         except:
#             pass
#     return response
#
#
# def update_question(_id=None, updates={}):
#     return_result = es.update(index=my_index, doc_type=my_doc_type, id=_id, body=updates, refresh=True,
#                               retry_on_conflict=2)
#     if return_result['result'] == 'updated':
#         return True
#     return False
#
#
# def add_questions(questions=[]):
#     success, failed = add_data_bulk(questions)
#     if success:
#         return True
#     return False
#
#
# def add_data_bulk(row_obj_list):
#     """
#     批量插入ES
#     """
#     load_data = []
#     i = 1
#     bulk_num = 10000
#     for row_obj in row_obj_list:
#         action = {
#             '_index': my_index,
#             '_type': my_doc_type,
#             '_source': {
#                 'topic': row_obj.get('topic', None),
#                 'title': row_obj.get('title', None),
#                 'question': row_obj.get('question', None),
#                 'answer': row_obj.get('answer', None),
#             }
#         }
#         load_data.append(action)
#         i += 1
#         # 批量处理
#         if len(load_data) == bulk_num:
#             print('插入', int(i / bulk_num), '批数据')
#             es.bulk(load_data, index=my_index, doc_type=my_doc_type, raise_on_error=True)
#             del load_data[0:len(load_data)]
#
#     if len(load_data) > 0:
#         success, failed = es.bulk(load_data, index=my_index, doc_type=my_doc_type, raise_on_error=True)
#         del load_data[0:len(load_data)]
#         return success, failed
#
#
# def get_record_by_id(_id=None):
#     doc = es.get(index=my_index, doc_type=my_doc_type, id=_id)
#     source = doc['_source']
#     question_item = {
#         "_id": doc['_id'],
#         "topic": re.sub(r'[\[\]]', '', source['topic']),
#         "title": source['title'],
#         "question": source['question'].replace('\n', ''),
#         "answer": source['answer']
#     }
#     return question_item
#
#
# def get_fresh_tag(source):
#     if source.get('updated_tag'):
#         return source['updated_tag']
#     else:
#         return re.sub(r'[\[\]]', '', source['tag'])
#
#
# async def get_tag_set():
#     async with Elasticsearch(hosts=IP) as es:
#         my_set = set()
#         async with Scan(
#                 es,
#                 index='fo-qa-index',
#                 doc_type='test-type',
#                 query={},
#         ) as scan:
#             async for doc in scan:
#                 source = doc['_source']
#                 try:
#                     my_set.add(re.sub(r'[\[\]]', '', source['topic']))
#                 except:
#                     pass
#
#         return my_set
#
#
# def get_tags():
#     loop = asyncio.get_event_loop()
#     my_set = loop.run_until_complete(get_tag_set())
#     return list(my_set)
#
#
# # async def update_domains(tag=None):
# #     async with Elasticsearch(hosts=IP) as es:
# #         my_query = {}
# #         if tag:
# #             my_query = {"query": {
# #                 "bool": {
# #                     "must": [
# #                         {"match": {"tag": tag}}
# #                     ]
# #                 }
# #             }}
# #         async with Scan(
# #                 es,
# #                 index='lawquestion',
# #                 doc_type='lawquestion',
# #                 query=my_query,
# #                 scroll=u'30m'
# #         ) as scan:
# #             async for doc in scan:
# #                 source = doc['_source']
# #                 id = doc['_id']
# #                 title = source['title'].replace(u'\n', '').replace(u'\xa0', ' ').replace(u'\u2002', ' ')
# #                 question = source['question'].replace(u'\n', '').replace(u'\xa0', ' ').replace(u'\u2002', ' ')
# #                 answers = source['answers'].replace(u'\n', '').replace(u'\xa0', ' ').replace(u'\u2002', ' ')
# #                 try:
# #                     domains = get_tags_from_baidu(title, question, answers)
# #                 except:
# #                     pass
# #                 changes = {
# #                     "doc": {
# #                         "domains": domains
# #                     }
# #                 }
# #                 update_question(id, changes)
# #
# #
# # def get_result(query=None):
# #     if query:
# #         my_body = {
# #           "query": {
# #             "multi_match": {
# #               "query": query,
# #               "fields": [
# #                 "domains",
# #                 "tag",
# #                 "title",
# #                 "question",
# #                 "answers",
# #                 "tag.keyword",
# #                 "updated_tag",
# #                 "final_answer"
# #               ],
# #               "type": "most_fields"
# #             }
# #           }
# #         }
# #         result = es.search(
# #             body=my_body,
# #             doc_type=my_doc_type,
# #             index=my_index
# #         )
# #         hits = result['hits']['hits']
# #         response = []
# #         base_score = 0
# #         for item in hits:
# #             score = item['_score']
# #             if base_score == 0:
# #                 base_score = score
# #             data = {}
# #             confidence = _calculate_score(base_score=base_score,original_score=score)
# #             data['content'] = item['_source']['answers'].replace(u'\xa0', ' ')
# #             data['confidence'] = confidence
# #             response.append(data)
# #         return response
# #     return []
# #
# #
# # def _calculate_score(base_score, original_score):
# #     return round(original_score/(base_score*1.1), 4)
#
#
#
#
# if __name__ == '__main__':
#     # changes = {
#     #     "doc" : {
#     #         "final_answer": "根据合同约定处理，发生争议不能协商解决，可以申请劳动仲裁。",
#     #         "reviewed": True,
#     #         "updated_tag": "劳动纠纷"
#     #     }
#     # }
#     # changes = {'doc': {'final_answer': '根据合同约定处理，发生争议不能协商解决，可以申请劳动仲裁。', 'reviewed': True, 'updated_tag': '劳动纠纷'}}
#     # print(update_question('AWIyuqeG7YT7ZlJSzFmw', changes))
#     # print(get_tags())
#     # print(get_all_questions(reviewed=True))
#     # print(len(get_all_questions(tag="刑事辩护")))
#     sid = get_records_by_page(topic="刑事辩护")
#     for item in sid['questions']:
#         print(item)
#
#     print(sid['scroll_id'])
#     mm = get_records_by_page(topic="刑事辩护",scroll_id=sid['scroll_id'])
#     for item in mm['questions']:
#         print(item)
#     # import io
#     # import sys
#     # sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf8')
#     # loop = asyncio.get_event_loop()
#     # loop.run_until_complete(update_domains())
#     # xxs = get_result("限制出境问题")
#     # for xx in xxs:
#     #     print(xx['confidence'])
#
#
