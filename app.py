from flask import Flask, request, abort,jsonify
from dal import *
from qa_es import *

app = Flask(__name__)
law_es = LoadElasticSearch()


@app.route('/cms')
def index():
    return 'Welcome to FO CMS!'


@app.route('/cms/v1/laws', methods=['GET', 'POST'])
def laws():
    if request.method == 'POST':
        if not request.json:
            return jsonify({
                'errcode': -1,
                'errmsg': 'request body is not right, need to be json'
            })
        json_data = request.get_json()
        errcode = 0
        errmsg = "ok"
        es_response = {
            'errcode': errcode,
            'errmsg': errmsg
        }
        if json_data.get("questions"):
            question_items = json_data['questions']
            law_es.add_data_bulk(question_items)
            return jsonify(es_response)
        else:
            es_response['errcode'] = -1
            es_response['errmsg'] = "miss 'questions' in request body"
            return jsonify(es_response)
    elif request.method == 'GET':
        question = request.args.get('question', None)
        answer = request.args.get('answer', None)
        input_string = []
        if question:
            input_string.append({"match_phrase": {"question": question}})
        if answer:
            input_string.append({"match_phrase": {"answer": answer}})
        scroll_id = request.args.get('scroll_id', None)
        return jsonify(law_es.get_laws_by_page(inputs=input_string, scroll_id=scroll_id))


@app.route('/cms/v1/laws/<_id>', methods=['GET','POST'])
def laws_update(_id=None):
    if request.method == 'POST':
        if not request.json or _id is None:
            return jsonify({
                'errcode': -1,
                'errmsg': 'request body is not right, need to be json or miss _id'
            })
        errcode = 0
        errmsg = "ok"
        es_response = {
            'errcode': errcode,
            'errmsg': errmsg
        }
        json_data = request.get_json()
        changes = {"doc": {}}
        if json_data.get("question"):
            changes['doc']['question'] = json_data['question']
        if json_data.get("answer"):
            changes['doc']['answer'] = json_data['answer']
        law_es.update_question(_id, changes)
        return jsonify(es_response)
    elif request.method == 'GET':
        if _id is None:
            return jsonify({
                'errcode': -1,
                'errmsg': 'request is not right, miss _id'
            })
        errcode = 0
        errmsg = "ok"
        es_response = {
            'errcode': errcode,
            'errmsg': errmsg,
            'data':law_es.query_data(_id)
        }
        return jsonify(es_response)


@app.route('/cms/v1/questions', methods=['GET', 'POST'])
def questions():
    if request.method == 'POST':
        if not request.json:
            return jsonify({
                'errcode': -1,
                'errmsg': 'request body is not right, need to be json'
            })
        json_data = request.get_json()
        errcode = 0
        errmsg = "ok"
        es_response = {
            'errcode': errcode,
            'errmsg': errmsg
        }
        if json_data.get("questions"):
            question_items = json_data['questions']
            add_questions(question_items)
            return jsonify(es_response)
        else:
            es_response['errcode'] = -1
            es_response['errmsg'] = "miss 'questions' in request body"
            return jsonify(es_response)
    elif request.method == 'GET':
        topic = request.args.get('topic', None)
        scroll_id = request.args.get('scroll_id', None)
        return jsonify(get_records_by_page(topic=topic, scroll_id=scroll_id))


@app.route('/cms/v1/questions/<_id>', methods=['GET', 'POST'])
def questions_update(_id=None):
    if request.method == 'POST':
        if not request.json or _id is None:
            return jsonify({
                'errcode': -1,
                'errmsg': 'request body is not right, need to be json or miss _id'
            })
        errcode = 0
        errmsg = "ok"
        es_response = {
            'errcode': errcode,
            'errmsg': errmsg
        }
        json_data = request.get_json()
        changes = {"doc": {}}
        if json_data.get("topic"):
            changes['doc']['topic'] = json_data['topic']
        if json_data.get("title"):
            changes['doc']['title'] = json_data['title']
        if json_data.get("question"):
            changes['doc']['question'] = json_data['question']
        if json_data.get("answer"):
            changes['doc']['answer'] = json_data['answer']
        update_question(_id, changes)
        return jsonify(es_response)
    elif request.method == 'GET':
        if _id is None:
            return jsonify({
                'errcode': -1,
                'errmsg': 'request is not right, miss _id'
            })
        errcode = 0
        errmsg = "ok"
        es_response = {
            'errcode': errcode,
            'errmsg': errmsg,
            'data': get_record_by_id(_id)
        }
        return jsonify(es_response)


@app.route('/cms/v1/topics', methods=['GET'])
def tags():
    return jsonify(get_tags())


# @app.route('/es/query', methods=['POST'])
# def es_query():
#     if not request.json:
#         return jsonify({
#             'errcode': -1,
#             'errmsg': 'request body is not right, need to be json'
#         })
#     json_data = request.get_json()
#
#     errcode = 0
#     errmsg = "ok"
#     es_response = {
#         'errcode': errcode,
#         'errmsg': errmsg
#     }
#
#     args_count = 0
#     type_check = 0
#     if json_data.get("intents"):
#         intents = json_data['intents']
#         args_count = args_count + 1
#         if not isinstance(intents,list):
#             type_check = type_check +1
#
#     if json_data.get("slots"):
#         slots = json_data['slots']
#         args_count = args_count + 1
#         if not isinstance(slots,list):
#             type_check = type_check +1
#
#     if json_data.get("domain"):
#         domain = json_data['domain']
#         args_count = args_count + 1
#         if not isinstance(domain,str):
#             type_check = type_check +1
#
#     if args_count != 3:
#         es_response['errcode'] = -2
#         es_response['errmsg'] = "Missing some args in [intents,slots,domain]!"
#
#     if type_check != 0:
#         es_response['errcode'] = -3
#         es_response['errmsg'] = "type of args is not right! [list,string]"
#
#     if args_count != 3 or type_check != 0:
#         return jsonify(es_response)
#
#     slots_str = ' '.join(intents)
#     intents_str = ' '.join(intents)
#     my_query = slots_str+' '+intents_str+' '+domain
#     data = get_result(my_query)
#
#     if len(data) == 0:
#         es_response['errcode'] = -4
#         es_response['errmsg'] = "no result!"
#     else:
#         es_response['data'] = data
#
#     return jsonify(es_response)


if __name__ == '__main__':
    app.run(host='0.0.0.0')