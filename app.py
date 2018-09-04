from flask import Flask, request, abort,jsonify
from qa_es import *
from datetime import datetime
import time
app = Flask(__name__)
law_es = LawElasticSearch()
question_es = QuestionElasticSearch()
topic_es = TopicElasticSearch()


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
        title = request.args.get('title', None)
        question = request.args.get('question', None)
        answer = request.args.get('answer', None)
        input_string = []
        if title:
            input_string.append({"match": {"title": title}})
        if question:
            input_string.append({"match": {"question": question}})
        if answer:
            input_string.append({"match": {"answer": answer}})
        scroll_id = request.args.get('scroll_id', None)
        return jsonify(law_es.get_laws_by_page(inputs=input_string, scroll_id=scroll_id))


@app.route('/cms/v1/laws/<_id>', methods=['GET', 'POST', 'DELETE'])
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
        changes = {"doc": {
            "updatedAt": int(time.time())
        }}
        if json_data.get("title"):
            changes['doc']['title'] = json_data['title']
        if json_data.get("question"):
            changes['doc']['question'] = json_data['question']
        if json_data.get("answer"):
            changes['doc']['answer'] = json_data['answer']
        updated_result = law_es.update_question(_id, changes)
        if updated_result['updated']:
            return jsonify(es_response)
        else:
            es_response['errcode'] = -1
            es_response['errmsg'] = updated_result['msg']
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
            'data': law_es.query_data(_id)
        }
        return jsonify(es_response)
    elif request.method == 'DELETE':
        errcode = 0
        errmsg = "ok"
        es_response = {
            'errcode': errcode,
            'errmsg': errmsg
        }
        deleted_result = law_es.delete_question(_id)
        if deleted_result['deleted']:
            return jsonify(es_response)
        else:
            es_response['errcode'] = -1
            es_response['errmsg'] = deleted_result['msg']
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
            topics_items = [item['topic'] for item in question_items]
            result_msg = topic_es.exist_topic(list(set(topics_items)))
            if not result_msg['exist']:
                es_response['errcode'] = -1
                es_response['errmsg'] = result_msg['msg']
                return jsonify(es_response)
            question_es.add_data_bulk(question_items)
            return jsonify(es_response)
        else:
            es_response['errcode'] = -1
            es_response['errmsg'] = "miss 'questions' in request body"
            return jsonify(es_response)
    elif request.method == 'GET':
        topic = request.args.get('topic', None)
        title = request.args.get('title', None)
        question = request.args.get('question', None)
        answer = request.args.get('answer', None)
        input_string = []
        if topic:
            input_string.append({"match_phrase": {"topic": topic}})
        if title:
            input_string.append({"match": {"title": title}})
        if question:
            input_string.append({"match": {"question": question}})
        if answer:
            input_string.append({"match": {"answer": answer}})
        scroll_id = request.args.get('scroll_id', None)
        return jsonify(question_es.get_questions_by_page(inputs=input_string, scroll_id=scroll_id))


@app.route('/cms/v1/questions/<_id>', methods=['GET', 'POST', 'DELETE'])
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
        changes = {"doc": {
            "updatedAt": int(time.time())
        }}
        if json_data.get("topic"):
            topics_items = [json_data['topic']]
            result_msg = topic_es.exist_topic(topics_items)
            if not result_msg['exist']:
                es_response['errcode'] = -1
                es_response['errmsg'] = result_msg['msg']
                return jsonify(es_response)
            changes['doc']['topic'] = json_data['topic']
        if json_data.get("title"):
            changes['doc']['title'] = json_data['title']
        if json_data.get("question"):
            changes['doc']['question'] = json_data['question']
        if json_data.get("answer"):
            changes['doc']['answer'] = json_data['answer']
        updated_result = question_es.update_question(_id, changes)
        if updated_result['updated']:
            return jsonify(es_response)
        else:
            es_response['errcode'] = -1
            es_response['errmsg'] = updated_result['msg']
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
            'data': question_es.query_data(_id)
        }
        return jsonify(es_response)
    elif request.method == 'DELETE':
        errcode = 0
        errmsg = "ok"
        es_response = {
            'errcode': errcode,
            'errmsg': errmsg
        }
        deleted_result = question_es.delete_question(_id)
        if deleted_result['deleted']:
            return jsonify(es_response)
        else:
            es_response['errcode'] = -1
            es_response['errmsg'] = deleted_result['msg']
            return jsonify(es_response)


@app.route('/cms/v1/topics', methods=['GET', 'POST'])
def topics():
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
        if json_data.get("topics"):
            topic_items = json_data['topics']
            topic_es.add_data_bulk(topic_items)
            return jsonify(es_response)
        else:
            es_response['errcode'] = -1
            es_response['errmsg'] = "miss 'questions' in request body"
            return jsonify(es_response)
    elif request.method == 'GET':
        return jsonify(topic_es.get_topics())


@app.route('/cms/v1/topics/<_id>', methods=['GET','POST', 'DELETE'])
def topics_update(_id=None):
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
        changes = {"doc": {
            "updated": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        }}
        if json_data.get("topic"):
            changes['doc']['topic'] = json_data['topic']
        topic_es.update_topic(_id, changes)
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
            'data': topic_es.query_data(_id)
        }
        return jsonify(es_response)
    elif request.method == 'DELETE':
        errcode = 0
        errmsg = "ok"
        es_response = {
            'errcode': errcode,
            'errmsg': errmsg
        }
        success = topic_es.delete_topic(_id)
        if not success:
            return jsonify({
                'errcode': -1,
                'errmsg': 'fail to remove data'
            })
        return jsonify(es_response)


if __name__ == '__main__':
    app.run(host='0.0.0.0')