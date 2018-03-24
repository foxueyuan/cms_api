from flask import Flask, request, abort,jsonify
from dal import *

app = Flask(__name__)


@app.route('/')
def index():
    return 'Welcome to FO CMS!'


@app.route('/cms/v1/questions', methods=['GET'])
@app.route('/cms/v1/questions/<_id>', methods=['POST'])
def questions(_id=None):
    if request.method == 'POST':
        if not request.json:
            abort(400)
        json_data = request.get_json()

        final_answer = json_data["final_answer"]
        reviewed = json_data["reviewed"]
        if not isinstance(reviewed,bool):
            abort(400)
        changes = {
            "doc" : {
                "final_answer": final_answer,
                "reviewed": reviewed
            }
        }
        if json_data.get("updated_tag"):
            changes['doc']['updated_tag'] = json_data['updated_tag']
        update_question(_id,changes)
        return jsonify(json_data)
    elif request.method == 'GET':
        tag = request.args.get('tag', None)
        reviewed = request.args.get('reviewed', None)
        scroll_id = request.args.get('scroll_id', None)
        return jsonify(get_records_by_page(tag=tag, reviewed=reviewed, scroll_id=scroll_id))


@app.route('/cms/v1/tags', methods=['GET'])
def tags():
    return jsonify(get_tags())


@app.route('/es/query', methods=['POST'])
def es_query():
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

    args_count = 0
    type_check = 0
    if json_data.get("intents"):
        intents = json_data['intents']
        args_count = args_count + 1
        if not isinstance(intents,list):
            type_check = type_check +1

    if json_data.get("slots"):
        slots = json_data['slots']
        args_count = args_count + 1
        if not isinstance(slots,list):
            type_check = type_check +1

    if json_data.get("domain"):
        domain = json_data['domain']
        args_count = args_count + 1
        if not isinstance(domain,str):
            type_check = type_check +1

    if args_count != 3:
        es_response['errcode'] = -2
        es_response['errmsg'] = "Missing some args in [intents,slots,domain]!"

    if type_check != 0:
        es_response['errcode'] = -3
        es_response['errmsg'] = "type of args is not right! [list,string]"

    if args_count != 3 or type_check != 0:
        return jsonify(es_response)

    slots_str = ' '.join(intents)
    intents_str = ' '.join(intents)
    my_query = slots_str+' '+intents_str+' '+domain
    data = get_result(my_query)

    if len(data) == 0:
        es_response['errcode'] = -4
        es_response['errmsg'] = "no result!"
    else:
        es_response['data'] = data

    return jsonify(es_response)


if __name__ == '__main__':
    app.run(host='0.0.0.0')