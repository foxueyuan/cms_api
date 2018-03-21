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
        return jsonify(get_all_questions(tag=tag, reviewed=reviewed))


@app.route('/cms/v1/tags', methods=['GET'])
def tags():
    return jsonify(get_tags())


if __name__ == '__main__':
    app.run()