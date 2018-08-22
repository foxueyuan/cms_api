# -*- coding: utf-8 -*-

import requests
from config import *

HEADER = {'content-type': "application/json"}


class SyncES(object):
    """
        知识库 问答库 和 训练集 同步 接口
    """
    def __init__(self):
        self.host = REMOTE_ES_HOST

    def post(self,intent,body):
        url = "{}/{}".format(self.host,intent)
        response = requests.put(url, json=body, headers=HEADER)
        return response.status_code,response.json()

    def get_all(self,intent):
        url = "{}/{}".format(self.host, intent)
        response = requests.get(url,headers=HEADER)
        return response.status_code,response.json()

    def get(self,intent,id):
        url = "{}/{}/doc/{}".format(self.host, intent,id)
        response = requests.get(url,headers=HEADER)
        return response.status_code,response.json()

    def delete(self,intent,id):
        url = "{}/{}/doc/{}".format(self.host, intent, id)
        response = requests.delete(url)
        return response.status_code, response.json()

    def update(self,intent,id,body):
        url = "{}/{}/doc/{}".format(self.host, intent, id)
        print(url)
        response = requests.post(url, json=body, headers=HEADER)
        return response.status_code, response.json()

    def get_by_title(self,intent,title):
        url = "{}/{}?title={}".format(self.host, intent,title)
        response = requests.get(url,headers=HEADER)
        return response.status_code,response.json()

    def delete_by_title(self,intent,title):
        url = "{}/{}?title={}".format(self.host, intent, title)
        response = requests.delete(url)
        return response.status_code, response.json()

    def update_title(self,intent,title,body):
        url = "{}/{}?title={}".format(self.host, intent, title)
        print(url)
        response = requests.post(url, json=body, headers=HEADER)
        return response.status_code, response.json()


if __name__ == '__main__':
    sync_es = SyncES()
    #get_all
    # status,result = sync_es.get_all("qa")
    # print(status)
    # for doc in result['result']['docs']:
    #     print(doc['title'])
    #     print(doc['_id'])

    #getByID
    # status, result = sync_es.get("qa",'AWVTIcv0H5zGG77qDiKT')
    # print(status, result)

    #update
    # status = sync_es.update('qa','AWVTIcv0H5zGG77qDiKT',{"title":"盗窃罪五万左右判多久"})
    # print(status)


    #delete
    # status, result = sync_es.delete('qa','AWVgHoxwH5zGG77qDiLT')
    # print(status, result)

    #post
    # body = {'title': '我的测试A', 'question': ['盗窃罪五万左右判多久'],
    #         'answer': '再无其他犯罪情形的情况下，三年以上七年以下，五万元当在5-6年左右。'}
    # status, result = sync_es.post('kg', body)
    # print(status, result)

    #getBytitle

    status, result = sync_es.get_by_title('kg','我的测试A')
    print(status, result)

    #updateBytitle
    # status, result = sync_es.update_title('kg', '我的测试A',{"answer": "三年以上七年以下，五万元当在5-6年左右"})
    # print(status, result)

    #delete_by_title
    # status, result = sync_es.delete_by_title('kg', '我的测试A')
    # print(status, result)
