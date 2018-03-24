#!/usr/bin/python
# -*- coding: utf-8 -*-

from aip import AipNlp


APP_ID = '10960775'
API_KEY = '91RL6dPzbiViYRPPtZc19P0L'
SECRET_KEY = 'sEO7GR3qxqGwbRu80HFhtftYRjRejAXk'

client = AipNlp(APP_ID, API_KEY, SECRET_KEY)


def get_tags_from_baidu(title, question, answers):
    """
    get tags fromm baidu
    :param title:
    :param question:
    :param answers:
    :return:
    """
    good_title = _limit_title_length(title)
    question_tag = client.keyword(good_title, question)
    answer_tag = client.keyword(good_title, answers)
    tag_set = set()
    if question_tag.get('items'):
        for item in question_tag['items']:
            tag_set.add(item['tag'])

    if answer_tag.get('items'):
        for item in answer_tag['items']:
            tag_set.add(item['tag'])

    return " ".join(tag_set)


def _limit_title_length(title):
    """
    baidu's NLP function of keyword has length limitation of title, when encoding is GBK, the largest length is 80.
    This function remove the length of exceeding from the beginning of the title.
    :param title:
    :return:
    """
    if len(title.encode('gbk')) > 80:
        gap = len(title.encode('gbk')) - 80
        begin = int(gap / 2)
        new_title = title[begin:]
        return new_title.replace(u'\xa0', ' ')
    else:
        return title



if __name__ == '__main__':
    title = "iphone手机出现“白苹果”原因及解决办法，用苹果手机的可以看下"
    content = '如果下面的方法还是没有解决你的问题建议来我们门店看下成都市锦江区红星路三段99号银石广场24层01室。在通电的情况下掉进清水，这种情况一不需要拆机处理。尽快断电。用力甩干，但别把机器甩掉，主意要把屏幕内的水甩出来。如果屏幕残留有水滴，干后会有痕迹。^H3 放在台灯，射灯等轻微热源下让水分慢慢散去。'
    question = "如果下面的方法还是没有解决你的问题建议来我们门店看下成都市锦江区红星路三段99号银石广场24层01室。在通电的情况下掉进清水，这种情况一不需要拆机处理。尽快断电。用力甩干，但别把机器甩掉，主意要把屏幕内的水甩出来。如果屏幕残留有水滴，干后会有痕迹。^H3 放在台灯，射灯等轻微热源下让水分慢慢散去。"
    client = AipNlp(APP_ID, API_KEY, SECRET_KEY)
    returnItem = get_tags_from_baidu(title, question, content)
    print(returnItem)
