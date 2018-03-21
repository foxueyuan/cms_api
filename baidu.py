#!/usr/bin/python
# -*- coding: utf-8 -*-

from aip import AipNlp
from six import unichr


if __name__ == '__main__':
    """ 你的 APPID AK SK """
    APP_ID = '10960775'
    API_KEY = '91RL6dPzbiViYRPPtZc19P0L'
    SECRET_KEY = 'sEO7GR3qxqGwbRu80HFhtftYRjRejAXk'
    # title = "iphone手机出现“白苹果”原因及解决办法，用苹果手机的可以看下"
    # content= "如果下面的方法还是没有解决你的问题建议来我们门店看下成都市锦江区红星路三段99号银石广场24层01室。在通电的情况下掉进清水，这种情况一不需要拆机处理。尽快断电。用力甩干，但别把机器甩掉，主意要把屏幕内的水甩出来。如果屏幕残留有水滴，干后会有痕迹。^H3 放在台灯，射灯等轻微热源下让水分慢慢散去。"
    title = "劳动合同期限内员工基本工资是每月固定的吗？"
    content = '入职时劳动合同约定试用期基本工资工资为转正后工资80%，人事出具的工资条试用期的基本工资 = 电话补助+绩效考核+餐补+交通补助 等等。这样一来所谓的基本工资就被定性成包含人为可调减得项目！这样符合劳动法么？难道不是电话补助、餐补、交通补助 本应是在基本工资之外的额外补助么？！'
    content = "".join(filter(str.isalnum,content))
    client = AipNlp(APP_ID, API_KEY, SECRET_KEY)
    returnItem = client.keyword(title,content)
    print(returnItem)
