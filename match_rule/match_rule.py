# from match_rule.utils.DbUtil import DbUtil
from utils.DbUtil import DbUtil
from python_re_text import PyReText
import os
import sys
import numpy as np
import pandas as pd
from utils.dict import read_dict
from Application import Application
import Application as App
# from boto.ec2.buyreservation import answer
pathname = os.path.dirname(os.path.abspath(__file__))
sys.path.append(pathname)
file_answer_name = "test2.csv"



class get_data:
    App.tasker()
    def __init__(self):
        self.q_original_label = Application.all_data['q_original_label']
        self.q_original_nolabel = Application.all_data['q_original_nolabel']
        
        
def get_dict():
    dict_label = read_dict.dict_label
    dict_use = {v: k for k, v in dict_label.items()}
    return dict_use
    
    
class match_re:

    def __init__(self):
        self.db = DbUtil().getDb()
    def run(self,question):
        p = False
        attr_ids = self.db.query('select DISTINCT attr_id from q_rule')#从表qa_rule里面提取出attr_id
        print(attr_ids)
        re_text = PyReText()
#         answers = []
        for attr_id in attr_ids:
            rules = self.db.query('select * from q_rule where attr_id = %s', 'all_dict', attr_id)
            #这句话没看懂，查看query的函数参数
            rule_type = re_text.getTreeDict(rules=rules, value=question)#从python_re_text调用函数
            if rule_type != None:
#                 answers.append(rule_type)
#             if answers != None:
                p = True
#                 return answers
                return rule_type
        if not p:
            return -1
        
            
def get_answer_rule(data_type):
    f = match_re()
    data = get_data()
    dict_answers = get_dict()
    if data_type == "nolabel":
        questions = data.q_original_nolabel
    elif data_type == "label":
        questions = data.q_original_label
    else:
        print("erro:no data")
    questions["label_predict"] = None
    for i in range(0 , len(questions)):
        question = questions.question[i]
        answer = float(f.run(question= question))
#         answer = f.run(question= question)
#         
#         for j in range(0,len(answer)):
#             answer[i] = float(answer[i])
#         for number in answer:

        questions.label_predict[i] = dict_answers[answer]
    questions.to_csv(file_answer_name)
    

if __name__ == '__main__':
    data_type = "label"
    get_answer_rule(data_type= data_type)
#     
#     test = match_re()
#     answer = test.run(("请协助将IPTV5602015270291套餐ID: 181153153 费用减免_购机顶盒送话费_总额240元分10元*24个月赠送_201408 处理当月生效"))

#    