#-*-coding=utf-8-*-


from utils.Constant import Constant

class PyReText(object):

#对一条规则进行比对
    def run_rule(self, rule_item, value):
        rule_symb = rule_item.get(Constant.DbTable.RULE_SYMB)
        rule_value = rule_item.get(Constant.DbTable.RULE_VALUE)
        # print('rule_express:'+rule_express+'  rule_value:' +rule_value)
        if rule_symb == '=':
            return value == rule_value
        elif rule_symb == 'like':
            return value.find(rule_value) != -1
        elif rule_symb == 'nolike':
            return value.find(rule_value) == -1

        return False


# 匹配规则树 返回result，true false
    def invoke_rule_tree(self, level_dict,rule_dict,value):
        max_level = max(level_dict.keys())
        rule_list = level_dict.get(max_level)
        prev_list = []
        rule_result_dict = {}
        pid_temp = {} # 保存pid，判断是否重复
        for rule_item in rule_list:
            pid = rule_item.get(Constant.DbTable.PID)
            rule_result = self.run_rule(rule_item, value)
            # print('----' + str(rule_result))
            # 保存结果
            result_list = rule_result_dict.get(pid)

            # 是最上级就返回
            if pid == -1:
                return rule_result

            if result_list is None:
                rule_result_dict[pid] = []
                result_list = rule_result_dict.get(pid)
            result_list.append(rule_result)

            # 获取上级信息
            p_rule = pid_temp.get(pid)
            if not p_rule:
                parent_rule = rule_dict.get(pid)
                pid_temp[pid] = parent_rule
                if parent_rule:
                    prev_list.append(parent_rule)

        max_level = max_level-1
        rule_list = level_dict.get(max_level)
        if rule_list:
            prev_list.extend(rule_list)

        while len(prev_list) > 0:
            pre_list_temp = []
            for rule_item in prev_list:
                rule_id = rule_item.get(Constant.DbTable.RULE_ID)
                pid = rule_item.get(Constant.DbTable.PID)
                rule_symb = rule_item.get(Constant.DbTable.RULE_SYMB)

                if rule_symb is None:
                    # 父级只判断子级结果
                    # 只有2个结果？
                    rule_result_sub = rule_result_dict.get(rule_id)
                    # print('--------------')
                    # print(rule_result_sub)
                    # print('--------------')
                    rule_operator = rule_item.get(Constant.DbTable.RULE_OPERATOR)
                    if rule_operator == '||':
                        rule_result = rule_result_sub[0] or rule_result_sub[1]
                    elif rule_operator == '&&':
                        rule_result = rule_result_sub[0] and rule_result_sub[1]
                    else:
                        rule_result = False
                    # print('----2' + str(rule_result_sub))
                else:
                    rule_result = self.run_rule(rule_item, value)
                    # print('----1' + str(rule_result))

                # 是最上级就返回
                if pid == -1:
                    return rule_result

                # 保存结果
                result_list = rule_result_dict.get(pid)
                if result_list is None:
                    rule_result_dict[pid] = []
                    result_list = rule_result_dict.get(pid)
                result_list.append(rule_result)

                p_rule = pid_temp.get(pid)
                if not p_rule:
                    parent_rule = rule_dict.get(pid)
                    pid_temp[pid] = parent_rule
                    if parent_rule:
                        pre_list_temp.append(parent_rule)

            max_level = max_level-1
            rule_list = level_dict.get(max_level)
            if rule_list:
                pre_list_temp.extend(rule_list)
            prev_list = pre_list_temp




# 匹配value（传入文本）


    def getTreeDict(self, rules,value):
        pid_dict = {} # 根据pid存items,数组
        rule_dict = {}
        level_dict = {} # 存数据的层级
        tree_dict = {} # 存最后结果

        for rule in rules:

            pid = rule.get(Constant.DbTable.PID)

            #规则字典  k-v rule id :rule
            rule_dict[rule.get(Constant.DbTable.RULE_ID)] = rule

            #填充pid_dic k-v rule_id(pid):[rule1,rule2]
            if pid is None:
                pid = -1
            items = pid_dict.get(pid)
            if items is None:
                pid_dict[pid] = []
                items = pid_dict.get(pid)
            items.append(rule)

        rule_type = pid_dict[-1][0].get(Constant.DbTable.RULE_TYPE)

        level_val = 0

        #顶级节点集
        first_item = pid_dict[-1]

        if first_item:
            #填充level_dict
            for rule_item in first_item:
                rule_id = rule_item.get(Constant.DbTable.RULE_ID)
                tree_dict[rule_id] = rule_item
                rule_item[Constant.ReTextInfo.ITEMS] = []
                sub_items = rule_item.get(Constant.ReTextInfo.ITEMS)

                is_val = self.deal_tree_sub_item(pid_dict, level_dict, level_val + 1, rule_id, sub_items)

                if not is_val:
                    level_item = level_dict.get(level_val)
                    if level_item is None:
                        level_dict[level_val] = []
                        level_item = level_dict.get(level_val)
                    level_item.append(rule_item)

        result = self.invoke_rule_tree(level_dict,rule_dict,value)
        # print('result:'+str(result))
        if result :
            return rule_type
        else:
            None



    def deal_tree_sub_item(self, pid_dict, level_dict, level_val, pid, items):
        sub_items = pid_dict.get(pid)
        if sub_items :
            items.extend(sub_items)
            for rule_item in sub_items:
                rule_id = rule_item.get(Constant.DbTable.RULE_ID)
                rule_item[Constant.ReTextInfo.ITEMS] = []
                sub_items = rule_item.get(Constant.ReTextInfo.ITEMS)
                is_val = self.deal_tree_sub_item(pid_dict, level_dict, level_val + 1, rule_id, sub_items)
                if not is_val:
                    level_item = level_dict.get(level_val)
                    if level_item is None:
                        level_dict[level_val] = []
                        level_item = level_dict.get(level_val)
                    level_item.append(rule_item)
            return True
        else:
            return False


