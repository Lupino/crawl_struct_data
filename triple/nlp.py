import json
from collections import defaultdict
import ahocorasick
import jieba
import re
import os
import aiohttp

from config import cayley_host

re_sp = re.compile('[;；]')

attr_ac  = None

async def load_attr_ac():
    global attr_ac
    preds = await get_predicates()
    attrs = await get_attrs()
    if attrs:
        preds += attrs
    attr_ac = ahocorasick.Automaton()
    for i, attr in enumerate(preds):
        attr = attr.strip('<>')
        attr_ac.add_word(attr,(i,attr))
    attr_ac.make_automaton()

async def parse_query(question):
    answer, query_type = "", None
    # question = question.upper()
    parts = re.split("：|:|<|>|<=|>=", question)
    en = await _entity_linking(parts[0])

    if len(parts) < 2:
        if len(en):
            query_type = 1
            answer,msg = await _search_single_subj(en[-1]) #查询实体
        else:
            return question, '未识别到实体',-1
    elif 'AND' in question or 'OR' in question:
        query_type = 4
        bool_ops = re.findall('AND|OR',question)
        exps = re.split('AND|OR',question)
        answer,msg = await _search_multi_PO(exps, bool_ops)
    elif len(en) > 0 and len(parts) == 2:
        query_type = 4
        answer, msg = await _search_multihop_SP(parts) # 实体属性查询 and 多跳查询实体
    elif len(await _map_predicate(parts[0])) != 0: #多属性查询实体   todo:需要轮询前后分词，判断属性和分词位置
        query_type = 5
        answer, msg = await _search_multi_PO([question],[])
    elif len(en):
        query_type = 3
        answer, msg = await _search_multihop_SP(parts) # 实体属性查询 and 多跳查询实体
    else:
        msg = '未识别到实体或属性: ' + parts[0]

    return answer, msg, query_type

async def _search_multihop_SP(parts):
    has_done = parts[0]
    v = parts[0]
    ov = parts[0]
    for i in range(1, len(parts)):
        en = await _entity_linking(v)
        if not len(en):
            return '执行到: ' + has_done, '==> 对应的结果为:' + v + ', 知识库中没有该实体: ' + v
        #card, msg = _search_single_subj(en[-1]) #找到了一个对象，所有搜索结果中第一个对象
        p = await _map_predicate(parts[i])
        card, msg = await _search_single_subj_with_po(en[-1],p)
        if not len(p):
            return '执行到: ' + has_done, '==> 知识库中没有该属性: ' + parts[i]
        if not card:
            card = {'subj': 'unknow'}
        p = p[0]
        if p not in card: #判断如果实体没有这个属性，直接返回，说明找到的实体不对。
            return '执行到: ' + has_done, '==> 实体 ' + card['subj'] + ' 没有属性 ' + p
        v = card[p]
        ov = card[p]
        if not isinstance(v,str):
            v = str(v)
        has_done += ":" + parts[i]
    return ov, 'done'

async def _search_multi_PO(exps, bool_ops): #多实体查询
    ans_list = []
    po_list = []
    cmp_dir = {
        "<":"lt",
        "<=":"lte",
        ">":"gt",
        ">=":"gte"
    }

    for e in exps:
        if e == "":
            return "", 'AND 或 OR 后不能为空'

        begin_with_NOT = False
        if e[0:3] == 'NOT':
            begin_with_NOT = True
            e = e[3:]
        elif 'NOT' in e:
            return e, 'NOT请放在PO对前面'

        op = re.findall("：|:|>|<|>=|<=",e)
        if len(op) != 1:
            return e, '语法错误'
        op = op[0]
        if op == '<' or op == '>':
            index = e.find(op)
            if e[index+1] == '=':
                op = op + '='
        pred,  obj  = e.split(op) # todo： 同样判断obj对象和pred属性位置

        pred = pred.strip()
        obj = obj.strip()

        c_pred = await _map_predicate(pred)
        if not len(c_pred):
            return e, '知识库中没有该属性: ' + pred
        if obj == '':
            return e+"?", '属性值不能为空'
        pred = c_pred[0]

        if not begin_with_NOT:
            if op == ':' or op == '：':
                po_list.append([pred, obj])
            else:
                return e, '该属性不支持比较大小'
        else:
            return e, '不支持 NOT'

    or_po = [False] * len(exps)
    should_list = []
    must_list = []
    i = 0
    while i < len(bool_ops):
        if bool_ops[i] == 'OR':
            adjacent_or = [po_list[i]]
            or_po[i] = True
            while i < len(bool_ops) and bool_ops[i] == 'OR':
                adjacent_or.append(po_list[i+1])
                or_po[i+1] = True
                i += 1
            should_list.append(adjacent_or)
        i += 1
    for i,po in enumerate(or_po):
        if not po:
            must_list.append(po_list[i])
    query = ''

    if must_list:
        has_list = ''.join(['.has("<{}>", "{}")'.format(x[0], x[1]) for x in must_list])
        query = 'g.V()' + has_list + '.all()'
        if should_list:
            pass
    else:
        pass

    ret = await cayley_query('gizmo', query)
    if ret:
        return get_objects(ret), 'done'
    else:
        return None, 'none'


async def _search_single_subj(entity_name):
    s = await cayley_query('gizmo', 'graph.V("{}").out("", "pred").all();'.format(entity_name))
    if s:
        card = make_card(entity_name, s)
        return card, 'done'
    else:
        return None, 'entity'


async def _search_single_subj_with_po(entity_name,po_name):
    po_name = ''.join(['"<{}>"'.format(po) for po in po_name])

    s = await cayley_query('gizmo', 'graph.V("{}").out([{}], "pred").all();'.format(entity_name, po_name))
    if s:
        card = make_card(entity_name, s)
        return card, 'done'
    else:
        return None, 'entity'

def make_card(subj, source):
    card = dict()
    card['subj'] = subj
    for s in source:
        attr = s['pred'].strip('<>')
        if attr in card:
            card[attr] += ' ' + s['id']
        else:
            card[attr] = s['id']

    return card

async def translate_NL2LF(nl_query):
    #'''
    # 使用基于模板的方法将自然语言查询转化为logic form
    #'''
    # nl_query = nl_query.upper()
    entity_list = await _entity_linking(nl_query)
    attr_list = await _map_predicate(nl_query,False)
    lf_query = ""
    if entity_list:
        if not attr_list: #如果没识别出属性 那么就是查询实体
            lf_query = entity_list[-1]
        else:
            first_entity_pos = nl_query.find(entity_list[-1]) #实体的位置
            first_attr_pos = nl_query.find(attr_list[0]) #属性的位置
            if len(attr_list) == 1: #一个属性
                if first_entity_pos < first_attr_pos: #判断属性位置
                    lf_query = "{}:{}".format(entity_list[-1], attr_list[0])
                else:
                    lf_query = "{}:{}".format(attr_list[0], entity_list[-1])
            else: #多个属性
                lf_query = entity_list[-1]
                for pred in attr_list:
                    lf_query += ":" + pred
    else:
        val_d = await _val_linking(nl_query)

        attr_pos = {}
        val_pos = {}

        for a in attr_list:
            attr_pos[a] = nl_query.find(a)

        for v in val_d:
            val_pos[v] = nl_query.find(v)

        retain_attr = []
        # for a in attr_pos:
        #     mapped_a = await attr_map(a)
        #     if mapped_a in number_attrs:
        #         retain_attr.append(a)

        # for a in number_attrs:
        #     if nl_query.find(a) > -1 and a not in retain_attr:
        #             retain_attr.append(a)

        tmp = {}
        for v in val_pos:
            to_retain = True
            for a in attr_pos:
                if(val_pos[v] >= attr_pos[a] and val_pos[v] + len(v) <= attr_pos[a] + len(a)):
                    to_retain = False
                    break
            if to_retain:
                tmp[v] = val_d[v]
        val_d = tmp

        has_attr = len(attr_pos) > 0

        final_val_d= {}
        for v in val_d:
            # if val_d[v] in number_attrs:
            #     if val_d[v] not in attr_pos and not has_attr:
            #         retain_attr.append(val_d[v])
            #         attr_pos[val_d[v]] = 0
            #     continue

            if not (v.isdigit() or v in '大于' or v in '小于'):
                final_val_d[v] = val_d[v]


        part_queries = []
        for a in retain_attr:
            mapped_a = await attr_map(a)
            part_query = ""

            if part_query:
                part_queries.append(part_query)

        for q in part_queries:
            if not lf_query:
                lf_query += q
            else:
                lf_query += ' AND ' + q

        prev_pred = []
        for v, pred in final_val_d.items():
            if len(v) < 2:
                continue
            if pred in prev_pred:
                lf_query += ' OR ' + '{}:{}'.format(pred, v)
            else:
                if not lf_query:
                    lf_query = '{}:{}'.format(pred, v)
                else:
                    lf_query += ' AND ' + '{}:{}'.format(pred, v)
                prev_pred.append(pred)
    return lf_query

def _remove_dup(word_list):  #删除多余的词组，只留下一个属性
    #'''
    #args:
    #    word_list: 一个字符串的list
    #'''
    distinct_word_list = []
    for i in range(len(word_list)):
        is_dup = False
        for j in range(len(word_list)):
            if j != i and word_list[i] in word_list[j]:
                is_dup = True
                break
        if not is_dup:
            distinct_word_list.append(word_list[i])
    return distinct_word_list


async def _map_predicate(pred_name, map_attr=True):   #找出一个字符串中是否包含知识库中的属性

    async def _map_attr(word_list):
        ans = []
        for word in word_list:
            an = await attr_map(word)
            ans.append(an)
        return ans

    match = []
    for w in attr_ac.iter(pred_name):
        match.append(w[1][1])
    if not len(match):
        return []

    ans = _remove_dup(match)
    if map_attr:
        ans = await _map_attr(ans)
    return ans

def _generate_ngram_word(word_list_gen):
    #'''
    #args:
    #    word_list_gen: 一个字符串的迭代器
    #'''
    word_list = []
    for w in word_list_gen:
        word_list.append(w)
    n = len(word_list)
    ans = []
    for i in range(1, n+1):
        for j in range(0,n+1-i):
            ans.append(''.join(word_list[j:j+i]))
    return ans

async def _entity_linking(entity_name):    #找出一个字符串中是否包含知识库中的实体，这里是字典匹配，可以用检索代替
    parts = re.split(r'的|是|有', entity_name)
    ans = []
    ans1 = ""
    for p in parts:
        pp = jieba.cut(p)
        if pp is not None:
            for phrase in _generate_ngram_word(pp):
                val = await get_out_predicates(phrase)
                if val:
                    ent = await ent_map(phrase)
                    ans.append(ent)
    return ans

async def _val_linking(nl_query):
    parts = re.split(r'的|是|有', nl_query)
    hit_val = []
    val_dict = {}
    for p in parts:
        for phrase in _generate_ngram_word(p):
            val = await get_in_predicates(phrase)
            print(val)
            if val:
                val_dict[phrase] = val
                hit_val.append(phrase)

    hit_val = _remove_dup(hit_val)
    ans = {}
    for p in hit_val:
        print(val_dict)
        ans[p] = val_dict[p][0]

    return ans

class TypeError(Exception):
    pass

async def cayley_query(method, query):
    url = cayley_host + '/api/v1/query/' + method

    async with aiohttp.ClientSession() as client:
        async with client.post(url, data=query) as rsp:
            content = await rsp.read()
            # print(content)
            data = json.loads(str(content, 'utf-8'))
            if data.get('result'):
                return data['result']
            if data.get('error'):
                error = data['error']
                if error.startswith('TypeError'):
                    raise TypeError(data['error'][11:])
                else:
                    raise Exception(error)

def get_objects(data):
    if data:
        return [x['id'].strip('<>') for x in data]
    return None

async def get_predicates(value = '', is_in = True):
    pred = 'inPredicates' if is_in else 'outPredicates'
    if value:
        value = '"' + value + '"'
    ret = await cayley_query('gizmo', 'graph.V({}).{}().all()'.format(value, pred))
    return get_objects(ret)

async def get_in_predicates(value):
    return await get_predicates(value, True)

async def get_out_predicates(value):
    return await get_predicates(value, False)

async def cayley_has(attr):
    ret = await cayley_query('gizmo', 'graph.V().has("{}").all()'.format(attr))
    return get_objects(ret)

async def get_attrs():
    return await cayley_has('<attr_mapping>')

async def attr_map(attr):
    ret = await cayley_query('gizmo', 'graph.V("<{}>").out("<attr_mapping>").all()'.format(attr))
    if ret:
        return ret[0]['id'].strip('<>')
    return attr

async def ent_map(ent):
    ret = await cayley_query('gizmo', 'graph.V("{}").out("<entity_mapping>").all()'.format(ent))
    if ret:
        return ret[0]['id']
    return ent
