import re
from triple.nlp import translate_NL2LF, parse_query, load_attr_ac
import asyncio

async def do_query(query):
    lf_question = await translate_NL2LF(query)
    print(lf_question)
    answer, msg, query_type = await parse_query(lf_question)
    if query_type == 5 and isinstance(answer, list):
        word = lf_question.split(':')[1]
        out = []
        for ans in answer:
            q = query.replace(word, ans[1])
            lf_qu = await translate_NL2LF(q)
            if lf_qu == ans[1]:
                continue

            parts = re.split("：|:|<|>|<=|>=", lf_qu)
            if len(parts) > 2:
                continue

            if 'AND' in lf_qu or 'OR' in lf_qu:
                continue

            ret = await parse_query(lf_qu)

            if ret[1] == 'done' and ret[2] == 3:
                query_type = 6
                out.append({'subj': ans[1], 'result': ret[0]})

        if query_type == 6:
            answer = out

    return answer, msg, query_type

async def do_query_with_diff(raw_query):
    query = raw_query.split('比')
    q = await translate_NL2LF(query[1])
    q = q.split(':')
    ret1 = do_query(query[1])
    if len(q) == 2:
        q2 = query[0] + query[1].replace(q[0], '')
        ret2 = do_query(q2)

        if ret1[1] == 'done' and ret2[1] == 'done':
            ret = []
            if raw_query.find('不同') > -1:
                for i in ret1[0]:
                    if i not in ret2[0]:
                        ret.append(i)
            else:
                for i in ret1[0]:
                    if i in ret2[0]:
                        ret.append(i)

            return ret, 'done', 10


    return [], 'error', 10

async def test(query):
    await load_attr_ac()
    print('====================================================================')
    print(query)
    if query.find('比') > -1:
        answer, msg, query_type = await do_query_with_diff(query)
    else:
        answer, msg, query_type = await do_query(query)
    print(answer, msg, query_type)
    print('====================================================================')

# asyncio.run(test('ffmpeg下载地址'))
asyncio.run(test('使用Java语言的项目'))
