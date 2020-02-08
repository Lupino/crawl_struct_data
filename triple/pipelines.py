import logging
import aiohttp
import json
import requests

logger = logging.getLogger('pipelines')

class PrintTripleItem(object):
    def process(self, item):
        logger.info('"{}" "{}" "{}"'.format(item.entity, item.attr, item.value))
        return item

class SaveToCayley(object):
    def __init__(self, host):
        self._host = host

    async def process(self, item):
        data = json.dumps([{
            "subject": item.entity,
            "predicate": item.attr,
            "object": item.value,
            # "label": item.label,
        }])
        headers = { "Content-Type": "application/json" }
        url = '{}/api/v1/write'.format(self._host)
        async with aiohttp.ClientSession() as client:
            async with client.post(url, data=data, headers=headers) as rsp:
                content = await rsp.read()
                data = json.loads(str(content, 'utf-8'))
                if data.get('result'):
                    logger.info(data['result'])
                if data.get('error'):
                    logger.error(data['error'])

        return item
