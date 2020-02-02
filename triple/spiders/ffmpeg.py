from grapy import BaseSpider, Request
from ..items import TripleItem
import re

re_release = re.compile('https://ffmpeg.org/releases/ffmpeg-(\d+.\d+.\d+).tar.bz2')

class FfmpegSpider(BaseSpider):
    name = "ffmpeg"

    def setup(self, config):
        entity = config['entity']
        for attr, value in config['static'].items():
            triple = TripleItem()
            triple['entity'] = entity
            triple['attr'] = attr
            if not isinstance(value, list):
                value = [value]

            for v in value:
                triple['value'] = v
                yield triple

        for req in config['dynamic']:
            url = req.pop('url')
            parser = req.pop('parser')
            yield Request(url, callback=parser, callback_args = [entity])

    def parse(self, rsp, entity):
        triple = TripleItem()
        triple['entity'] = entity

        elems = rsp.select('#download .btn-download-wrapper a')
        for elem in elems:
            href = elem.get('href')
            m = re_release.search(href)
            if m:
                triple['attr'] = '下载地址'
                triple['value'] = href
                yield triple
                triple['attr'] = '版本号'
                triple['value'] = m.group(1)
                yield triple
