import logging

logger = logging.getLogger('pipelines')

class PrintTripleItem(object):
    def process(self, item):
        logger.info('"{}" "{}" "{}"'.format(item.entity, item.attr, item.value))
