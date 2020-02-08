from grapy import engine
from grapy.core import BaseRequest, Item
from grapy.sched import Scheduler
from triple.utils import import_spiders
import logging
import yaml
from config import cayley_host
from triple.pipelines import PrintTripleItem, SaveToCayley

formatter = "[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
logging.basicConfig(level=logging.DEBUG, format=formatter)


async def process_items(engine, spider_name, items):
    for item in items:
        await process_item(engine, spider_name, item)

async def process_item(engine, spider_name, item):
    if isinstance(item, BaseRequest):
        item.spider = spider_name
        await engine.push_req(item)
    elif isinstance(item, Item):
        await engine.push_item(item)

def main(script, *args):

    spiders = import_spiders('triple')

    sched = Scheduler()
    engine.set_sched(sched)
    engine.set_spiders(spiders)
    engine.set_pipelines([SaveToCayley(cayley_host), PrintTripleItem()])

    for arg in args:
        with open(arg, 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)
            spider = engine.get_spider(config['spider'])
            items = spider.setup(config)
            engine.loop.create_task(process_items(engine, config['spider'], items))

    engine.start()

if __name__ == '__main__':
    import sys
    main(*sys.argv)
