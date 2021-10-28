from grapy import engine
from grapy.core import BaseRequest, Item
from grapy.sched import Scheduler
from grapy.utils import import_spiders
import logging
import yaml
from config import cayley_host
from triple.pipelines import PrintTripleItem, SaveToCayley
import asyncio
import os.path

root_path = os.path.dirname(__file__)

spider_path = os.path.join(root_path, 'triple', 'spiders')

formatter = "[%(asctime)s] %(name)s:%(lineno)d %(levelname)s - %(message)s"
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


async def main(script, *args):
    spiders = import_spiders(spider_path, module_prefix='triple.spiders.')
    if len(spiders) == 0:
        logging.error('Spiders not founds')
        return

    sched = Scheduler(size=20)
    engine.set_sched(sched)
    engine.set_spiders(spiders)
    engine.set_pipelines([SaveToCayley(cayley_host), PrintTripleItem()])

    for arg in args:
        with open(arg, 'r') as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)
            spider = engine.get_spider(config['spider'])
            items = spider.setup(config)
            await process_items(engine, config['spider'], items)

    await engine.start()
    await sched.join()


if __name__ == '__main__':
    import sys
    asyncio.run(main(*sys.argv))
