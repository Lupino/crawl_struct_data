import os
import glob
from importlib import import_module
import logging

root_path = os.path.dirname(__file__)

spider_path = os.path.join(root_path, 'spiders')

def import_spiders(project_name):
    spiders = []

    for path in glob.glob(spider_path + '/*.py'):
        if path.endswith('__init__.py'):
            continue

        spider_name = os.path.basename(path)[:-3]
        module_path = project_name + '.spiders.' + spider_name

        module = import_module(module_path)
        module_name = spider_name.capitalize() + 'Spider'
        ignore = getattr(module, 'ignore', False)

        if ignore:
            continue

        spider = getattr(module, module_name, None)
        if spider:
            spiders.append(spider())
        else:
            logging.error('{}.{} invalid.'.format(module_path, module_name))

    return spiders
