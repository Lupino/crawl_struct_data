from grapy.core import Item

class TripleItem(Item):
    _filed = [
        {'name': 'entity', 'type': 'str'},
        {'name': 'attr',   'type': 'str'},
        {'name': 'value',  'type': 'json'}, # use dyn type
    ]
