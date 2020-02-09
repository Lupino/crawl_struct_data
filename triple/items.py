from grapy.core import Item


class TripleItem(Item):
    _filed = [
        {
            'name': 'entity',
            'type': 'str'
        },
        {
            'name': 'attr',
            'type': 'str'
        },
        {
            'name': 'value',
            'type': 'str'
        },
    ]


def tripleItem(entity, attr, value):
    return TripleItem({'entity': entity, 'attr': attr, 'value': value})
