"""
Converts an NGSI v2 Simplified Representation (a.k.a. keyValues)
into a Normalized Representation
Copyright (c) 2018 FIWARE Foundation e.V.
Author: José Manuel Cantera
"""


def keyValues_2_normalized(entity):
    out = {}
    a = entity.keys()
    for key in entity:
        if key == 'id' or key == 'type':
            out[key] = entity[key]
            continue

        out[key] = {
            'value': entity[key]
        }

        if key == 'location':
            out[key]['type'] = 'geo:json'

        if key.startswith('date'):
            out[key]['type'] = 'DateTime'

        if key == 'address':
            out[key]['type'] = 'PostalAddress'

        if key.startswith('ref'):
            out[key]['type'] = 'Relationship'

        if key.startswith('has'):
            out[key]['type'] = 'Relationship'

    return out
