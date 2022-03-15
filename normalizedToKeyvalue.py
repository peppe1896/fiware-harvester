def normalized_2_keyvalue(entity):
    out = {}

    for key in entity:
        if key == 'id' or key == 'type':
            out[key] = entity[key]
            continue

        if "value" in entity[key].keys():
            out[key] = entity[key]["value"]
        else:
            a = None

    return out
