import os

def add_to_log(message, log):
    log += message + "\n"

def create_folders(folders:list):
    for _folder in folders:
        os.makedirs(_folder, exist_ok=True)

# Pass a payload. See if the structure of it is a normalized structure, or just keyvalue.
def is_normalized(payload):
    for key in payload:
        if key == 'id' or key == 'type':
            continue
        if type(payload[key]) is dict:
            if "value" in payload[key].keys():
                return False
            else:
                return True
        elif type(payload[key]) is str:
            return False

# Convert a normalized payload into keyvalue payload
def normalized_2_keyvalue(payload):
    out = {}
    meta_schema = {}
    for key in payload:
        if key == 'id' or key == 'type':
            out[key] = payload[key]
            continue

        if "value" in payload[key].keys():
            out[key] = payload[key]["value"]
        else:
            print(f"'value' not found in {key}")
        if "metadata" in payload[key].keys():
            if len(payload[key]["metadata"]) > 0:
                meta_schema[key] = payload[key]["metadata"]

    return out, meta_schema

def json_is_equals(json_a, json_b):
    def ordered(obj):
        if isinstance(obj, dict):
            return sorted((k, ordered(v)) for k, v in obj.items())
        if isinstance(obj, list):
            return sorted(ordered(x) for x in obj)
        else:
            return obj

    return ordered(json_a) == ordered(json_b)