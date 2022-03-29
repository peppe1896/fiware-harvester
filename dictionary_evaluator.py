import requests
import re

# Controlla il dizionario di s4c - Crea un nuovo value_type che può essere aggiunto agli attributi di s4c
class DictEval():
    def __init__(self, db_helper, base_link_dict):
        self.dictionary = []
        self.dictionary_link = base_link_dict
        self.db_helper = db_helper
        self._load_dict()


    def _load_dict(self):
        _temp_dict = requests.get(self.dictionary_link+"?get_all").json()
        if _temp_dict["result"] == "OK" and _temp_dict["code"] == 200:
            self.dictionary = _temp_dict["content"]

    # attribute_name talvolta è già value_type. Ad esempio quando prendo il dato da un attributo checked.
    def fit_value_type(self, attribute_name, silent=True):
        _possibilities = []
        for item in self.dictionary:
            if item["type"] == "value type":
                if item["value"] == attribute_name:
                    return item["value"], item["id"]
                _attribute_name = attribute_name.lower()
                _attribute_name = re.sub("[ -_]", "", _attribute_name)
                _value_name = item["value"].lower()
                _value_name = re.sub("[ -_]", "", _value_name)
                _label = item["label"].lower()
                _label = re.sub("[ -_]", "", _label)
                if len(_attribute_name) > len(_value_name):
                    _min_string = _value_name
                    _max_string = _attribute_name
                else:
                    _min_string = _attribute_name
                    _max_string = _value_name
                if _max_string.find(_min_string) != -1:
                    _possibilities.append((item["value"], item["id"]))
                    continue
                elif _min_string.find(_max_string) != -1:
                    _possibilities.append((item["value"], item["id"]))
                    continue
                _iterator = 1
                while _iterator < len(_max_string) // 2:
                    if _max_string.find(_min_string, _iterator) != -1:
                        _possibilities.append((item["value"], item["id"]))
                        continue
                    _iterator += 1
        if not silent:
            print(f"Attribute '{attribute_name}': try to find some value_type simil to this name.")
            print(f"Found some definition (exactly {len(_possibilities)}): "+str(_possibilities))
        return _possibilities
