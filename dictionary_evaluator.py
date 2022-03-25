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

    def fit_value_type(self, attribute_name):
        for item in self.dictionary:
            if item["type"] == "value type":
                _attribute_name = attribute_name.lower()
                _label = item["label"]
                _value_name = item["value"].lower()
                _value_type = None
                if not re.search(_value_name, _attribute_name) or not re.search(_label, _attribute_name):
                    if len(_attribute_name) < 7:
                        continue
                    _middle = len(_attribute_name)//2
                    _found = False
                    _iterator = 1
                    while not _found and _iterator < _middle:
                        _left_part = _attribute_name[_iterator:]
                        if re.search(_left_part, _value_name) or re.search(_left_part, _label):
                            _found = True
                            _value_type = _value_name
                            return _value_type, item["id"]
                        _iterator += 1
                else:
                    _value_type = _value_name
                if _value_type is not None:
                    return _value_type, item["id"]
        return None
