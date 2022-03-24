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

    # Elaborazione delle regole per la sostituzione automatica
    # nome_regola | [{"field":"", "operator": operatore, "value":valore}..] | [{field:value_type, valueThen: "valore da assegnare"}] | Organizzazione | timestamp

    def fit_value_type(self, attribute_name):
        for item in self.dictionary:
            if item["type"] == "value type":
                _attribute_name = attribute_name.lower()
                _label = item["label"]
                _value_name = item["value"].lower()
                _value_type = None
                if not re.search(_value_name, _attribute_name) or not re.search(_label, _attribute_name):
                    if len(_attribute_name) < 8:
                        continue
                    _middle = len(_value_name)//2
                    _found = False
                    _iterator = 1
                    while not _found or _iterator < _middle:
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

    # Bisogna trovare il modo di fare una query usando le regex

    # ID | value | label | type | data_type_id[] | data_type_value[] | parent_id[] | parent_value | children_id[] | children_value[]
    # metadata: [actionType, ignoreType, noAttrDetail
    sss = """{
			"id":"694",
			"value":"timestamp",
			"label":"Timestamp",
			"type":"value type",
			"data_type_id":[
				"920"
			],
			"data_type_value":[
				"string"
			],
			"parent_id":"",
			"parent_value":"",
			"children_id":[
				"784"
			],
			"children_value":[
				"timestamp"
			]
	} """