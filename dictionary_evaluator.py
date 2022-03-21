import requests

class DictEval():
    def __init__(self, base_link_dict, db_helper):
        self.dictionary = {}
        self.dictionary_link = base_link_dict
        self.db_helper = db_helper
        self._load_dict()
        self.a()
        self.analize_schema_attrs("a","a","a","a")
        print()

    def _load_dict(self):
        _temp_dict = requests.get(self.dictionary_link+"?get_all").json()
        if _temp_dict["result"]=="OK" and _temp_dict["code"]==200:
            self.dictionary = _temp_dict["content"]

    # Elaborazione delle regole per la sostituzione automatica
    # nome_regola | [{"field":"", "operator": operatore, "value":valore}..] | [{field:value_type, valueThen: "valore da assegnare"}] | Organizzazione | timestamp

    def analize_all_schema_attrs(self):
        return

    def analize_schema_attrs(self, model, subdomain, domain, version):
        a = self.db_helper.get_unchecked_attrs()
        for tuple in a:
            for val_name in tuple[4]:
                s = "ss"
        print()

    def fit_value_type(self):
        return

    def a(self):
        ss = {}
        for item in self.dictionary:
            _type = item["type"]
            if _type not in ss.keys():
                ss[_type] = 1
            else:
                ss[_type] += 1
        self.ss = ss

    # Bisogna trovare il modo di fare una query usando le regex
    # Porto un esempio di un valore del database

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