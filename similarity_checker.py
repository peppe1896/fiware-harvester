import dictionary_evaluator
import statics


class SimilarityChecker():
    def __init__(self, db_helper, s4c_dict_link):
        self.db_helper = db_helper
        self.s4c_dictionary = dictionary_evaluator.DictEval(db_helper, s4c_dict_link)

    def fit_value_type(self, attribute_key, schema_tuple):
        if attribute_key == "type" or attribute_key == "id":
            return None
        _model = schema_tuple[0]
        _subdomain = schema_tuple[1]
        _domain = schema_tuple[2]
        _version = schema_tuple[3]
        _value_type_1 = self._check_against_attrs_of_schema(attribute_key, _model, _subdomain, _domain)
        if _value_type_1:
            return _value_type_1
        _value_type_2 = self._check_against_dict(attribute_key)
        if _value_type_2:
            return _value_type_2
        _value_type_3 = self._check_against_attrs_structure(attribute_key, "True", schema_tuple)
        if _value_type_3:
            return _value_type_3
        _value_type_4 = self._check_against_attrs_structure(attribute_key, "", schema_tuple) # Vuol dire che trova tutti, checked false e true
        if _value_type_4:
            return _value_type_4

        return #value_type

    # MAIN CHECK - Se questo è valido, allora ho già il value type
    # Controlla che nello schema previsto dal payload, l'attributo sia checked e quindi con un value_type definito
    # nella stessa voce dell'attributo
    def _check_against_attrs_of_schema(self, attribute, model, subdomain, domain):
        _schema_attrs = self.db_helper.get_attributes(model, subdomain, domain, onlyChecked="True", also_attributes_logs=False, excludeType=True)
        if len(_schema_attrs) > 0:
            if attribute in _schema_attrs.keys():
                return _schema_attrs[attribute]["value_type"]
        return None

    # Controlla che nel dizionario s4c esista un attributo con quel nome
    def _check_against_dict(self, attribute_key):
        return self.s4c_dictionary.fit_value_type(attribute_key)

    # Controllo se tra tutti gli attributi con checked=True di TUTTI gli schema, ho una struttura simile a quella
    # dell'attributo passato in ingresso
    def _check_against_attrs_structure(self, attribute_key, onlyChecked, schema_tuple):
        _all_attributes_checked = self.db_helper.get_attributes(onlyChecked=onlyChecked, excludeType=True)
        # Devo prendere l'attributo raw e confrontarlo con gli attributi che sono checked
        # Con quelli che non lo sono, non è necessario
        _model = schema_tuple[0]
        _subdomain = schema_tuple[1]
        _domain = schema_tuple[2]
        _version = schema_tuple[3]
        _attribute = self.db_helper.get_attribute(attribute_key, _model, _subdomain, _domain, _version)
        if len(_attribute) == 0:
            print(f"No attribute found with name '{attribute_key}'. Control the schema...")
            _schema = self.db_helper.get_model_schema(_model, _subdomain, _domain, _version)
            statics.window_json(_schema, f"Schema tuple: {str(schema_tuple)}\nAttention! No attribute found with name '{attribute_key}'")
        else:
            _attribute = _attribute[0]
            _raw_attribute = _attribute["raw_attribute"]

        return None