import dictionary_evaluator
import statics
import json

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
        _value_type_1 = self._check_against_attrs_of_schema(attribute_key, _model, _subdomain, _domain, _version)
        if _value_type_1:
            # Qui trovo l'attributo che è checked, quindi il value_type è valido e corrisponde al value type di s4c
            return self.s4c_dictionary.fit_value_type(_value_type_1)

        _value_type_2 = self._check_against_dict(attribute_key)
        if len(_value_type_2) > 0:
            _attrs = json.dumps(_value_type_2)
            statics.window_edit_attribute(
                statics.ATTRIBUTE_MASK,
                attribute_key, f"Update '{attribute_key}'",
                _model, _subdomain, _domain, _version, self.db_helper,
                text=f'Found following match in dictionary of S4C:\n{_attrs}.\nWrite into "value_type" any of this value type,'
                     f' and write "True" in "checked". Otherwise, if you won\'t change, just close this window.')

        _value_type_3 = self._check_against_attrs_structure(attribute_key, "True", schema_tuple)
        if len(_value_type_3) > 0:
            _attrs = json.dumps(_value_type_3)
            statics.window_edit_attribute(
                statics.ATTRIBUTE_MASK,
                attribute_key, f"I couldn't find any attribute for '{attribute_key}'. Would you like to add it now?",
                _model, _subdomain, _domain, _version, self.db_helper,
                text=f'Found following match in dictionary of S4C:\n{_attrs}. Write into "value_type" any of this value type,'
                     f' and write "True" in "checked".')
        #_value_type_4 = self._check_against_attrs_structure(attribute_key, "", schema_tuple) # Vuol dire che trova tutti, checked false e true
        #_value_types.extend(_value_type_4 if _value_type_4 else ["-"])
        # Faccio un altro controllo, che vi sia stato assegnato un value_type
        _value_type_1 = self._check_against_attrs_of_schema(attribute_key, _model, _subdomain, _domain, _version)
        if _value_type_1:
            # Qui trovo l'attributo che è checked, quindi il value_type è valido e corrisponde al value type di s4c
            return self.s4c_dictionary.fit_value_type(_value_type_1)
        #if input("Do you want to create a new attribute?\n ").lower() in ["yes", "y"]:
        #statics.window_edit_attribute(
        #        statics.ATTRIBUTE_MASK,
        #        attribute_key, f"I couldn't find any attribute for '{attribute_key}'. Would you like to add it now?",
        #                         _model, _subdomain, _domain, _version, self.db_helper,
        #                        f"\nIf you're here, the attribute is not inside. Would you like to create a new attribute with '{attribute_key}' name?")
        return None


    # MAIN CHECK - Se questo è valido, allora ho già il value type
    # Controlla che nello schema previsto dal payload, l'attributo sia checked e quindi con un value_type definito
    # nella stessa voce dell'attributo
    def _check_against_attrs_of_schema(self, attribute, model, subdomain, domain, version):
        #_schema_attrs = self.db_helper.get_attributes(model, subdomain, domain, onlyChecked="True", also_attributes_logs=False, excludeType=True)
        _attr = self.db_helper.get_attribute(attribute, model, subdomain, domain, version)
        if _attr:
            if _attr[0][4]["checked"] == "True":
                return _attr["value_type"]
        return None

    # Controlla che nel dizionario s4c esista un attributo con quel nome
    def _check_against_dict(self, attribute_key):
        return self.s4c_dictionary.fit_value_type(attribute_key)

    # Controllo se tra tutti gli attributi con checked=True di TUTTI gli schema, ho una struttura simile a quella
    # dell'attributo passato in ingresso
    def _check_against_attrs_structure(self, attribute_key, onlyChecked, schema_tuple):
        _all_attributes_checked = self.db_helper.get_attributes(onlyChecked=onlyChecked, excludeType=True)
        _model = schema_tuple[0]
        _subdomain = schema_tuple[1]
        _domain = schema_tuple[2]
        _version = schema_tuple[3]
        _attribute = self.db_helper.get_attribute(attribute_key, _model, _subdomain, _domain, _version)
        _possibilities = []
        if len(_attribute) == 0:
            _common_attr = self.db_helper.get_attribute(attribute_key, domain="definition-schemas")
            if len(_common_attr) == 0:
                print(f"No attribute found with name '{attribute_key}'. Control the schema...")
                _schema = self.db_helper.get_model_schema(_model, _subdomain, _domain, _version)
                statics.window_read_json(_schema,
                    f"Schema tuple: {str(schema_tuple)}\nAttention! No attribute found with name '{attribute_key}'",
                    title=f"No attribute found with name '{attribute_key}'. Control the schema...")
                statics.window_edit_attribute(statics.ATTRIBUTE_MASK, attribute_key,
                                              f"Procedure to add a new attribute into schema attributes.",
                                              _model, _subdomain, _domain, _version, self.db_helper,
                                              f"\nCreate a new attribute and save it to add an attribute named '{attribute_key}' into "
                                              f"attributes of model '{_model}', in subdomain '{_subdomain}', domain '{_domain}' and version '{_version}'.")

            elif len(_common_attr) == 1:
                _cm_attr = _common_attr[0]
                if _cm_attr[4]["checked"] == "False":
                    print(f"This attribute '{attribute_key}' is a common attribute.")
                    statics.window_edit_attribute(_cm_attr[4], attribute_key, f"Common attribute '{attribute_key}'",
                                                  model=_cm_attr[0], subdomain=_cm_attr[1], domain=_cm_attr[2], version=_cm_attr[3],
                                                  db=self.db_helper, text="\nUpdate attribute: set checked \"True\", edit eventually some new attributes, and then click update to save changes")
                else:
                    a = None
            else:
                print(f"Found some versions of the common attribute '{attribute_key}'. Edit any of them you need to update, and click on Update.")
                for common in _common_attr:
                    statics.window_edit_attribute(common[4], attribute_key, f"Common_attribute {attribute_key}",
                                                  model=common[0], subdomain=common[1],
                                                  domain=common[2], version=common[3],
                                                  db=self.db_helper,
                                                  text="\nUpdate attribute: set checked \"True\", edit eventually some new attributes, and then click update to save changes")

        else:
            _attribute = _attribute[0][4]
            _raw_attribute = _attribute["raw_attribute"]
            _raw_attr_keys = _raw_attribute.keys()
            if len(_raw_attr_keys) > 2:
                while len(_all_attributes_checked) > 0:
                    _tuple = _all_attributes_checked.pop(0)
                    for _other_attr in _tuple[0]:
                        _obj = _tuple[_other_attr]["raw_attribute"]
                        if not isinstance(_obj, dict):
                            _obj = json.loads(_obj)
                        if len(_obj.keys()) > 2:
                            if statics.json_is_equals(_obj.keys(), _raw_attr_keys):
                                if _obj["type"] == _raw_attribute["type"]:
                                    _possibilities.append(_tuple[_other_attr]["value_type"])
        return _possibilities