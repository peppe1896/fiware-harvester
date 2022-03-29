import dictionary_evaluator
import statics
import json

class SimilarityChecker():
    def __init__(self, db_helper, s4c_dict_link):
        self.db_helper = db_helper
        self.s4c_dictionary = dictionary_evaluator.DictEval(db_helper, s4c_dict_link)

    # Vengono analizzati gli attributi del payload con gli attributi dello schema, o degli altri schema.
    # Quando si modifica un attributo, inserisci all'interno della voce "value_type", il value type corrispondente
    # a quello presente nel dizionario di S4C. Se c'è bisogno di aggiungere un nuovo value_type,
    # bisogna aggiungerlo proprio dal sito S4C e non qui.
    # Una volta inserito il value_type corrispondente a quell'attributo, inserire "True" in CHECKED
    def fit_value_type(self, attribute_key, schema_tuple):
        if attribute_key == "type" or attribute_key == "id":
            return None
        print(f"Fit of '{attribute_key}' in model : '{json.dumps(schema_tuple)}'")
        _model = schema_tuple[0]
        _subdomain = schema_tuple[1]
        _domain = schema_tuple[2]
        _version = schema_tuple[3]
        if self.db_helper.attribute_exists(attribute_key, _model, _subdomain, _domain, _version):
            # Questi metodi RICHIEDONO che l'attributo esista nella definizione dello schema.json
            _value_type_1 = self._check_against_attrs_of_schema(attribute_key, _model, _subdomain, _domain, _version)
            if _value_type_1:
                # Qui trovo l'attributo che è checked, quindi il value_type è valido e corrisponde al value type di s4c
                return self.s4c_dictionary.fit_value_type(attribute_key)
            # Se non è assegnato un value_type all'attributo dello schema:
            # cerca definizioni nel dizionario. Se ne trova, le mostra e chiede di impostarne una.
            self._check_against_dict(attribute_key, _model, _subdomain, _domain, _version)
            # Controllo che non sia stato inserito un value_type valido nella procedura sopra
            _value_type_1 = self._check_against_attrs_of_schema(attribute_key, _model, _subdomain, _domain, _version)
            if _value_type_1:
                # Qui trovo l'attributo che è checked, quindi il value_type è valido e corrisponde al value type di s4c
                return self.s4c_dictionary.fit_value_type(attribute_key)
            self._check_against_attrs_structure(attribute_key, "True", schema_tuple)
            _value_type_1 = self._check_against_attrs_of_schema(attribute_key, _model, _subdomain, _domain, _version)
            if _value_type_1:
                return self.s4c_dictionary.fit_value_type(attribute_key)
            # Attributo nuovo
            self.edit_attribute(attribute_key, _model, _subdomain, _domain, _version)
            _value_type_1 = self._check_against_attrs_of_schema(attribute_key, _model, _subdomain, _domain, _version)
            if _value_type_1:
                return self.s4c_dictionary.fit_value_type(attribute_key)
        else:
            _value_type_2 = self._check_in_common_schemas(attribute_key, schema_tuple)
            if _value_type_2:
                return _value_type_2
        # Se non trovo un attributo con quel nome in quel modello, è possibile che sia un attributo di un common-schema
        # oppure una mancanza dovuta a versioni vecchie
        # if len(_value_type_3) > 0 and self.db_helper.attribute_exists(attribute_key, _model, _subdomain, _domain, _version):
        #     _attrs = json.dumps(_value_type_3)
        #     _attr = self.db_helper.get_attribute(attribute_key, _model, _subdomain, _domain, _version)[0]
        #     statics.window_edit_attribute(
        #         _attr,
        #         attribute_key, f"I couldn't find any attribute for '{attribute_key}'. Would you like to add it now?",
        #         _model, _subdomain, _domain, _version, self.db_helper,
        #         text=f'Found following match in dictionary of S4C:\n{_attrs}. Write into "value_type" any of this value type,'
        #              f' and write "True" in "checked".')
        #_value_type_4 = self._check_against_attrs_structure(attribute_key, "", schema_tuple) # Vuol dire che trova tutti, checked false e true
        #_value_types.extend(_value_type_4 if _value_type_4 else ["-"])

        #if input("Do you want to create a new attribute?\n ").lower() in ["yes", "y"]:
        #statics.window_edit_attribute(
        #        statics.ATTRIBUTE_MASK,
        #        attribute_key, f"I couldn't find any attribute for '{attribute_key}'. Would you like to add it now?",
        #                         _model, _subdomain, _domain, _version, self.db_helper,
        #                        f"\nIf you're here, the attribute is not inside. Would you like to create a new attribute with '{attribute_key}' name?")

        # Sono qui quando non ho impostato nessun value type per quel modello
        _value_type_1 = self._check_against_attrs_of_schema(attribute_key, _model, _subdomain, _domain, _version)
        if _value_type_1:
            return self.s4c_dictionary.fit_value_type(attribute_key)
        return None


    # MAIN CHECK - Se questo è valido, allora ho già il value type
    # Controlla che nello schema previsto dal payload, l'attributo sia checked e quindi con un value_type definito
    # nella stessa voce dell'attributo
    def _check_against_attrs_of_schema(self, attribute, model, subdomain, domain, version):
        #_schema_attrs = self.db_helper.get_attributes(model, subdomain, domain, onlyChecked="True", also_attributes_logs=False, excludeType=True)
        _attr = self.db_helper.get_attribute(attribute, model, subdomain, domain, version)
        if _attr:
            if _attr[0][4]["checked"] == "True":
                val_type = _attr[0][4]["value_type"]
                print(f"Attribute '{attribute}' is checked. Expect to have '{val_type}' inside Snap4City dictionary")
                _temp = self.s4c_dictionary.fit_value_type(val_type, silent=True)
                if isinstance(_temp, str):
                    return _temp
                else:
                    print(f"Mismatch: value_type inside attribute is wrong. Value_type: '{val_type}'. Assumed as value_type: '{val_type}'")
                    return val_type
        return None

    def _check_in_common_schemas(self, attribute_key, schema_tuple):
        _common_attr = self.db_helper.get_attribute(attribute_key, domain="definition-schemas")
        _model = schema_tuple[0]
        _subdomain = schema_tuple[1]
        _domain = schema_tuple[2]
        _version = schema_tuple[3]
        if len(_common_attr) == 0:
            print(f"No attribute found with name '{attribute_key}'. Control the schema...")
            _schema = self.db_helper.get_model_schema(_model, _subdomain, _domain, _version)
            statics.window_read_json(_schema,
                                     f"Schema tuple: {str(schema_tuple)}\nAttention! No attribute found with name '{attribute_key}'",
                                     title=f"No attribute found with name '{attribute_key}'. Control the schema...")
            statics.window_edit_attribute(statics.ATTRIBUTE_MASK, attribute_key,
                                          f"Procedure to add a new attribute into schema attributes.",
                                          _model, _subdomain, _domain, _version, self.db_helper,
                                          f"This attribute is not defined anywhere.\nCreate a new attribute and save it to add an attribute named '{attribute_key}' into "
                                          f"attributes \nof the model '{_model}', in subdomain '{_subdomain}', domain '{_domain}' and version '{_version}'.")
        elif len(_common_attr) == 1:
            _cm_attr = _common_attr[0]
            _model = _cm_attr[0]
            _subdomain = _cm_attr[1]
            _domain = _cm_attr[2]
            _version = _cm_attr[3]
            print(f"Found '{attribute_key}' in model: '{_model}', '{_subdomain}', '{_domain}', '{_version}'")
            if _cm_attr[4]["checked"] == "False":
                print(f"This attribute '{attribute_key}' is a common attribute.")
                statics.window_edit_attribute(_cm_attr[4], attribute_key, f"Common attribute '{attribute_key}'",
                                              model=_cm_attr[0], subdomain=_cm_attr[1], domain=_cm_attr[2],
                                              version=_cm_attr[3],
                                              db=self.db_helper,
                                              text=f"\nUpdate attribute: set checked as \"True\", set a value_type, edit other fields, and then click update to save changes"
                                                   f"\nAttention: you got here by working on model '{schema_tuple[0]}', subdomain '{schema_tuple[1]}'"
                                                   f", domain '{schema_tuple[2]}', version '{schema_tuple[3]}', so pay attention on value type!")
                if self.db_helper.attribute_is_checked(attribute_key, _cm_attr[0], _cm_attr[1], _cm_attr[2], _cm_attr[3]):
                    return _cm_attr[4]["value_type"]
            elif _cm_attr[4]["checked"] == "True":
                print(f"Assumed for '{attribute_key}' value_type '{_cm_attr[4]['value_type']}'")
                return _cm_attr[4]["value_type"]

        else:
            # Ho più attributi comuni con questo nome. Quale intendi?
            print(f"Found some attributes with name '{attribute_key}' inside the common attributes. "
                  f"Edit any of them you need to update, and click on Update.")
            _possibilities = []
            for common in _common_attr:
                _model = common[0]
                _subdomain = common[1]
                _domain = common[2]
                _version = common[3]
                if not self.db_helper.attribute_is_checked(attribute_key, common[0], common[1], common[2], common[3]):
                    statics.window_edit_attribute(common[4], attribute_key, f"Common_attribute '{attribute_key}'",
                                              model=common[0], subdomain=common[1],
                                              domain=common[2], version=common[3],
                                              db=self.db_helper,
                                              text="Found some attributes with this name inside common attributes"
                                                   "\nUpdate attribute: set checked as \"True\", set a value_type, edit other fields if you need to, and click update to save changes"
                                                   f"\nAttention: you got here by working on model '{schema_tuple[0]}', subdomain '{schema_tuple[1]}', "
                                                   f"domain '{schema_tuple[2]}', version '{schema_tuple[3]}', so pay attention on value type!")
                if self.db_helper.attribute_is_checked(attribute_key, common[0], common[1], common[2], common[3]):
                    _possibilities.append((common[4]["value_type"], common[0]))
            if len(_possibilities) > 1 :
                old_poss = None
                _equals = True          # Se sono uguali, non ha senso controllare.
                for poss in _possibilities:
                    if old_poss is None:
                        old_poss = poss[0]
                    elif old_poss != poss[0]:
                        _equals = False
                        break
                if _equals:
                    print(f"Attribute '{attribute_key}': returning value_type '{old_poss}'")
                    return old_poss
                _attrs = json.dumps(_possibilities)
                print(f"Attribute '{attribute_key}': found some definition for this attribute. Choose one of this inside"
                      f"value_type field of this attribute!")

                # Quando sono in questo punto:
                # - NON ho un attributo attribute_key tra gli attributi conosciuti degli schema
                # - Ho una o più scelte possibili per questo value_type a causa dell'ambiguità del nome
                # Quindi ne DEVO scegliere uno.
                # Apro l'editor, creo un nuovo attributo che si chiama attribute_key,
                # e metto come value_type uno di quelli trovati (quelli in possibilities, che
                # per costruzione sono tutti Checked True)
                #_attr = self.db_helper.get_attribute(attribute_key, _model, _subdomain, _domain, _version)[0]
                _attr = dict(statics.ATTRIBUTE_MASK)
                _attr["value_name"] = attribute_key
                statics.window_edit_attribute(
                                     _attr,
                                     attribute_key, f"Ambiguous definition for '{attribute_key}'",
                                     _model, _subdomain, _domain, _version, self.db_helper,
                                     text=f'There is some possible definition of "{attribute_key}". Set as "value_type" one of the followig '
                                          f' match (value_type|model):\n{_attrs}.\n'
                                          f' Write "True" in "checked".')
            elif len(_possibilities) == 1:
                return _possibilities[0]

    # Controlla che nel dizionario s4c esista un attributo con quel nome
    # Se sono qui, avrò una lista di candidati ai value_type.
    # Chiedo se si vuole inserire uno di quelli
    def _check_against_dict(self, attribute_key, model, subdomain, domain, version):
        _pool = self.s4c_dictionary.fit_value_type(attribute_key)
        if len(_pool) > 0:
            # pool contiene tutti i candidati trovati da fit_value_type, e gli id
            _attrs = json.dumps(_pool)
            _attribute = self.db_helper.get_attribute(attribute_key, model, subdomain, domain, version)
            statics.window_edit_attribute(
                _attribute[4],
                attribute_key, f"Update '{attribute_key}'",
                model, subdomain, domain, version, self.db_helper,
                text=f'Found following match in dictionary of S4C (name|id):\n{_attrs}\nWrite into "value_type" any of this value type,'
                     f' and write "True" in "checked".')

    # Controllo se tra tutti gli attributi con checked=True di TUTTI gli schema, ho una struttura simile a quella
    # dell'attributo passato in ingresso
    def _check_against_attrs_structure(self, attribute_key, onlyChecked, schema_tuple):
        print(f"Attribute '{attribute_key}': try to find some attribute with same keys.")
        _all_attributes_checked = self.db_helper.get_attributes(onlyChecked=onlyChecked, excludeType=True)
        _model = schema_tuple[0]
        _subdomain = schema_tuple[1]
        _domain = schema_tuple[2]
        _version = schema_tuple[3]
        _attribute = self.db_helper.get_attribute(attribute_key, _model, _subdomain, _domain, _version)
        _possibilities = []
        if len(_attribute) > 0:
            # NON HO TROVATO L'ATTRIBUTO tra quelli previsti dal modello. E' un attributo common?
            _attribute = _attribute[0][4]
            _raw_attribute = _attribute["raw_attribute"]
            if isinstance(_raw_attribute, dict):
                _raw_attr_keys = _raw_attribute.keys()
            else:
                return _possibilities
            if len(_raw_attr_keys) > 2: # Voglio almeno 3 keys nella struttura dell'attributo
                while len(_all_attributes_checked) > 0:
                    _tuple = _all_attributes_checked.pop(0)
                    for _other_attr in _tuple[0]:
                        _obj = _tuple[0][_other_attr]["raw_attribute"]
                        if isinstance(_obj, str):
                            _obj = json.loads(_obj)     # alcune volte trovo raw_attribute ancora string.
                        if isinstance(_obj, dict):
                            if len(_obj.keys()) > 2:
                                if statics.json_is_equals(_obj.keys(), _raw_attr_keys):
                                    if _obj["type"] == _raw_attribute["type"]:
                                        _possibilities.append((_tuple[0][_other_attr]["value_type"], _tuple[1]))
                        elif isinstance(_obj, list):
                            for item in _obj:
                                if len(item.keys()) > 2:
                                    if statics.json_is_equals(item.keys(), _raw_attr_keys):
                                        if item["type"] == _raw_attribute["type"]:
                                            _possibilities.append((_tuple[0][_other_attr]["value_type"], _tuple[1]))
        if len(_possibilities) > 0 and len(_attribute) > 0:
            if len(_possibilities) == 1:
                print(f"Attribute '{attribute_key}': found just one value_type for this attribute. Value_type: '{_possibilities[0]}' (value_type|model)")
                return _possibilities[0]
            print(f"Attribute '{attribute_key}': found some simil attribute")
            _attrs = json.dumps(_possibilities)
            statics.window_edit_attribute(
                _attribute,
                attribute_key, f"Update '{attribute_key}'",
                _model, _subdomain, _domain, _version, self.db_helper,
                text=f'Found following match in checked attributes of ALL schemas (value_type|model):\n{_attrs}\nWrite into "value_type" any of this value type,'
                     f' and write "True" in "checked"')

    def edit_attribute(self, attribute_key, _model, _subdomain, _domain, _version):
        _attribute = self.db_helper.get_attribute(attribute_key, _model, _subdomain, _domain, _version)
        statics.window_edit_attribute(
            _attribute[0][4],
            attribute_key, f"Update '{attribute_key}'",
            _model, _subdomain, _domain, _version, self.db_helper,
            text=f'Set a "value_type" and set checked "True"')
