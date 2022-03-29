import similarity_checker

# Avendo un payload validato, procedo a generare il value_type. Se lo trovo, creo una regola per quel dispositivo
class RuleGenerator():
    def __init__(self, database, dict_link):
        self.sim_checker = similarity_checker.SimilarityChecker(database, dict_link)

# Rule is a tuple:
    # Rule name
    # [{field:"", operator:"",value:""}..] - fields : [cb, device, deviceType, value_name, data_type, model, value_type, value_unit ]
    # [{field:"", valueThen:""}...]
    # Organization
    # time
    # mode (1)
    # contextbroker
    # service
    # servicePath
###
    def _gen_if(self, field=None, operator=None, value=None):
        return {"field": field, "operator": operator, "value": value}

    def _gen_then(self, field=None, value=None):
        return {"field": field, "valueThen": value}

    def _check_valid_dict(self, _dict):
        if None in _dict.values():
            return False
        return True

    def create_rule(self, payload:dict, context_brocker, multitenancy, service, servicePath):
        _payload = payload[0][0]
        _metadata = payload[0][1]
        _schema_tuple = payload[1]
        _device = _payload["id"]
        _organization = ""
        _context_broker = context_brocker

        if multitenancy:
            _service = service
            _servicePath = servicePath
        _rules = []
        print(f"Creating rules for device '{_device}'")
        for attribute in _payload.keys():
            _create_rule = False
            value_type = self.sim_checker.fit_value_type(attribute, _schema_tuple)
            if isinstance(value_type, tuple) or isinstance(value_type, str): # Quando trovo un match con id
                _create_rule = True
            elif isinstance(value_type, list):
                a = None # Valutare se si verifica
                if len(value_type) > 0:
                    value_type = value_type.pop(0)
                    print(f"Multiple choises for attribute '{attribute}'. Assumed '{value_type}'.")
                    _create_rule = True
                else:
                    a = None
            elif attribute not in ["type", "id"]:
                print(f"No value_type found for '{attribute}'")

            if _create_rule:
                _rule_name = _device+f"-{attribute}"
                _ifs = []
                _thens = []
                _ifs.append(self._gen_if("cb", "IsEqual", _context_broker))
                _ifs.append(self._gen_if("model", "IsEqual", _payload["type"]))
                _ifs.append(self._gen_if("device", "IsEqual", _payload["id"]))
                _ifs.append(self._gen_if("value_name", "IsEqual", attribute))
                _thens.append(self._gen_then("value_type",
                                             value_type[0] if isinstance(value_type, tuple) else value_type)
                              )
                if attribute in _metadata.keys():
                    if "unit" in _metadata[attribute].keys():
                        # Devo creare una nuova regola che vincola questo unit?
                        # Potrebbe essere non necessario
                        _thens.append(self._gen_then("value_unit", _metadata[attribute]["unit"]))
                _rule = [_rule_name, _ifs, _thens, _organization, _context_broker]
                if multitenancy:
                    _rule.append(service)
                    _rule.append(servicePath)
                _rule.append(_device)
                _rules.append(tuple(_rule))  # Devo creare una regola per ognuno degli attributi.


        return _rules
