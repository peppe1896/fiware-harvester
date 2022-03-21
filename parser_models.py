from jsonschema import validate
import statics


# Set a list of payloads by using set_payloads, or add one per one by using append_payload
class Parser():
    def __init__(self, db_helper):
        self.raw_payloads = []
        self.validated_payloads = []
        self.unvalidated_payloads = []
        self.db_helper = db_helper
        self.thrown_exceptions = []

    def _correct_payload(self, payload):
        if statics.is_normalized(payload):
            return payload
        else:
            res = statics.normalized_2_keyvalue(payload)
            return res[0]

    def append_payload(self, payload: dict):
        _temp = self._correct_payload(payload)
        self.raw_payloads.append(_temp)

    def set_payloads(self, payloads: list, also_execute=True):
        self.reset_parser()
        _temp = []
        for payload in payloads:
            _temp.append(self._correct_payload(payload))
        self.raw_payloads = _temp
        if also_execute:
            self.execute_parsing()

    def execute_parsing(self):
        for payload in self.raw_payloads:
            _payload_model = payload["type"]
            _vers_subd_dom = self.db_helper.get_all_versions(_payload_model)
            if len(_vers_subd_dom) == 1:
                _schema = self.db_helper.get_schema(_payload_model)
                try:
                    validate(payload, _schema)
                    self.validated_payloads.append(payload)
                except Exception as e:
                    self.thrown_exceptions.append(e)
                    self.unvalidated_payloads.append((payload, None))
                continue  # Vado al prossimo payload
            elif len(_vers_subd_dom) > 1:
                # Caso in cui ci sono più schema
                _iterator = 0
                _validated = []
                _unval_errors = {"unvalidated": [], "errors": []}
                while _iterator < len(_vers_subd_dom):
                    _tuple = _vers_subd_dom[_iterator]  # VERSION - SUBDOMAIN - DOMAIN
                    _iterator += 1
                    _schema = self.db_helper.get_schema(_payload_model, subdomain=_tuple[1], domain=_tuple[2], version=_tuple[0])
                    try:
                        validate(payload, _schema)
                        _validated.append([_payload_model, _tuple[1], _tuple[2], _tuple[0]])
                    except Exception as e:
                        _unval_errors["unvalidated"].append([_payload_model, _tuple[1], _tuple[2], _tuple[0]])
                        _unval_errors["errors"].append(e)
                if len(_validated) == 1: # Solo uno schema ha validato - C'era ambiguità su modello, ma è stata risolta
                    # Ho bisogno di salvare per quali motivi non è stato validato il payload.
                    self.validated_payloads.append(payload)
                elif len(_validated) == 0:  # Nessuno degli schema ha validato.
                    self.unvalidated_payloads.append((payload, _unval_errors))
                else: # Più schema hanno validato payload. Sono uguali?
                    _temp_schema = None
                    for _tuple in _validated:
                        _schema = self.db_helper.get_schema(_payload_model, subdomain=_tuple[1], domain=_tuple[2], version=_tuple[0])
                        if _temp_schema is None:
                            _temp_schema = _schema
                        else:
                            if not statics.json_is_equals(_schema, _temp_schema):
                                print("Payload validable against more schemas..")

            else:
                print("")
    def get_results(self):
        return [self.validated_payloads, self.unvalidated_payloads, self.thrown_exceptions]

    def reset_parser(self):
        self.validated_payloads = []
        self.unvalidated_payloads = []
        self.thrown_exceptions = []
        self.raw_payloads = []
