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
            return statics.normalized_2_keyvalue(payload)

    def append_payload(self, payload:dict):
        _temp = self._correct_payload(payload)
        self.raw_payloads.append(_temp)

    def set_payloads(self, payloads:list, also_execute=True):
        self.reset_parser()
        _temp = []
        for payload in payloads:
            _temp.append(self._correct_payload(payload))
        self.raw_payloads = _temp
        if also_execute:
            self.execute()

    def execute(self):
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
                    self.unvalidated_payloads.append(payload)
                continue  # Vado al prossimo payload
            elif len(_vers_subd_dom)>1:
                _def_vers = self.db_helper.get_default_version(_payload_model)
                _count = 0
                for item in _vers_subd_dom:
                    if item[0] == _def_vers:
                        _count += 1
                if _count == 1:
                    # Allora ho una sola versione che, quindi è univoca la chiave model - default_version
                    _schema = self.db_helper.get_schema(_payload_model, version=tuple[0])
                    try:
                        validate(payload, _schema)
                        self.validated_payloads.append(payload)
                    except Exception as e:
                        self.thrown_exceptions.append(e)
                        self.unvalidated_payloads.append(payload)
                    continue    # Vado al prossimo payload
            else:
                _iterator = 0
                while _iterator < len(_vers_subd_dom):
                    tuple = _vers_subd_dom[_iterator]       # VERSION - SUBDOMAIN - DOMAIN
                    _schema = self.db_helper.get_schema(_payload_model, subdomain=tuple[1], domain=tuple[2], version=tuple[0])
                    try:
                        validate(payload, _schema)
                        self.validated_payloads.append(payload)
                    except Exception as e:
                        self.thrown_exceptions.append(e)
                        self.unvalidated_payloads.append(payload)

    def get_results(self):
        return [self.validated_payloads, self.unvalidated_payloads, self.thrown_exceptions]

    def reset_parser(self):
        self.validated_payloads = []
        self.unvalidated_payloads = []
        self.thrown_exceptions = []
        self.raw_payloads = []