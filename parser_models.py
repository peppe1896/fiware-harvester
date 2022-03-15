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
            _versions = self.db_helper.get_all_versions(_payload_model)
            if len(_versions) == 1:
                _schema = self.db_helper.get_schema(_payload_model, version=str(_versions[0][0]))
                try:
                    validate(payload, _schema)
                    self.validated_payloads.append(payload)
                except Exception as e:
                    self.thrown_exceptions.append(e)
                    self.unvalidated_payloads.append(payload)
            elif len(_versions) == 0:
                print("No schema found")
            else:
                print("Found more versions. Take newer and go below.")

    def get_results(self):
        return [self.validated_payloads, self.unvalidated_payloads, self.thrown_exceptions]

    def reset_parser(self):
        self.validated_payloads = []
        self.unvalidated_payloads = []
        self.thrown_exceptions = []
        self.raw_payloads = []