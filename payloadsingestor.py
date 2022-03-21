import parser_models as pm
import json
import requests
import statics
import ast


class PayloadsIngestor():
    def __init__(self, database, result_folder, dict_eval):
        self.database = database
        self.model_parser = pm.Parser(database)
        self.payloads_list = []  # List of payloads kept
        self.results_folder = result_folder
        self.dictionary_eval = dict_eval

    # curl -H "Fiware-Service:Tampere" https://context.tampere.fiware.cityvision.cloud:443/v2/entities
    def open_link(self, link: str, header=""):
        if header:
            _h = ast.literal_eval(header)
            r = requests.get(link, headers=_h)
        else:
            r = requests.get(link)
        r = r.json()
        if len(r) > 0:
            self.payloads_list.append(r)
            self.model_parser.set_payloads(r, True)
            _triple = self.model_parser.get_results()
            return _triple
        return [[], [], []]

    def open_via_api(self, command, api_uri, header=""):
        if not header:
            with open(api_uri) as api:
                _temp = ""
                for row in api:
                    if row.startswith(command):
                        _temp = row.replace(command + ": ", "")
                        break
                if _temp != "":
                    return self.open_link(_temp)
                else:
                    return [[], [], []]

    def open_payloads_file(self, payloads_file: str):
        _list = []
        with open(payloads_file, encoding="utf8") as file:
            a = json.load(file)
            if type(a) is list:
                _list = a
        if len(_list) > 0:
            self.payloads_list.append(_list)
            self.model_parser.set_payloads(_list, True)
        _triple = self.model_parser.get_results()
        return _triple

    def analize_results(self, triple):
        _correct_payloads = triple[0]
        _uncorrect_payloads = triple[1]
        _error_thrown = triple[2]
        _messages = "Payload Analysis results\n"
        _iterator = len(_uncorrect_payloads) - 1
        while _iterator >= 0:
            _err = _error_thrown[_iterator]
            _messages += "ID: " + _err.instance["id"] + "\tMessage: '"
            _messages += _err.message + "'\n"
            _iterator -= 1
        statics.create_folders([self.results_folder + "Payload-Ingestor/"])
        with open(self.results_folder + "Payload-Ingestor/results.txt", "w", encoding="utf8") as results:
            results.write(_messages)

    def clean_payloads(self):
        self.payloads_list = []
