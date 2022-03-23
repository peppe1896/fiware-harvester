import dictionary_evaluator

class SimilarityChecker():
    def __init__(self, s4c_dict_link, db_helper):
        self.db_helper = db_helper
        self.s4c_dictionary = dictionary_evaluator.DictEval(s4c_dict_link, db_helper)

    def check(self, attribute:dict, model, subdomain, domain, version):
        _attr_keys = attribute.keys()

    # MAIN CHECK - Se questo è valido, allora ho già il value type
    # Controlla che nello schema previsto dal payload, l'attributo sia checked e quindi con un value_type definito
    # nella stessa voce dell'attributo
    def _check_against_attrs_of_schema(self, attribute):

        return

    # Controlla che nel dizionario s4c esista un attributo con quel nome
    def _check_against_dict(self, attribute):
        return

    # Controllo se tra tutti gli attributi con checked=True di TUTTI gli schema, ho una struttura simile a quella
    # dell'attributo passato in ingresso
    def _check_against_attrs_structure(self, attribute):
        return