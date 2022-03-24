import similarity_checker

# Avendo un payload validato, procedo a generare il value_type. Se lo trovo, creo una regola per quel dispositivo
class RuleGenerator():
    def __init__(self, database, dict_link):
        self.sim_checker = similarity_checker.SimilarityChecker(database, dict_link)

# Rule is a tuple:
    # Rule name
    # [{field:"", operator:"",value:""}..]
    # [{field:"", valueThen:""}...]
    # Organization
    # time
    # mode (1)
    # contextbroker
    # service
    # servicePath
###
    def create_rule(self, payload:dict):
        _payload = payload[0][0]
        _metadata = payload[0][1]
        _schema_tuple = payload[1]
        for attribute in _payload.keys():
            value_type = self.sim_checker.fit_value_type(attribute, _schema_tuple)
            if isinstance(value_type, list):
                if len(value_type) == 1:
                    # Ho un solo value_type: uso questo
                    a = None
                elif len(value_type) > 1:
                    # Ho un paio di value_type, quindi ne prendo uno solo a scelta dell'utente
                    b = None
                else:
                    # Non ne trovo nessuno, e quindi lo faccio mettere all'utente
                    c = None
                # Set value type inside schema attributes and set checked = True
            else:
                a = None