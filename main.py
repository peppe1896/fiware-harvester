#!/usr/bin/env python

import harvester as hv
import dbschemahelper as db
import payloadsingestor as ingstr
import statics

if __name__ == "__main__":
    # LOAD CONFIG #
    _config = statics.load_config("./config.json")
    base_link = _config["repository_base_link"]
    dictionary_link = _config["s4c_dictionary_link"]
    download_folder = _config["download_folder"]
    result_folder = _config["results_folder"]
    db_config_file = _config["db_config_file"]
    domains = _config["domains"]

    # EXECUTE CONFIG #
    statics.create_folders([download_folder, result_folder])
    dbconfig = statics.load_db_config(db_config_file)
    _hv = None
    _db_helper = None
    _ingestor = None
    _dict_evaluator = None
    while True:
        if _db_helper is None:
            _db_helper = db.DbSchemaHelper(dbconfig)
        if _hv is None:
            _hv = hv.SmartDataModelsHarvester(
                base_link=base_link,
                domains=domains,
                download_folder=download_folder,
                result_folder=result_folder,
                database=_db_helper
            )
        if _ingestor is None:
            _ingestor = ingstr.PayloadsIngestor(_db_helper, result_folder, dictionary_link)
        _input = input("cmd> ")
        if _input == "harvester":
            _overwrite = input("Overwrite data?").lower()
            _wipe = False
            _also_wrongs = True if input("Also wrong models? ").lower() in ["yes", "y"] else False
            if _overwrite in ["yes", "y"]:
                _wipe = True
            _hv.create_db_from_dict(also_wrongs=_also_wrongs, overwrite=_wipe)
        elif _input == "ingestor":
            _cmd = input("From? ")
            if _cmd == "file":
                context_broker = input("Context broker: ")
                multitenancy = True if input("Multitenancy? ").lower() in ["yes", "y"] else False
                service = ""
                servicePath = ""
                if multitenancy:
                    service = input("Service: ('Tampere', for example)")
                    servicePath = input("Service path: ('/' for example)")
                file = statics.ask_open_file("json")
                _analyzed_payloads = _ingestor.open_payloads_file(file.name)
                _ingestor.analize_results(_analyzed_payloads, context_broker, multitenancy, service, servicePath)
            elif _cmd == "url":
                _link = input("Write link:")
                _header = input("Header: \nFor Tampere multitenancy server, {'Fiware-Service': 'Tampere'}")
                context_broker = input("Context broker: ")
                multitenancy = True if input("Multitenancy?").lower() in ["yes", "y"] else False
                service = ""
                servicePath = ""
                if multitenancy:
                    service = _header
                    servicePath = "/"
                _analyzed_payloads = _ingestor.open_link(_link, header=_header)
                _ingestor.analize_results(_analyzed_payloads, context_broker, multitenancy, service, servicePath)
        elif _input == "db":
            _command = input("db> ")
            while _command != "exit":
                if _command == "def_vers":
                    _model = input("Model: ")
                    _sub = input("Subdomain: ")
                    _dom = input("Domain: ")
                    a = _db_helper.get_default_version(_model, subdomain=_sub)
                    b = None
                elif _command == "get_schema":
                    _model = input("Model: ")
                    _sub = input("Subdomain: ")
                    _dom = input("Domain: ")
                    a = _db_helper.get_model_schema(_model, _sub, _dom)
                    b = None
                elif _command == "get_errors":
                    _model = input("Model: ")
                    _sub = input("Subdomain: ")
                    _dom = input("Domain: ")
                    a = _db_helper.get_errors(_model, _sub, _dom)
                    b = None
                elif _command == "get_attrs":
                    _model = input("Model: ")
                    _sub = input("Subdomain: ")
                    _dom = input("Domain: ")
                    a = _db_helper.get_attributes(_model, _sub, _dom, also_attributes_logs=True)
                    b = None
                elif _command == "get_unchecked":
                    _model = input("Model: ")
                    _sub = input("Subdomain: ")
                    _dom = input("Domain: ")
                    _vers = input("Version: ")
                    a = _db_helper.get_unchecked_attrs(_model, _sub, _dom,_vers)
                    b = None
                elif _command == "get_attribute":
                    _attr = input("Attribute name: ")
                    _model = input("Model: ")
                    _sub = input("Subdomain: ")
                    _dom = input("Domain: ")
                    _vers = input("Version: ")
                    a = _db_helper.get_attribute(_attr, _model, _sub, _dom, _vers, "False")
                elif _command == "update_checked":
                    _model = input("Model: ")
                    _sub = input("Subdomain: ")
                    _dom = input("Domain: ")
                    _vers = input("Version: ")
                    _attr_name = input("Attribute name: ")
                    _checked = input("True or False")
                    _db_helper.update_json_attribute(_model, _sub, _dom, _vers, _attr_name, _checked)
                elif _command == "count_attr":
                    _attr = input("Attribute name: ")
                    _group_by = input("Group by.. Write one, or mode followed by a comma, of this: [model, subdomain, domain]")
                    a = _db_helper.count_attributes(_attr, _group_by)
                    b = None
                elif _command == "query":
                    _tables = _db_helper.generic_query("SHOW TABLES;")
                    print("Tables in MySQL database:\n")
                    [print(f"\t{x[0]}\n") for x in _tables]
                    _query = input("Write your query")
                    a = _db_helper.generic_query(_query)
                    b = None
                elif _command == "add_rule":
                    rule = ("a", [{"a":0}], [{"a":1}], "BB", "aaa", "Service", "ServicePath")
                    _db_helper.add_rule(rule, multitenancy=True)
                    a = None
                elif _command == "delete_attr":
                    _attr_name = input("Attribute name: ")
                    _model = input("Model: ")
                    _sub = input("Subdomain: ")
                    _dom = input("Domain: ")
                    _vers = input("Version: ")
                    _db_helper.delete_attribute(_attr_name, _model, _sub, _dom, _vers)
                elif _command == "edit_attribute":
                    _attr_name = input("Attribute name: ")
                    _model = input("Model: ")
                    _sub = input("Subdomain: ")
                    _dom = input("Domain: ")
                    _vers = input("Version: ")
                    _attribute = _db_helper.get_attribute(_attr_name, _model, _sub, _dom, _vers)[0]
                    statics.window_edit_attribute(_attribute[4], _attr_name, f"Edit of attribute '{_attr_name}'",
                                                  _model, _sub, _dom, _vers, _db_helper,
                                                  text="Close this window to end the procedure without saving.\nEditing from 'main.py'")
                    a = None
                _command = input("db> ")
        elif _input == "exit":
            break
