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
    _harvest_models = _config["harvest-smartdatamodels"]

    # EXECUTE CONFIG #
    statics.create_folders([download_folder, result_folder])
    dbconfig = statics.load_db_config(db_config_file)
    _db_helper = db.DbSchemaHelper(dbconfig)
    _hv = hv.SmartDataModelsHarvester(
                base_link=base_link,
                domains=domains,
                download_folder=download_folder,
                result_folder=result_folder,
                database=_db_helper
            )
    _ingestor = ingstr.PayloadsIngestor(_db_helper, dictionary_link)

    if _harvest_models:
        _overwrite = _config["overwrite_database_models"]
        _also_wrongs = _config["save_wrong_models"]
        _hv.create_db_from_dict(also_wrongs=_also_wrongs, overwrite=_overwrite)

    _logs = _db_helper.get_external_update()
    while len(_logs) > 0:
        _log = _logs.pop(0)
        _model = _log[0]
        _subdomain = _log[1]
        _domain = _log[2]
        _version = _log[3]
        _attrs = _log[4]
        _attrsLog = _log[5]
        for _attribute in _attrsLog:
            if _attribute not in _attrs.keys():
                _db_helper.create_empty_attribute(_attribute, _model, _subdomain, _domain, _version)
                _new_attr = dict(statics.ATTRIBUTE_MASK)
            a = None

    _brokers = _db_helper.get_brokers()
    while len(_brokers) > 0:
        _broker = _brokers.pop()
        _organization = _broker[0]
        _context_broker = _broker[1]
        _link = _broker[2]
        _multitenancy = _broker[3]
        _ngsi_version = _broker[4]
        _service = _broker[5]
        _service_path = _broker[6]
        _status = _broker[7]
        _header = ""

        if _multitenancy:
            _header = "{'Fiware-Service': '"+_service+"'}"
        if _ngsi_version == "v2":
            _link += "/v2/entities"
            _analyzed_payloads = _ingestor.open_link(_link, header=_header)
            _ingestor.analize_results(_analyzed_payloads, _context_broker, _multitenancy, _service, _service_path)
