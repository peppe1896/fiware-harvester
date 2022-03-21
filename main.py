###########
# IMPORTS #
###########
import harvester as hv
import dbschemahelper as db
from tkinter import filedialog
import payloadsingestor as ingstr
import statics
import os
import dictionary_evaluator as dict_evaluator
###############
# DEFINITIONS #
###############
base_link = "https://github.com/smart-data-models/"             # Don't change
dictionary_link = "https://processloader.snap4city.org/processloader/api/dictionary/"
download_folder = "/media/giuseppe/Archivio2/Download/"#filedialog.askdirectory() + "/"#os.path.dirname(__file__)+ "/Download/"       # Must end with /
result_folder = os.path.dirname(__file__)+ "/Results/" #filedialog.askdirectory() + "/" #os.path.dirname(__file__)+ "/Results/"          # Must end with /
backup_folder = "/media/giuseppe/Archivio2/Download/Backup/"#os.path.dirname(__file__)+ "/Results/Backup/" #filedialog.askdirectory() + "/"
domains = [
    "data-models",
    "SmartCities",
    "SmartAgrifood",
    "SmartWater",
    "SmartEnergy",
    "SmartEnvironment",
    "SmartRobotics",
    "Smart-Sensoring",
    "CrossSector",
    "SmartAeronautics",
    "SmartDestination",
    "SmartHealth",
    "SmartManufacturing"]

########
# MAIN #
########
if __name__ == "__main__":
    statics.create_folders([download_folder, result_folder, backup_folder])
    _hv = None
    _db_helper = None
    _ingestor = None
    _dict_evaluator = None
    while True:
        if _db_helper is None:
            _db_helper = db.DbSchemaHelper(result_folder, backup_folder)
        if _hv is None:
            _hv = hv.SmartDataModelsHarvester(
                base_link=base_link,
                domains=domains,
                download_folder=download_folder,
                result_folder=result_folder,
                database=_db_helper
            )
        if _dict_evaluator is None:
            _dict_evaluator = dict_evaluator.DictEval(dictionary_link, _db_helper)
        if _ingestor is None:
            _ingestor = ingstr.PayloadsIngestor(_db_helper, result_folder, dictionary_link)
        _input = input("cmd> ")
        if _input == "create_db":
            _hv.create_db_from_dict()
        elif _input == "ingestor":
            _cmd = input("From? ")
            if _cmd == "file":
                file = filedialog.askopenfile("r", filetypes=[("Json files","*.json")])
                _Res = _ingestor.open_payloads_file(file.name)
                _ingestor.analize_results(_Res)
            elif _cmd == "url":
                _link = input("Write link:")
                a = _ingestor.open_link(_link, header="{'Fiware-Service': 'Tampere'}")
                _ingestor.analize_results(a)
        elif _input == "db":
            _command = input("cmd> ")
            while _command != "exit":
                if _command == "def_vers":
                    _model = input("Model: ")
                    _sub = input("Subdomain: ")
                    _dom = input("Domain: ")
                    a = _db_helper.get_default_version(_model, subdomain=_sub)
                    b = None
                #a = _db_helper.get_attributes("Building", also_attributes_logs=True)
                elif _command == "get_schema":
                    _model = input("Model: ")
                    _sub = input("Subdomain: ")
                    _dom = input("Domain: ")
                    a = _db_helper.get_schema(_model)
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
                    a = _db_helper.get_unchecked_attrs(_model)#, _sub, _dom,_vers)#, _sub, _dom, also_attributes_logs=True)
                    b = None
                _command = input("cmd> ")
        elif _input == "exit":
            break