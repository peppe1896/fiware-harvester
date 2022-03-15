###########
# IMPORTS #
###########
import harvester as hv
import db_schema_helper as db
from tkinter import filedialog
import ingestor as ingstr
import statics
import os
###############
# DEFINITIONS #
###############
base_link = "https://github.com/smart-data-models/"             # Don't change
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
    while True:
        if _db_helper is None:
            _db_helper = db.Db_schema_helper(result_folder, backup_folder)
        if _hv is None:
            _hv = hv.Harvester(
                base_link=base_link,
                domains=domains,
                download_folder=download_folder,
                result_folder=result_folder,
                database=_db_helper
            )
        if _ingestor is None:
            _ingestor = ingstr.Ingestor(_db_helper, result_folder)
        _input = input("cmd> ")
        if _input == "create_db":
            _hv.create_db_from_dict()
            a = None
        elif _input == "ingestor":
            if input("From file? ") == "yes":
                file = filedialog.askopenfile("r", filetypes=[("Json files","*.json")])
                _Res = _ingestor.open_payloads_file(file.name)
                _ingestor.analize_results(_Res)
            else:
                _link = input("Write link:")
                a = _ingestor.open_link(_link)
                _ingestor.analize_results(a)
        elif _input == "db":
            a = _db_helper.get_attributes("Building", also_attributes_logs=True)
            b = None
        else:
            break