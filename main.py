###########
# IMPORTS #
###########
import os
import Harvester as hv
import Db_schema_helper as db

###############
# DEFINITIONS #
###############
base_link = "https://github.com/smart-data-models/"             # Don't change
download_folder = os.path.dirname(__file__)+ "/Download/"       # Must end with /
result_folder = os.path.dirname(__file__)+ "/Results/"          # Must end with /
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
    h = None
    db_helper = None
    os.makedirs(download_folder, exist_ok=True)
    os.makedirs(result_folder, exist_ok=True)
    while True:
        if db_helper is None:
            db_helper = db.Db_schema_helper(result_folder)
        if h is None:
            h = hv.Harvester(
                base_link=base_link,
                domains=domains,
                download_folder=download_folder,
                result_folder=result_folder,
                database=db_helper
            )

        _input = input("cmd>\n")
        if _input == "create_db":
            h.create_db_from_dict()
        else:
            break