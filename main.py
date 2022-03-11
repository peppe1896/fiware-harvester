###########
# IMPORTS #
###########
import os
import Harvester as hv
import Db_schema_helper as db

###############
# DEFINITIONS #
###############

# Da testare fuori da ambiente linux. Potrebbe non funzionare per via del separatore "/" negli uri
base_link = "https://github.com/smart-data-models/"             # Don't change
download_folder = "/media/giuseppe/Archivio2/Download"
result_folder = os.path.dirname(__file__) + "/Results/"
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