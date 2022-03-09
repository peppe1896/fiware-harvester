# LOADER da link di github
# Quello che decodifica l'ontologia
# Il modulo che si occupa di scaricare il modello (schema.json) dalla cartella indicata
# Il traduttore del modulo nel db pandas
# Creatore di db (direttamente implementato da pandas)
# Eventuale interfaccia grafica per fare comunicare questi oggetti tra loro
# Main che gestisce questi moduli

###########
# IMPORTS:#
###########
import os
import Harvester as hv

###############
# DEFINITIONS:#
###############
base_link = "https://github.com/smart-data-models/"
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

h = None
if __name__ == "__main__":
    while True:
        if h is None:
            h = hv.Harvester(
                base_link=base_link,
                domains=domains,
                download_folder=download_folder,
                result_folder=result_folder)
        else:
            _input = _input("cmd>\n")
            if _input == "read":
                print(h.query_db("SELECT * FROM raw_schema_model"))
