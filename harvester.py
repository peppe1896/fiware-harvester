import loader
import datetime
import os
import pandas as pd
import json
import db_schema_helper as db_helper
import schema_for_snap4city as s4c

class harvester:

    def __init__(self,
                 base_link="https://github.com/smart-data-models/",
                 domains=None,
                 download_folder="",
                 result_folder=""):

        if domains is None:
            domains = [
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
        self.domains = domains
        if download_folder == "":
            self.download_folder = "/media/giuseppe/Archivio2/Download"   # Where to downaload Repos
        else:
            self.download_folder = download_folder
        self.loader = loader.loader(self.download_folder)
        if result_folder == "":
            self.result_folder = os.path.dirname(__file__) + "/Results/"
        else:
            self.result_folder = result_folder
        os.makedirs(self.result_folder[:-1], exist_ok=True)
        self.db_helper = db_helper.db_schema_helper(self.result_folder)
        self.base_link = base_link
        self.timestamp = datetime.datetime.today()
        self.location_schemas = None
        self.pandas_dataframe = None
        if not self.dict_already_exists():
            self.load_required_files()
            self.load_domain_dict()
            self.save_domain_dict()
        else:
            self.load_created_dict()
        self.schema_reader = s4c.schema_for_s4c(result_folder=self.result_folder)
        self.create_db_from_dict()

    def dict_already_exists(self):
        return os.path.exists(self.result_folder + "schemas_location.json")

    def load_created_dict(self):
        with open(self.result_folder+"schemas_location.json") as file:
            self.location_schemas = json.load(file)

    def create_db_from_dict(self):
        _columns = ["Domain", "Subdomain", "Model", "jsonschema", "time", "version"]
        if not os.path.exists(self.result_folder+"db_schema-pandas.json"):
            self.pandas_dataframe = pd.DataFrame(columns=_columns)
            for domain in self.location_schemas.keys():
                for subdomain in self.location_schemas[domain].keys():
                    for model in self.location_schemas[domain][subdomain].keys():
                        #with open(self.location_schemas[domain][subdomain])
                        _schema_link = self.location_schemas[domain][subdomain][model]
                        with open(_schema_link) as _json_schema:
                            _schema_content = _json_schema.read()

                        self.schema_reader.procedure(_schema_link, domain, subdomain, model)
                        _scalar_attr = self.schema_reader.get_scalar_attribute()
                        _attributes = self.schema_reader.get_attributes()

                        self.db_helper.add_tuple((domain, subdomain, model, _scalar_attr["$schemaVersion"], _schema_content, self.timestamp))

                        _row = {"Domain":[domain],"Subdomain": [subdomain], "Model":[model], "jsonschema":[_schema_content], "time":[self.timestamp], "version":[_scalar_attr["$schemaVersion"]]}
                        _append = pd.DataFrame(_row, columns=_columns)
                        self.pandas_dataframe = pd.concat([self.pandas_dataframe, _append], ignore_index=True)

            with open(self.result_folder+"db_schema-pandas.json", "w") as file:
                _temp = self.pandas_dataframe.to_json(indent=2, force_ascii=False)
                file.write(_temp)
        else:
            for domain in self.location_schemas.keys():
                for subdomain in self.location_schemas[domain].keys():
                    for model in self.location_schemas[domain][subdomain].keys():
                        _schema_link = self.location_schemas[domain][subdomain][model]
                        with open(_schema_link) as _json_schema:
                            _schema_content = _json_schema.read()
                        self.schema_reader.procedure(_schema_link, domain, subdomain, model)
                        _scalar_attr = self.schema_reader.get_scalar_attribute()
                        _attributes = self.schema_reader.get_attributes()
                        _esit, return_msg = self.db_helper.add_tuple((domain, subdomain, model, _schema_content, self.timestamp, _scalar_attr["$schemaVersion"]))

                        if not _esit:
                            print(return_msg)
                            if input("Would you like to continue?") in ["False", "false", "no", "No", "NO", "FALSE"]:
                                return
        print(self.db_helper.read_db())

    def load_required_files(self):
        for domain in self.domains:
            print(f"Loading {domain}")
            self.loader.get_repo(link=self.base_link + domain + ".git", folder_name=domain)
        print("Domains loaded.")

    def load_domain_dict(self):
        main_dict = dict()
        for domain in self.domains:
            self.loader.set_last_folder(domain)
            main_dict[domain] = self.loader.find_schemas()
        self.location_schemas = main_dict

    def get_locations_schema(self):
        return self.location_schemas

    def get_domain_schemas(self, domain):
        return self.location_schemas[domain]

    def delete_local_files(self):
        self.loader.delete_local_data(True)

    def save_domain_dict(self):
        with open(self.result_folder+"schemas_location.json", "w") as file:
            json.dump(self.location_schemas, file, indent=2)


h = harvester()
print("hello")