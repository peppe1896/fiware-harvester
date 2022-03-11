import Loader as ld
import datetime
import os
import pandas as pd
import json
import Db_schema_helper as db_helper
import Schema_interpreter as s4c

class Harvester:

    def __init__(self,
                 base_link="https://github.com/smart-data-models/",
                 domains=None,
                 download_folder="",
                 result_folder="",
                 database = None):

        if domains is None:
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
        self.domains = domains
        if download_folder == "":
            self.download_folder = "/media/giuseppe/Archivio2/Download"   # Where to downaload Repos
        else:
            self.download_folder = download_folder
        self.loader = ld.Loader(self.download_folder)
        if result_folder == "":
            self.result_folder = os.path.dirname(__file__) + "/Results/"
        else:
            self.result_folder = result_folder
        os.makedirs(self.result_folder[:-1], exist_ok=True)
        self.blacklist_schemas = ["geometry-schema.json", "schema.org.json"]
        if database is None:
            self.db_helper = db_helper.Db_schema_helper(self.result_folder)
        else:
            self.db_helper = database
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
        self.schema_reader = s4c.Schema_interpreter(result_folder=self.result_folder)

    def dict_already_exists(self):
        return os.path.exists(self.result_folder + "schemas_location.json")

    def load_created_dict(self):
        with open(self.result_folder+"schemas_location.json") as file:
            self.location_schemas = json.load(file)

    def _clean_location_schema(self):
        for _definition_schema in self.location_schemas["0"]["0"]:
            _schema = self.location_schemas["0"]["0"][_definition_schema]
            self.schema_reader.procedure(_schema, None, None, None)
        for _schema in self.location_schemas["data-models"]["common-schemas"]:
            _name = os.path.basename(_schema)#_schema.rsplit("/")[-1]
            if _name not in self.blacklist_schemas:
                self.schema_reader.procedure(_schema, None, None, None)
        _keys_to_delete = ["data-models", "0"]
        for _key in _keys_to_delete:
            self.location_schemas.pop(_key)

    def create_db_from_dict(self, create_pandas=False, also_wrongs=False):
        self._clean_location_schema()
        if create_pandas:
            self._create_pandas()
        else:
            for domain in self.location_schemas.keys():
                for subdomain in self.location_schemas[domain].keys():
                    for model in self.location_schemas[domain][subdomain].keys():
                        _schema_link = self.location_schemas[domain][subdomain][model]
                        with open(_schema_link, encoding="utf8") as _json_schema:
                            _schema_content = _json_schema.read()
                        self.schema_reader.procedure(_schema_link, domain, subdomain, model)
                        if model not in self.schema_reader.get_wrongs() or also_wrongs:
                            _scalar_attr = self.schema_reader.get_scalar_attribute()
                            _attributes = str(self.schema_reader.get_attributes())
                            _errors = str(self.schema_reader.get_errors())
                            _attr_log = str(self.schema_reader.get_attributes_log())
                            _esit, return_msg = self.db_helper.add_tuple((domain, subdomain, model,
                                                                          _scalar_attr["$schemaVersion"], _attributes,
                                                                          _errors, _attr_log, _schema_content, self.timestamp))
                            if not _esit:
                                print(return_msg)
                                if input("Would you like to continue?") in ["False", "false", "no", "No", "NO", "FALSE"]:
                                    return

    def _create_pandas(self):
        _columns = ["Domain", "Subdomain", "Model", "jsonschema", "time", "version"]
        if not os.path.exists(self.result_folder+"db_schema-pandas.json"):
            self.pandas_dataframe = pd.DataFrame(columns=_columns)
            for domain in self.location_schemas.keys():
                for subdomain in self.location_schemas[domain].keys():
                    for model in self.location_schemas[domain][subdomain].keys():
                        _schema_link = self.location_schemas[domain][subdomain][model]
                        with open(_schema_link) as _json_schema:
                            _schema_content = _json_schema.read()

                        self.schema_reader.procedure(_schema_link, domain, subdomain, model)
                        _scalar_attr = self.schema_reader.get_scalar_attribute()
                        _attributes = self.schema_reader.get_attributes()
                        _errors = self.schema_reader.get_errors()
                        _attr_log = str(self.schema_reader.get_attributes_log())
                        self.db_helper.add_tuple((domain, subdomain, model,
                                                  _scalar_attr["$schemaVersion"], _attributes,
                                                  _errors, _attr_log, _schema_content, self.timestamp))

                        _row = {"Domain":[domain],"Subdomain": [subdomain], "Model":[model], "jsonschema":[_schema_content], "time":[self.timestamp], "version":[_scalar_attr["$schemaVersion"]]}
                        _append = pd.DataFrame(_row, columns=_columns)
                        self.pandas_dataframe = pd.concat([self.pandas_dataframe, _append], ignore_index=True)
            with open(self.result_folder+"db_schema-pandas.json", "w") as file:
                _temp = self.pandas_dataframe.to_json(indent=2, force_ascii=False)
                file.write(_temp)

    def load_required_files(self):
        for domain in self.domains:
            print(f"Loading {domain}")
            self.loader.get_repo(link=self.base_link + domain + ".git", folder_name=domain)
        print("Domains loaded.")

    def load_domain_dict(self):
        main_dict = dict()
        main_dict["0"] = {}
        main_dict["0"]["0"] = None
        for domain in self.domains:
            self.loader.set_last_folder(domain)
            main_dict[domain] = self.loader.find_schemas()
        main_dict["0"]["0"] = self.loader.get_definition_schemas()
        self.location_schemas = main_dict

    def get_locations_schema(self):
        return self.location_schemas

    def get_domain_schemas(self, domain):
        return self.location_schemas[domain]

    def delete_local_files(self):
        self.loader.delete_local_data(True)

    def save_domain_dict(self):
        _keys_to_clean = ["AllSubjects", "ontologies_files"]
        with open(self.result_folder+"schemas_location.json", "w") as file:
            json.dump(self.location_schemas, file, indent=2)
