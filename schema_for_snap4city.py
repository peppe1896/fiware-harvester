import json
import os
import shutil

class schema_for_s4c:

    def __init__(self,
                 schema_uri="/home/giuseppe/PycharmProjects/auto_downl_ghub/Results/SmartEnvironment/dataModel.WasteManagement/WasteContainer/schema.json",
                 domain="SmartCities", subdomain="Transportation", model="Road",
                 attribute_to_clean=["$schema", "$id", "$schemaVersion", "modelTags"],
                 constraint_schema=["allOf", "anyOf", "oneOf", "not"]):

        self.base_folder = os.path.dirname(__file__) + "/"
        self.domain = domain
        self.subdomain = subdomain
        self.model = model
        self.type = None                # Type property (Building, etc)
        self.schema_uri = schema_uri
        self.raw_schema = None
        self.attribute_to_clean = attribute_to_clean
        self.scalar_attributes = {
            "title": "",
            "description": "",
            "required": "",
            "$schema": "",
            "$id": "",
            "$schemaVersion": "",
            "modelTags": ""
        }
        self.default_attribute = {
            "title": "-DEFAULT-",
            "description": "-DEFAULT DESCRIPTION-",
            "required": "-DEFAULT REQUIREMENTS-",
            "$schema": "-DEFAULT-",
            "$id": "-DEFAULT-",
            "$schemaVersion": "0.0.0",
            "modelTags": ""
          }
        self.constraint_schema = constraint_schema
        self.known_ref = []
        self.schema_type = "object"              # Normally it's "object"
        self.attributes = {}
        self.schema_details = ""
        self.schema_valid = "VALID"              # append this to the end of the file that save schema_details
        self.properties = None
        self.errors = []

        self.procedure(self.schema_uri, domain, subdomain, model)

    # Discard [$schema, $id]
    # Prendo [title, description, required] - attributi scalari
    # type può essere un OBJ, ARRAY, primitivi
    # propierties
    # allOf, anyOf, oneOf, not

    # Procedure istanzia un nuovo modello - Può essere chiamato da fuori contenendo questo oggetto
    def procedure(self, schema_uri, domain, subdomain, model):
        self.schema_uri = schema_uri
        if self._exists_schema(self.schema_uri):
            self.domain = domain
            self.subdomain = subdomain
            self.model = model
            self._load_schema()
            self._clean_schema()
            self._validate_schema_structure()
            self._control_standardized_schema()
            self._control_properties()
            self._save_details()
        else:
            print("Error. Schema not found.")
            self.schema_details += f"Error: Schema ({self.schema_uri}) not found.\n"

    # Load schema from a json file containing it
    def _load_schema(self):
        self.schema_details += "AUTOMATIC GENERATED DETAILS\n\n"
        if not os.path.exists(self.schema_uri):
            self.schema_details += f"Error: {self.schema_uri} doesn't exist! THIS SCHEMA IS INVALID-!\n"
            self.schema_valid = "INVALID-"
            return

        with open(self.schema_uri) as schema:
            self.raw_schema = json.load(schema)

        with open(self.base_folder + "assets/known_$ref.txt") as known_ref:
            for line in known_ref:
                self.known_ref.append(line[:-1])
                # known_$ref must end with newline

    def _validate_schema_structure(self):
        if self.properties is None:
            # print(f"Schema {self.schema_uri} have the following keys: {self.raw_schema.keys()}")
            self.schema_details += "This schema is non common.\n"
            self.schema_details += "Remaining items in this schema: " + str(self.raw_schema.keys()) + "\n"
            self.schema_details += "Was expected to have only one item of [allOf, anyOf, properties, oneOf]\n"
            print(self.schema_uri + " has no attribute properties.")
            self.schema_valid = "INVALID-"

    # Check te schema to define if its a common schema or not
    def _control_standardized_schema(self):
        if self.schema_type != "object" :
            self.schema_details += f"Attention: this schema is not OBJECT. It's {self.schema_type} . THIS SCHEMA IS INVALID-\n"
            self.schema_valid = "INVALID-"
                 # Delete "type" attribute (this "type" is nomally object, and its not reffered to true "type" in propierties)
        if "definitions" in self.raw_schema.keys():
            self.schema_valid = "INVALID-"
            self.schema_details += "This schema is a definition schema. It contains definitions (collections of objects), " \
                                   " instead of attributes! THIS SCHEMA IS INVALID.\n"

    # After this execution, raw_schema must contain only the details of this schema.
    def _clean_schema(self):
        self.properties, path = self.find_from_schema("properties", True)
        for key in self.scalar_attributes.keys():
            if key not in self.raw_schema.keys():
                if key != "required":
                    self.schema_details += "ATTENTION: the attribute  " + str(key) + " can be wrong. Check it by yourself.\n"
                _temp, _path = self.find_from_schema(key, True)
                if _temp:
                    self.scalar_attributes[key] = _temp
                else:
                    self.schema_details += "I haven't found any attribute named " + key + ". Set default value for this.\n"
                    self.schema_valid = "INVALID-"
                    self.scalar_attributes[key] = self.default_attribute[key]

            else:
                _temp, _path = self.find_from_schema(key, True)
                if _temp:
                    self.scalar_attributes[key] = _temp
                else:
                    self.scalar_attributes[key] = self.default_attribute[key]
        self.schema_type = self.find_from_schema("type", True)[0]

    def get_scalar_attribute(self):
        return self.scalar_attributes

    def _control_properties(self):
        if self.properties:
            if "type" in self.properties.keys():
                _type = self.properties.pop("type")
                if _type:
                    self.analyze_attribute(_type, "type")
                for property in self.properties.keys():
                    if type(self.properties[property]) is dict:
                        if self.analyze_attribute(self.properties[property], property)[0]:
                            self.attributes[property] = self.properties[property]
                        else:
                            self.schema_details += f"Attention, {property} is not a common attribute. Please check the schema.json\n"
                            self.schema_valid = "INVALID-"
                    else:
                        self.schema_details += f"{property} is not a dict. Please check schema.json.\n"
                        self.schema_valid = "INVALID-"
                if self.type is None:
                    self.type = _type
                else:
                    self.schema_details += "Error: this schema aready have a type. Please check the schema.json."
                    self.schema_valid = "INVALID-"
            else:
                self.schema_details += "This schema haven't type property, and so it's wrong.\n"
                self.schema_valid = "INVALID-"

    def _exists_schema(self, schema_uri):
        if os.path.exists(schema_uri) and not os.path.isdir(schema_uri):
            return True
        return False

    def _calculate_errors(self):
        # verifica i nomi di ogni singolo attributo
        # verifica che ci siano tutti gli attributi
        # scrivi quando manca l'attributo di localizzazione
        return


    # Return FIRST value corresponding to input key  || None . You can also delete the key->value by using delete True
    def find_from_schema(self, key, delete=False):
        path = []
        if type(self.raw_schema) is dict:
            result = self.find_key_from_dict(key, delete, self.raw_schema, path)
            path = path[::-1]
            return result, path
        else:
            return None, None

    # Attention: it'll find only the first attribute with that name
    # please control if the path is correct, by printing the path
    def find_from_attribute(self, attribute, target_key):
        attribute, path = self.find_from_schema(key=attribute, delete=False)
        result = self.find_key_from_dict(target_key, False, attribute, path)
        return result, path

    def find_from_path(self, path:list):
        if len(path) == 0:
            print("ERROR: Path have size 0!")
            return None
        result, full_result, goal = self._path_composer(path)

        if result is None:
            if type(full_result) is dict:
                if goal in full_result.keys():
                    return full_result[goal]        # Single key in an attribute
                else:
                    return full_result              # Attribute selected
            elif type(full_result) is list:
                if goal in full_result:
                    return goal
                else:
                    return full_result
        else:
            return result

    # Expect dictionary
    def find_key_from_dict(self, target_key, delete, entry_dict, path):
        for _key in entry_dict.keys():
            if _key == target_key:
                path.append(_key)
                return_temp = entry_dict[target_key]
                if delete:
                    entry_dict.pop(target_key, None)
                return return_temp
            else:
                temp = entry_dict[_key]
                if type(temp) is list:
                    for item in entry_dict[_key]:
                        if type(item) is dict:
                            maybe_append = _key
                            temp_res = self.find_key_from_dict(target_key, delete, item, path)
                            if temp_res is not None:
                                path.append(maybe_append)
                                return temp_res
                elif type(temp) is dict:
                    temp_res = self.find_key_from_dict(target_key, delete, temp, path)
                    if temp_res is not None:
                        path.append(_key)
                        return temp_res

    # Given a path, return the corresponding value to that path
    # ATTENTION: It seems working, but maybe some problem can be seen on multiple array values
    # But, i think, a schema.json can't have some arrays concatenated without using a dictionary
    # Normally, it have to end with a dict, and in that dict you have to find the goal (last item of the path)
    # so, i need more test, but for the only one i tried, it worked
    def _path_composer(self, path):
        _path = path[::-1]
        goal = _path[0]
        temp = self.raw_schema
        while len(_path) > 0:
            last = _path.pop()
            if type(temp[last]) is list:
                for item in temp[last]:
                    if type(item) is dict:
                        if len(_path) > 0:
                            new_index = _path.pop()
                        else:
                            return None, temp[last]
                        if new_index in item.keys():
                            temp = item[new_index]

                        else:
                            _path.append(new_index)
                #_path.append(new_index)
            elif type(temp[last]) is dict:
                temp = temp[last]

        if type(temp) is dict:
            if goal in temp.keys():
                return temp[goal], temp, goal
            else:
                return None, temp, goal
        elif type(temp) is list:
            return None, temp, goal
        else:
            return None, temp, goal

    def print_path(self, path:list):
        temp = ""
        last = path.pop()
        for level in path:
            temp = temp + str(level) + "->"
        temp += last
        print(temp)
        return temp

    def get_type_attribute(self, attribute):
        _attribute, path = self.find_from_schema(attribute, False)
        if type(_attribute) is dict:
            if "type" in _attribute.keys():
                if type(_attribute["type"]) is dict:
                    print("Error: you're asking for the type of schema, NOT that one of an attribute")
                    return None
                else:
                    return _attribute["type"]
        else:
            print("Error: this attribute haven't a type. Maybe it's reffered to an external schema. Print result:")
            print(_attribute)
            return None

    def add_attribute(self, attribute_raw):
        print(attribute_raw)
        return

    def _jumpable_ref(self, _ref):
        if _ref in self.known_ref:
            return True
        return False

    def analyze_attribute(self, attribute, attribute_name):
        if attribute_name == "type":
            self.schema_details += f"-> Devices of type ~ {self.model} ~\n"
            if "enum" in attribute.keys():
                if len(attribute["enum"]) == 1:
                    self.schema_details += f"This device can be only {attribute['enum']}.\n"
                else:
                    self.schema_details += "This is a non common schema, because 'enum' in type isn't of lenght = 1.\n"
                    self.schema_details += str(attribute["enum"]) + "\n"
            else:
                self.schema_details += "No enum found. This can be wrong."
            return True, attribute  # Close the procedure for model type identification
        else:
            # Check for constraint ([allOf...])
            _eventually_constraint = self._check_constraint(attribute.keys())   # None se non è un attributo con un constraint
            if _eventually_constraint is not None:
                _local_attributes = attribute[_eventually_constraint]
                self.analyze_attribute(_local_attributes, attribute_name+f"[{_eventually_constraint}]")
            else:                                                               # Siamo in un attributo normale
                if "type" in attribute.keys():
                    self.schema_details += "-> Attribute: \t" + attribute_name + "\n"
                    _attr_keys = attribute.keys()
                    _attr_type = attribute['type']
                    if "description" in attribute.keys():
                        _attr_descr = attribute['description']
                    else:
                        _attr_descr = ""
                        self.schema_details += "\tThis attribute haven't a description.\n"
                    self.schema_details += f"\tType : {_attr_type}\n\tDescription: {_attr_descr}"
                    if _attr_type == "array":
                        self._manage_array(attribute)
                    elif _attr_type == "integer":
                        pass
                    elif _attr_type == "object":
                        pass
                    elif _attr_type == "boolean":
                        pass
                    elif _attr_type == "null":
                        pass
                    elif _attr_type == "number":
                        pass
                    elif _attr_type == "object":
                        print(f"\t{attribute_name} is an object.\n")
                        if "properties" in attribute.keys():
                            print("... and this obj have te following properties:")
                            print(attribute["properties"])
                        else:
                            print(self.schema_uri + " haven't properties in " + attribute_name)

                    self.schema_details += f"\tRaw attribute: {attribute}\n\tUse a json formatter for read attribute.\n"
        return True, attribute

    def _check_constraint(self, _list):
        for item in _list:
            if item in self.constraint_schema:
                return item
        return None

    def _save_details(self):
        dir = self.base_folder + "Results/" + self.domain + "/dataModel." + self.subdomain + "/" + self.model
        os.makedirs(dir, exist_ok=True)
        with open(dir + f"/{self.schema_valid}{self.model}_Analysis.txt", "w") as details:
            details.write(self.schema_details)
        shutil.copyfile(self.schema_uri, dir+f"/schema.json")
        self.schema_details = ""
        self._calculate_errors()

    def get_attributes(self):
        return self.attributes

    def _manage_array(self, attribute):
        if "enum" in attribute.keys():
            self.schema_details += "\tEnum found. This attribute have to be one of the following:\n\t["
            last_enum = attribute["enum"].pop()
            if len(attribute["enum"]) > 0:
                for enum in attribute["enum"]:
                    self.schema_details += '"' + str(enum) + '", '
            self.schema_details += '"' + str(last_enum) + '"]\n'
        if "items" in attribute.keys():
            if type(attribute["items"]) is dict:
                if "type" in attribute["items"].keys():
                    self.schema_details += f"\tItems have to be of type {attribute['items']['type']}\n"
                elif "$ref" in attribute["items"].keys():
                    self.schema_details += "\tThis array can contain types referred in this link: "
                    if attribute["items"]["$ref"] in self.known_ref:
                        self.schema_details += f"{attribute['items']['$ref']} (This ref is a known one).\n"
                    else:
                        self.schema_details += f"{attribute['items']['$ref']} (Unknown ref).\n"
                else:
                    self.schema_details += "\tThis item is unknown. It's wrong.\n"
                    self.schema_valid = "INVALID-"
            else:
                self.schema_details += "\tRead the items value to understand what's inside.\n"
        elif "prefixItems" in attribute.keys():
            self.schema_details += "\tThis array have a prefixItems: this means that this array have to be\n" \
                                   "done by concatenating values in this way (this can also be considered TUPLE):"
            for temp in attribute["prefixItems"]:
                self.schema_details += "[ " + str(temp)
            self.schema_details += " ]"
        else:
            self.schema_details += "\tNo items found for this array - Error.\n"

a = schema_for_s4c()
print(a.get_attributes())

#print(a.raw_schema)
#print(a.schema_scalar_attribute)
#res, path = a.find_from_schema("enum", False)
#res = a.get_type_attribute("refRoadSegment")
#print(res)
#a.find_from_path(path)
#print(a.path_composer(path))
#print(a.print_path(path))
#print(a.find_key_from_attribute("refRoadSegment", "type"))
