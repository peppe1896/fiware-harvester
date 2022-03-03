import json
import os
import shutil
import re
from schema_exceptions import schema_exception

class schema_for_s4c:

    def __init__(self,
                 schema_uri="/home/giuseppe/PycharmProjects/auto_downl_ghub/Results/SmartEnvironment/dataModel.WasteManagement/WasteContainer/schema.json",
                 domain="SmartCities", subdomain="Transportation", model="Road",
                 common_schema_uri="/home/giuseppe/PycharmProjects/auto_downl_ghub/common-schema.json",
                 constraint_schema=["allOf", "anyOf", "oneOf", "not"],
                 result_folder=None
                 ):
        self.base_folder = os.path.dirname(__file__) + "/"
        if result_folder is None:
            self.result_folder = self.base_folder + "Results/"
        else:
            self.result_folder = result_folder
        self.domain = domain
        self.subdomain = subdomain
        self.model = model
        self.type = None                        # Type property (Building, etc)
        self.schema_uri = schema_uri
        self.common_schema_uri = common_schema_uri
        self.raw_schema = None
        self.raw_common_schema = None
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
            "title": "-DEFAULT TITLE-",
            "description": "-DEFAULT DESCRIPTION-",
            "required": "-DEFAULT REQUIREMENTS-",
            "$schema": "-DEFAULT-",
            "$id": "-DEFAULT-",
            "$schemaVersion": "0.0.0",
            "modelTags": ""
          }
        self.constraint_schema = constraint_schema
        self.known_ref = []
        self.external_ref = []
        self.schema_type = "object"              # Normally it's "object"
        self.attributes = {}
        self.schema_details = ""
        self.schema_valid = "VALID"              # append this to the end of the file that save schema_details
        self.properties = None
        self.errors = []
        self.common_definitions = []

        self.procedure(self.schema_uri, domain, subdomain, model)

    # Discard [$schema, $id]
    # Prendo [title, description, required] - attributi scalari
    # type può essere un OBJ, ARRAY, primitivi
    # propierties
    # allOf, anyOf, oneOf, not

    # Procedure istanzia un nuovo modello - Può essere chiamato da fuori contenendo questo oggetto
    def procedure(self, schema_uri, domain, subdomain, model):
        print(f"\nAnalisi di {domain} {subdomain} {model}")
        self.schema_uri = schema_uri
        #if self._exists_schema(self.schema_uri):
        self.type = None
        self.domain = domain
        self.subdomain = subdomain
        self.model = model
        try:
            self._load_schema()
            self._clean_schema()
            self._validate_schema_structure()
            self._control_standardized_schema()
            self._control_properties()
            self._save_details()
        except schema_exception as e:
            self.errors.append(f"- Error. Received this message: {e.get_error()}")
            print(e)
        #else:
        #    print(f"Error: Schema ({self.schema_uri}) not found.")
        #    self.schema_details += f"Error: Schema ({self.schema_uri}) not found.\n"

    # Load schema from a json file containing it
    def _load_schema(self):
        self.schema_details += "AUTOMATIC GENERATED DETAILS\n\n"
        if not self._exists_schema(self.schema_uri):
            self.schema_details += f"Error: {self.schema_uri} doesn't exist! THIS SCHEMA IS INVALID-!\n"
            self.schema_valid = "INVALID-"
            raise schema_exception(f"Error: {self.schema_uri} doesn't exist! THIS SCHEMA IS INVALID-!")
            return

        with open(self.schema_uri) as schema:
            self.raw_schema = json.load(schema)

        with open(self.base_folder + "assets/known_$ref.txt") as known_ref:
            for line in known_ref:
                self.known_ref.append(line[:-1])
                # known_$ref must end with newline
        if self.raw_common_schema is None:
            with open(self.common_schema_uri) as common_schema:
                self.raw_common_schema = json.load(common_schema)
        if len(self.external_ref) == 0:
            self.external_ref.append("type")
            self.load_common_schema()

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
            self.errors.append("- Schema is not an object (so its wrong).")
                 # Delete "type" attribute (this "type" is nomally object, and its not reffered to true "type" in propierties)
        if "definitions" in self.raw_schema.keys():
            self.schema_valid = "INVALID-"
            self.schema_details += "This schema is a definition schema. It contains definitions (collections of objects), " \
                                   " instead of attributes! THIS SCHEMA IS INVALID.\n"
            self.errors.append("- This is a definition schema (not instantiable).")
    # After this execution, raw_schema must contain only the details of this schema.
    def _clean_schema(self):
        _temp, path = self.find_from_schema("properties")
        _constraint = self._check_constraint(path)
        if _constraint is None:
            self.properties = _temp
        else:
            path.remove("properties")
            self.properties = self.find_from_path(path)
        for key in self.scalar_attributes.keys():
            if key not in self.raw_schema.keys():
                if key != "required":
                    self.schema_details += "ATTENTION: the attribute  " + str(key) + " can be wrong. Check it by yourself.\n"
                _temp, _path = self.find_from_schema(key, True)
                if type(_temp) is list:
                    self.scalar_attributes[key] = _temp
                else:
                    self.schema_details += "I haven't found any attribute named " + key + ". Set default value for this.\n"
                    self.schema_valid = "INVALID-"
                    self.scalar_attributes[key] = self.default_attribute[key]
                    self.errors.append("- No requirements found.")

            else:
                _temp, _path = self.find_from_schema(key, True)
                if _temp:
                    self.scalar_attributes[key] = _temp
                else:
                    self.errors.append(f"- Set default value for {key} (in scalar attributes)")
                    self.scalar_attributes[key] = self.default_attribute[key]
        self.schema_type = self.find_from_schema("type", True)[0]

    def get_scalar_attribute(self):
        return self.scalar_attributes

    def _control_properties(self, properties=None):
        if properties is None:
            _properties = self.properties
        else:
            _properties = properties
        if _properties:
            if type(_properties) is dict:
                if "type" in _properties.keys():
                    _type = _properties.pop("type", None)
                    if _type:
                        self.analyze_attribute(_type, "type")
                        if self.type is None:
                            self.type = _type
                        else:
                            self.schema_details += "Error: this schema aready have a type. Please check the schema.json."
                            self.schema_valid = "INVALID-"
                            self.errors.append("- Found two type definition for this model (wrong, only one is expected)")
                    for property in _properties.keys():
                        if type(_properties[property]) is dict:
                            self.schema_details += "\n"
                            if self.analyze_attribute(_properties[property], property)[0]:
                                self.attributes[property] = _properties[property]
                            else:
                                self.schema_details += f"Attention, {property} is not a common attribute. Please check the schema.json\n"
                                self.schema_valid = "INVALID-"
                        else:
                            self.schema_details += f"{property} is not a dict. Please check schema.json.\n"
                            self.schema_valid = "INVALID-"
                            self.errors.append(f"- The attribute {property} isn't a dictionary (expected to be a dictionary)")
                elif "$ref" in _properties.keys():
                    if self._analyze_ref(_properties['$ref']):
                        self.schema_details += f"\tThis attribute is referred by the link: {_properties['$ref']}.\n" \
                                               f"\tCheck this link to get more information about.\n"
                        self.errors.append(f"- {_properties} is a referenced obj. Link: {_properties['$ref']}")
                        # self.attributes[]
                else:
                    self._control_properties(_properties)
                    self.schema_details += "This schema haven't type property, and so it's wrong.\n"
                    self.schema_valid = "INVALID-"
                    self.errors.append("- Type value isn't defined.")
            elif type(_properties) is list:
                for item in _properties:
                    if type(item) is dict:
                        if "$ref" in item.keys():
                            print(item)
                            self._analyze_ref(item["$ref"])
                        elif "properties" in item.keys():
                            print("item[pro]")
                            print(item["properties"])
                            self._control_properties(item["properties"])
                        else:
                            # Controlla che non sia un attributo
                            print("If you are here, we're probably in another property")
                            for _key in item.keys():
                                if "$ref" in item[_key].keys():
                                    if self._analyze_ref(item[_key]["$ref"]):
                                        self.attributes[_key] = item[_key]["$ref"]
                                elif "properties" in item[_key].keys():
                                    self.attributes[_key] = item[_key]
                                    self.analyze_attribute(item[_key], _key)
                            self.schema_details += "->Attribute error: NO $ref AND NO properties. Keys:"+str(item)+"\n"
                else:
                    self.errors.append(f"- {_properties} have un'expected type: ({type(_properties)})")
        else:
            self.errors.append("- This schema haven't properties.")

    def _exists_schema(self, schema_uri):
        if os.path.exists(schema_uri) and not os.path.isdir(schema_uri):
            return True
        return False

    def _calculate_errors(self):
        self._escape_attributes_name()          # verifica i nomi di ogni singolo attributo (fai l'escape)
        self._check_required()                  # verifica che ci siano tutti gli attributi
        self._localization_check()              # scrivi quando manca l'attributo di localizzazione
                                                # (questa operazione viene fatta in check required)

    def _escape_attributes_name(self):
        _temp_attributes = {}
        for _attr_key in self.attributes.keys():
            _escaped_key = re.sub("[><=;()]", "", _attr_key)
            _escaped_key = re.sub('["]', "", _escaped_key)
            _escaped_key = re.sub("[']", "", _escaped_key)
            _temp_attributes[_escaped_key] = self.attributes[_attr_key]
        self.attributes = _temp_attributes

    def _check_required(self):
        _required = self.scalar_attributes["required"]
        for requirement in _required:
            if requirement not in self.external_ref:   # Known referenced
                if requirement not in self.attributes.keys():
                    if requirement not in self.common_definitions:
                        self.errors.append(f"- Missing requirement '{requirement}'")
                        print(f"Missing {requirement}")

    def _localization_check(self):
        return
    #    _location_commons = self.find_key_from_dict("Location-Commons", False, self.raw_common_schema, [])
    #    _defs_location = []
    #    _properties = self.find_key_from_dict("properties", False, _location_commons, [])
    #    for _loc in _properties:
    #        _defs_location.append(_loc)
    #    for _attr in self.attributes.keys():
    #        if _attr in _defs_location:

    def load_common_schema(self):
        _defs = self.find_key_from_dict("definitions", False, self.raw_common_schema, [])
        for _definition in _defs.keys():
            _temp_propr = self.find_key_from_dict("properties", False, _defs[_definition], [])
            if _temp_propr is None:
                self.external_ref.append(_definition)
            else:
                for _property in _temp_propr:
                    self.external_ref.append(_property)
            self.common_definitions.append(_definition)

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
                            return None, temp[last], goal
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

    def _analyze_ref(self, _ref):
        if re.search("common-schema", _ref):
            _ref_list = _ref.rsplit("/")
            _ref_link = []
            _iterator = len(_ref_list) - 1
            _path = []
            if _iterator <= 0:
                print(f"Ref {_ref} is not defined. Please check if it's referred in another extra schema.")
                self.errors.append(f"- Ref {_ref} is not a recognizable reference. Maybe it's defined outside from common-schema.json")
                self.schema_details += f"\t- Ref {_ref} is not a recognizable reference\n"
                return False
            while _iterator > 0:
                _temp = _ref_list[_iterator]
                if not _temp.endswith("#"):
                    _path.append(_temp)
                else:
                    break
                _iterator -= 1
            _path = _path[::-1]
            _temp_pop = _path.pop()
            if _temp_pop in self.common_definitions:
                self.errors.append(f"- {_temp_pop} is defined into common-schema.json")
                self.schema_details += f"-> Ref {_ref} is a DEFINITION in common-schema.json\n"
                return True
            elif _temp_pop in self.external_ref:
                self.errors.append(f"- {_temp_pop} is an attribute of an object in common-schema.json")
                self.schema_details += f"-> Ref {_ref} is an ATTRIBUTE defined in common-schema.json\n"
                return True
            else:
                self.errors.append(f"- {_temp_pop} is a unknown and uncommon reference.")
                self.schema_details += f"-> Ref {_ref} is not defined into common-schema.json\n"
                return False
        self.errors.append(f"- {_ref} not belongs to common-schema.json")
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
                if type(_local_attributes) is dict:
                    if "description" in _local_attributes.keys():
                        self.schema_details += f"\t{attribute_name} contains a key {_eventually_constraint}.\n " \
                                               f"\tDescription: {attribute[_eventually_constraint]['description']}\n" \
                                               f"\tSubfields in {_eventually_constraint}:\n"
                    self.analyze_attribute(_local_attributes, attribute_name+f"[{_eventually_constraint}]")
                    self.schema_details += f"\t->End of {attribute_name}.\n"
                else:
                    for item in _local_attributes:
                        if type(item) is dict:
                            self.analyze_attribute(item, attribute_name+f"[{_eventually_constraint}]")
                        self.schema_details += "\n"
            else:                                                               # Siamo in un attributo normale
                if "type" in attribute.keys():
                    if type(attribute["type"]) is not list:
                        self.schema_details += "-> Attribute: \t" + attribute_name + "\n"
                        _attr_keys = attribute.keys()
                        _attr_type = attribute['type']
                        if "description" in attribute.keys():
                            _attr_descr = attribute['description']
                        else:
                            _attr_descr = "(This attribute haven't a description)."
                        self.schema_details += f"\tType : {_attr_type}\n\tDescription: {_attr_descr}\n"

                        if _attr_type == "array":
                            self._manage_array(attribute, attribute_name)
                        elif _attr_type == "integer":
                            self._manage_integer(attribute, attribute_name)
                        elif _attr_type == "boolean":
                            self._manage_boolean(attribute, attribute_name)
                        elif _attr_type == "string":
                            self._manage_string(attribute, attribute_name)
                        elif _attr_type == "null":
                            self._manage_null(attribute, attribute_name)
                        elif _attr_type == "number":
                            self._manage_numeric(attribute, attribute_name)
                        elif _attr_type == "object":
                            self._manage_object(attribute, attribute_name)

                        self.schema_details += f"\tRaw attribute: \n\t{attribute}\n\tUse a json formatter for read attribute.\n"
                    else:
                        self.errors.append(f"- {attribute_name} can accept more than one type.")
                if "$ref" in attribute.keys():
                    self._analyze_ref(attribute['$ref'])
                    self.schema_details += f"\tThis attribute is referred by the link: {attribute['$ref']}.\n" \
                                           f"\tCheck this link to get more information about.\n"
                    self.errors.append(f"- {attribute_name} is a referenced obj. Link: {attribute['$ref']}")
        return True, attribute

    def _check_constraint(self, _list):
        for item in _list:
            if item in self.constraint_schema:
                return item
        return None

    def _save_details(self):
        _dir = self.result_folder + self.domain + "/dataModel." + self.subdomain + "/" + self.model
        self.schema_details += "\n\n~~~ Error messages ~~~\n\n"
        for error in self.errors:
            self.schema_details += f"\t{error}\n"
        os.makedirs(_dir, exist_ok=True)
        with open(_dir + f"/{self.schema_valid}{self.model}_Analysis.txt", "w") as details:
            details.write(self.schema_details)
        shutil.copyfile(self.schema_uri, f"{_dir}/schema_"+re.sub('\.','-',self.scalar_attributes['$schemaVersion'])+".json")
        self.schema_details = ""
        self._calculate_errors()
        self.errors = []
        self.attributes = {}

    def get_attributes(self):
        return self.attributes

    def _manage_array(self, attribute, attribute_name):
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

    def _manage_numeric(self, attribute, attribute_name):
        _attr_keys = attribute.keys()
        if "minimum" in _attr_keys and "maximum" in _attr_keys:
            _min = attribute["minimum"]
            _max = attribute["maximum"]
            if _min == 0 and _max == 1:
                self.errors.append(f"- Attribute {attribute_name} could be a percentage.")

    def _manage_integer(self, attribute, attribute_name):
        pass

    def _manage_boolean(self, attribute, attribute_name):
        pass

    def _manage_null(self, attribute, attribute_name):
        pass

    def _manage_object(self, attribute, attribute_name):
        self.errors.append(f"- {attribute_name} is an object (it's defined by ITS OWN PROPERTIES)")
        if "properties" in attribute.keys():
            self.analyze_attribute(attribute["properties"], attribute_name + "[properties]")
        elif "$ref" in attribute.keys():
            self.analyze_attribute(attribute["$ref"], attribute_name + "[$ref]")
            print("")

    def _manage_string(self, attribute, attribute_name):
        return

    def get_errors(self):
        return self.errors

#a = schema_for_s4c(schema_uri="/media/giuseppe/Archivio2/Download/Domains/SmartCities/dataModel.Streetlighting/Streetlight/schema.json")

#print(a.raw_schema)
#print(a.schema_scalar_attribute)
#res, path = a.find_from_schema("enum", False)
#res = a.get_type_attribute("refRoadSegment")
#print(res)
#a.find_from_path(path)
#print(a.path_composer(path))
#print(a.print_path(path))
#print(a.find_key_from_attribute("refRoadSegment", "type"))
