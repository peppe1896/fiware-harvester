import json
import os
import shutil

class schema_to_s4c:

    def __init__(self,
                 schema_uri="/home/giuseppe/PycharmProjects/auto_downl_ghub/SmartCities/dataModel.Transportation/Road/schema.json",
                 domain="SmartCities", subdomain="Transportation", model="Road",
                 attribute_to_clean=["$schema", "$id", "$schemaVersion", "modelTags"], constraint_schema=["allOf", "anyOf", "oneOf", "not"]):
        self.base = os.path.dirname(__file__) + "/"
        self.domain = domain
        self.subdomain = subdomain
        self.model = model
        self.type = None                # Type property
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
            "modelTags": ""}
        self.constraint_schema = constraint_schema
        self.known_ref = []
        self.schema_type = "object"
        self.definition_schema = False
        self.attributes = {}
        self.schema_details = ""
        self.schema_valid = "VALID"              # append this to the end of the file that save schema_details

        self._load_schema()
        self._control_standardized_schema()
        self._clean_schema()
        self._validate_schema_structure()
        self._save_details()

    # Discard [$schema, $id]
    # Prendo [title, description, required] - attributi scalari
    # type può essere un OBJ, ARRAY, primitivi
    # propierties
    # allOf, anyOf, oneOf, not

    # Load schema from json file containing it
    def _load_schema(self):
        self.schema_details += "AUTOMATIC GENERATED DETAILS\n\n"
        if not os.path.exists(self.schema_uri):
            self.schema_details += f"Error: {self.schema_uri} doesn't exist! THIS SCHEMA IS INVALID!\n"
            self.schema_valid = "- INVALID"
            return

        with open(self.schema_uri) as schema:
            self.raw_schema = json.load(schema)

        with open(self.base + "assets/known_$ref.txt") as known_ref:
            for line in known_ref:
                self.known_ref.append(line[:-1])
                # known_$ref must end with newline

    # Check te schema to define if its a common schema or not
    def _control_standardized_schema(self):
        if self.raw_schema["type"] != "object" :
            self.schema_details += f"Attention: this schema is not OBJECT. It's {self.raw_schema['type']} . THIS SCHEMA IS INVALID\n"
            self.schema_valid = "- INVALID"
            self.schema_type = self.raw_schema["type"]
        self.find_from_schema("type", True)         # Delete "type" attribute (this "type" is nomally object, and its not reffered to true "type" in propierties)
        if "definitions" in self.raw_schema.keys():
            self.schema_valid = "- INVALID"
            self.schema_details = "This schema is a definition schema. It contains definitions, instead of attributes! THIS SCHEMA IS INVALID\n"
            self.definition_schema = True

    # After this execution, raw_schema must contain only the details of this schema.
    def _clean_schema(self):
        for key in self.scalar_attributes.keys():
            if key not in self.raw_schema.keys():
                if key != "required":
                    self.schema_details += "ATTENTION: the attribute  " + str(key) + " can be wrong. Check it by yourself.\n"
                self.scalar_attributes[key], path = self.find_from_schema(key, True)
            else:
                self.scalar_attributes[key], path = self.find_from_schema(key, True)

    def _validate_schema_structure(self):
        if len(self.raw_schema.keys()) == 1:
            print("OK, VALID SCHEMA")
            # print(self.raw_schema)

        properties, path = self.find_from_schema("properties")
        if "type" in properties.keys():
            _type = properties.pop("type")
            if _type:
                self.analize_attribute(_type, "type")
            for property in properties.keys():
                if type(properties[property]) is dict:
                    if self.analize_attribute(properties[property], property)[0]:
                        self.attributes[property] = properties[property]
                    else:
                        print("This attribute is not standard")
                else:
                    print("Attribute is not a dict:")
                    print(properties[property])
            self.type = _type



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

    def analize_attribute(self, attribute, attribute_name):
        if "type" in attribute.keys():
            self.schema_details += "\n"
            if attribute_name == "type":
                self.schema_details += f"This schema gives back an object of type {self.model}\n"
            else:
                self.schema_details += "Attribute: \t" + attribute_name + "\n"
            if "enum" in attribute.keys():
                self.schema_details += "Enum found. This attribute have to be one of the following:\n\t["
                last_enum = attribute["enum"].pop()
                if len(attribute["enum"]) > 0:
                    for enum in attribute["enum"]:
                        self.schema_details += '"'+str(enum)+ '", '
                self.schema_details += '"' + str(last_enum) + '"]\n'

            self.schema_details += "\n"
        return True, attribute

    def _save_details(self):
        dir = self.base + "Results/"+self.domain+"/dataModel." + self.subdomain + "/" + self.model
        os.makedirs(dir, exist_ok=True)
        with open(dir + f"/{self.model}-Analisis.txt", "w") as details:
            details.write(self.schema_details)



a = schema_to_s4c()

#print(a.raw_schema)
#print(a.schema_scalar_attribute)
#res, path = a.find_from_schema("enum", False)
#res = a.get_type_attribute("refRoadSegment")
#print(res)
#a.find_from_path(path)
#print(a.path_composer(path))
#print(a.print_path(path))
#print(a.find_key_from_attribute("refRoadSegment", "type"))

