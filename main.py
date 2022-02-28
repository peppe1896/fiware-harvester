# LOADER da link di github
# Quello che decodifica l'ontologia
# Il modulo che si occupa di scaricare il modello (schema.json) dalla cartella indicata
# Il traduttore del modulo nel db pandas
# Creatore di db (direttamente implementato da pandas)
# Eventuale interfaccia grafica per fare comunicare questi oggetti tra loro
# Main che gestisce questi moduli
import loader as ld
import os
#import functions as func
import json
#repository_name = "SmartCities"
github_repo_link = "https://github.com/smart-data-models/SmartDestination.git"


if __name__ == "__main__":
    loader = ld.loader()
    domain = "SmartAeronautics"
    #ghub.get_tools()
    loader.get_repo(link=github_repo_link, folder_name=domain)
    #loader.delete_local_data()
    schemas_dict = loader.find_schemas()
    #print(schemas_dict)
    for subdomain in schemas_dict:
        for model in schemas_dict[subdomain]:
            os.makedirs("./Results/"+domain+"/dataModel."+subdomain+"/"+model, exist_ok=True)
            with open("./Results/dataModel."+subdomain+"/"+model+"/"+model+".json","w") as new_model:
                #snap4city_model = func.calculate_model(schemas_dict[subdomain][model])
                #print(snap4city_model)
                break
                #new_model.write(json.dump(func.calculate_model(schemas_dict[subdomain][model])))


