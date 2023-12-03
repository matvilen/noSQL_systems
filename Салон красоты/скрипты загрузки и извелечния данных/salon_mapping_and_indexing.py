import json
from elasticsearch import Elasticsearch

client = Elasticsearch([{"host": "127.0.0.1", "port": 9200}])
index_1= "salon_procedure"
index_2 = "salon_master"

#client.indices.delete(index=index_1)
if client.indices.exists(index=index_1):
    print("Recreate " + index_1 + " index")
    client.indices.delete(index=index_1)
    client.indices.create(index=index_1)
else:
    print("Create " + index_1)
    client.indices.create(index=index_1)

if client.indices.exists(index=index_2):
    print("Recreate " + index_2 + " index")
    client.indices.delete(index=index_2)
    client.indices.create(index=index_2)
else:
    print("Create " + index_2)
    client.indices.create(index=index_2)


Salon_Settings = {
    "analysis" : {
        "filter": {
            "russian_stop_words": {
                "type": "stop",
                "stopwords": "_russian_"
            },
            "filter_ru_sn": {
                "type": "snowball",
                "language":"Russian"
            }
        },
        "analyzer":
        {
            "analitic_for_ru":
            {
                "type": "custom",
                "tokenizer": "standard",
                "filter": [
                    "lowercase",
                    "russian_stop_words",
                    "filter_ru_sn"
                ]
            }
        }
    }
}

client.indices.close(index=index_1)
client.indices.put_settings(index=index_1, body=Salon_Settings)
client.indices.close(index=index_2)
client.indices.put_settings(index=index_2, body=Salon_Settings)

client.indices.open(index=index_1)         

ProcedureMapping = {
    "properties":{
        "id_of_client": {
            "type": "text",
            "fielddata": True
        },
        "client_age": {
            "type": "integer"
        }, 
        "client_personal_data":{
            "type": "text",        
            "analyzer":"analitic_for_ru",
            "fielddata": True
        },
        "id_of_procedure": {
            "type": "text",
            "fielddata": True
        },
        "date_of_procedure": {
            "type":   "date",
            "format": "yyyy-MM-dd"
        },
        "price": {
            "type": "integer"
        },
        "id_of_specialist": {
            "type": "text",
            "fielddata": True
        },
        "service": {
            "type": "text",
            "fielddata": True,
            "analyzer":"analitic_for_ru",
            "search_analyzer":"analitic_for_ru"
        },
        "cosmetics": {
            "type": "text",            
            "analyzer":"analitic_for_ru",
            "fielddata": True,
        }
    }
}


client.indices.put_mapping(index=index_1,
                           doc_type="Procedure",
                           include_type_name="true",
                           body=ProcedureMapping)


client.indices.open(index="salon_procedure")

with open("/home/hadoopuser/js_files/Procedure.json", 'r') as file_Procedure:
    Procedure_data = json.load(file_Procedure)

for data in Procedure_data:
    try:
        client.index(index=data["index"],
                     doc_type=data["doc_type"],
                     id=data["id"],
                     body=data["body"]
                     )
    except Exception as e:
        print(e)
print("Procedure_indexed")


client.indices.open(index=index_2) 

MasterMapping = {
    "properties":{
        "specialisation": {
            "type": "text",
            "fielddata": True,        
            "analyzer":"analitic_for_ru",
            "search_analyzer":"analitic_for_ru"
            
        }, 
        "work_experience": {
            "type": "integer"
        }, 
        "master_personal_data": {
            "type": "text", 
            "fielddata": True,       
            "analyzer":"analitic_for_ru",
            "search_analyzer":"analitic_for_ru"
            
        }, 
        "schedule": {
            "type": "text",
            "fielddata": True,
            "analyzer":"analitic_for_ru",
            "search_analyzer":"analitic_for_ru"
        },
        "reviews": {
            "type": "text",
            "fielddata": True,
            "analyzer":"analitic_for_ru",
            "search_analyzer":"analitic_for_ru"
        }
    }
}  

client.indices.put_mapping(index=index_2,
                           doc_type="Master",
                           include_type_name="true",
                           body=MasterMapping)

client.indices.open(index="salon_master")

with open("/home/hadoopuser/js_files/Master.json", 'r') as file_Master:
    data_Master = json.load(file_Master)

for data in data_Master:
    try:
        client.index(index=data["index"],
                     doc_type=data["doc_type"],
                     id=data["id"],
                     body=data["body"]
                     )
    except Exception as e:
        print(e)
print("Master_indexed")
