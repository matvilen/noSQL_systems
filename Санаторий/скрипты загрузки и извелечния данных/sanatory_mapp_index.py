import json
from elasticsearch import Elasticsearch

client = Elasticsearch([{"host": "127.0.0.1", "port": 9200}])
index_1= "procedure"
index_2 = "patient"

client.indices.delete(index=index_1)
client.indices.delete(index=index_2)
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


Procedure_Settings = {
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
client.indices.put_settings(index=index_1, body=Procedure_Settings)
client.indices.close(index=index_2)
client.indices.put_settings(index=index_2, body=Procedure_Settings)

client.indices.open(index=index_1)

ProcedureMapping = {
    "properties":{
	"name": {
		"type": "text",
		"fielddata": True,
		"analyzer":"analitic_for_ru",
            	"search_analyzer":"analitic_for_ru" 
	},
	"description": {
		"type": "text",
		"fielddata": True,
		"analyzer":"analitic_for_ru",
            	"search_analyzer":"analitic_for_ru"
	},
	"price": {
		"type": "integer"
	}
    }
}


client.indices.put_mapping(index=index_1,
                           doc_type="Procedure",
                           include_type_name="true",
                           body=ProcedureMapping)


client.indices.open(index="procedure")

with open("Procedure.json", 'r') as file_Procedure:
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

PatientMapping = {
    "properties":{
	"patient_id": {
            "type": "text",
            "fielddata": True
        },
	"personal_data": {
            "type": "text",
            "analyzer":"analitic_for_ru",
	    "search_analyzer":"analitic_for_ru",
            "fielddata": True
        },
	"voucher":  {
            "type": "text",
            "fielddata": True
        },
	"date_of_arrival":  {
            "type":   "date",
            "format": "yyyy-MM-dd"
        },
	"number_of_days": {
            "type": "integer"
        },
	"diagnosis":{
            "type": "text",
            "analyzer":"analitic_for_ru",
	    "search_analyzer":"analitic_for_ru",
            "fielddata": True
        },
	"precedures_id": {
            "type": "text",
            "fielddata": True
        }
    }
}

client.indices.put_mapping(index=index_2,
                           doc_type="Patient",
                           include_type_name="true",
                           body=PatientMapping)

client.indices.open(index="patient")

with open("Patient.json", 'r') as file_Client:
    data_Client = json.load(file_Client)

for data in data_Client:
    try:
        client.index(index=data["index"],
                     doc_type=data["doc_type"],
                     id=data["id"],
                     body=data["body"]
                     )
    except Exception as e:
        print(e)
print("Patient_indexed")
