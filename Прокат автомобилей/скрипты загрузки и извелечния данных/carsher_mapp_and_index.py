import json
from elasticsearch import Elasticsearch

client = Elasticsearch([{"host": "127.0.0.1", "port": 9200}])
index_1= "arendator"
index_2 = "car"

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

ArendatorMapping = {
    "properties":{
	"arendator_id": {
            "type": "text",
            "fielddata": True
        },
	"arendator_data": {
            "type": "text",        
            "analyzer":"analitic_for_ru",
	    "search_analyzer":"analitic_for_ru",
            "fielddata": True
        },
	"arenda_id":  {
            "type": "text",
            "fielddata": True
        },
	"date_of_arenda":  {
            "type":   "date",
            "format": "yyyy-MM-dd"
        },
	"numb_days_of_arend": {
            "type": "integer"
        },
	"price": {
            "type": "integer"
        },
	"car_id": {
            "type": "text",
            "fielddata": True
        }
    }
} 


client.indices.put_mapping(index=index_1,
                           doc_type="Arendator",
                           include_type_name="true",
                           body=ArendatorMapping)


client.indices.open(index="arendator")

with open("/home/hadoopuser/js_files/Arendator.json", 'r') as file_Arendator:
    Arendator_data = json.load(file_Arendator)

for data in Arendator_data:
    try:
        client.index(index=data["index"],
                     doc_type=data["doc_type"],
                     id=data["id"],
                     body=data["body"]
                     )
    except Exception as e:
        print(e)
print("Arendator_indexed")


client.indices.open(index=index_2) 

CarMapping = {
    "properties":{
	"car_data": {
            "type": "text",        
            "analyzer":"analitic_for_ru",
	    "search_analyzer":"analitic_for_ru",
            "fielddata": True
	"diagnostic_card":{
            "type": "text",        
            "analyzer":"analitic_for_ru",
	    "search_analyzer":"analitic_for_ru",
            "fielddata": True
        },
	"car_reviews":{
            "type": "text",        
            "analyzer":"analitic_for_ru",
	    "search_analyzer":"analitic_for_ru",
            "fielddata": True
        }
    }
}  

client.indices.put_mapping(index=index_2,
                           doc_type="Car",
                           include_type_name="true",
                           body=CarMapping)

client.indices.open(index="car")

with open("/home/hadoopuser/js_files/Car.json", 'r') as file_Car:
    data_Car = json.load(file_Car)

for data in data_Car:
    try:
        client.index(index=data["index"],
                     doc_type=data["doc_type"],
                     id=data["id"],
                     body=data["body"]
                     )
    except Exception as e:
        print(e)
print("Car_indexed")
