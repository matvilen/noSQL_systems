from elasticsearch import Elasticsearch
from py2neo import Graph, Node, Relationship

client = Elasticsearch([{"host": "127.0.0.1", "port": 9200}])

indexName1 = "arendator"
indexName2 = "car"

graph_db = Graph("bolt://localhost:7687", auth=("neo4j", "sergey"))

graph_db.delete_all()



#array of arendator
arendator_array = {
    "size": 1000,
    "query": {
        "match_all": {}
  }
}


#array of cars
car_array = {
  "size": 1000,
  "query": {
    "match_all": {}
  }
}


result_arendator = client.search(index = indexName1, body = arendator_array)
result_car = client.search(index = indexName2, body = car_array)

# cycle for my_node
for my_node in result_arendator ['hits']['hits']:
    Arendator_Node = Node("arendator",
    Arendator_id = my_node['_source']['arendator_id'],
	Arendator_data = my_node['_source']['arendator_data'])
    graph_db.create(Arendator_Node)
    try:
        for car in result_car ['hits']['hits']:
            Car_Node = graph_db.nodes.match("Car", Car_id = car['_id']).first()
            if Car_Node == None:
                Car_Node = Node("Car", Car_id = car['_id'],
                Car_data = car['_source']['car_data'],
                Diagnostic_card = car['_source']['diagnostic_card'],
                Car_reviews = car['_source']['car_reviews'])
                graph_db.create(Car_Node)
    # Link
               if car['_id'] == my_node['_source']['car_id']:
                   Lent_car = Relationship( Arendator_Node, "Lent_car", Car_Node,
                    Arenda_id = my_node['_source']['arenda_id'],
	        	    Date_of_arenda = my_node['_source']['date_of_arenda'],
	        	    Numb_of_days = my_node['_source']['numb_days_of_arend'],
	        	    Price = my_node['_source']['price'])
                    graph_db.create(Lent_car)

    except Exception as e:(print(e))