from elasticsearch import Elasticsearch
from py2neo import Graph, Node, Relationship

client = Elasticsearch([{"host": "127.0.0.1", "port": 9200}])

indexName1 = "procedure"
indexName2 = "patient"

graph_db = Graph("bolt://localhost:7687", auth=("neo4j", "anton"))

graph_db.delete_all()



#array of procedure
procedure_array = {
    "size": 1000,
    "query": {
        "match_all": {}
  }
}


#array of clients
patient_array = {
  "size": 1000,
  "query": {
    "match_all": {}
  }
}


result_procedure = client.search(index = indexName1, body = procedure_array)
result_patient = client.search(index = indexName2, body = patient_array)

# cycle for my_node
for my_node in result_patient ['hits']['hits']:
	Patient_Node = Node("patient",
	Patient_id=my_node['_source']['patient_id'],
	Patient_personal_data=my_node['_source']['personal_data'],
	Voucher=my_node['_source']['voucher'],
	Arrival_date=my_node['_source']['date_of_arrival'],
	Numb_of_days=my_node['_source']['number_of_days'],
	Diagnosis=my_node['_source']['diagnosis'],
	Procedures_id=my_node['_source']['precedures_id'])
	graph_db.create(Patient_Node)
	try:
           for procedure in result_procedure ['hits']['hits']:
               Procedure_Node = graph_db.nodes.match("Procedure", Procedure_id=procedure['_id']).first()
               if Procedure_Node == None:
                       Procedure_Node = Node("Procedure", Procedure_id=procedure['_id'],
                         Name = procedure['_source']['name'],
                         Description=procedure['_source']['description'],
                         Price = procedure['_source']['price'])
                       graph_db.create(Procedure_Node)
    # Link
               if procedure['_id'] in  my_node['_source']['precedures_id']:
                	Got_procedure = Relationship( Patient_Node,
                	"Got_procedure", Procedure_Node,
                	Procedure_Price=procedure['_source']['price'])
                	graph_db.create(Got_procedure)

	except Exception as e:
            print(e)
