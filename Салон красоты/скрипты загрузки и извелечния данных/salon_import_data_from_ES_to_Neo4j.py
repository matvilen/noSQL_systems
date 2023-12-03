from elasticsearch import Elasticsearch
from py2neo import Graph, Node, Relationship

client = Elasticsearch([{"host": "127.0.0.1", "port": 9200}])

indexName1 = " salon_procedure"
indexName2 = " salon_master"

graph_db = Graph("bolt://localhost:7687", auth=("neo4j", "matvienko"))

graph_db.delete_all()

# array of procedure
procedure_array = {
    "size": 1000,
    "query": {
        "match_all": {}
    }
}

# array of specialists
master_array = {
    "size": 1000,
    "query": {
        "match_all": {}
    }
}

result_procedure = client.search(index=indexName1, body=procedure_array)
result_master = client.search(index=indexName2, body=master_array)

# cycle for my_node
for my_node in result_procedure['hits']['hits']:
    Procedure_Node = Node("procedure",
                          Client_id=my_node['_source']['id_of_client'],
                          Master_id=my_node['_source']['id_of_specialist'],
                          Service=my_node['_source']['service'],
                          Cosmetics=my_node['_source']['cosmetics'],
                          Price=my_node['_source']['price'],
                          Date_of_procedure=my_node['_source']['date_of_procedure'])
    try:
        Client_Node = graph_db.nodes.match("Client",
                                           Client_id=my_node['_source']['id_of_client']).first()
        if Client_Node == None:
            # Create node for Client
            Client_Node = Node("Client",
                               Client_id=my_node['_source']['id_of_client'],
                               Client_personal_data=my_node['_source']['client_personal_data'],
                               Client_age=my_node['_source']['client_age'])
            graph_db.create(Client_Node)
    except Exception as e:
        print(e)
    # Create cycle for Master node and Link
    try:
        for master in result_master['hits']['hits']:
            Master_Node = graph_db.nodes.match("Master",
                                               Master_id=Master['_id']).first()
            if Master_Node == None:
                Master_Node = Node("Master",
                                   Master_id=master['_id'],
                                   Specialization=master['_source']['specialisation'],
                                   Work_experience=master['_source']['work_experience'],
                                   Rewiews=master['_source']['rewiews'])
                graph_db.create(Master_Node)

            if master['_id'] == my_node['_source']['Master_id']:
                Link_to_master = Relationship(Procedure_Node,
                                              "connect_master", Master_Node,
                                              Date_begin=my_node['_source']['date_of_procedure'])
                graph_db.create(Link_to_Master)

                Link_to_procedure = Relationship(Client_Node,
                                                 "connect_procedure", Procedure_Node,
                                                 Date_begin=my_node['_source']['date_of_procedure'])
                graph_db.create(Link_to_procedure)

    except Exception as e:
        print(e)
