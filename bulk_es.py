import csv
import os
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.client import IndicesClient

es = Elasticsearch()
es_index=IndicesClient(es)

def CSVimportES(indexName,typeName,fileName):
    if os.path.isfile(fileName) == False:
        print ("The file dose not exist.")
    if es_index.exists(indexName) == False:
        es.indices.create(
            index=indexName,
            body={'settings': {
                    'number_of_shards': 5,'number_of_replicas': 1,
                    'analysis': {"analyzer":{"default":{"type": "smartcn"}}}},
                  'mappings': {
                    typeName: {
                        "date_detection": false
                    }
                }
                  #'mappings':{"_all":{"ignore_malformed": true}}
            },
            ignore=400)
    datas=[]
    with open('demo_test.csv','r+') as p_file:
        raw_data=csv.DictReader(p_file)
        for item in raw_data:
            datas.append({"_index":indexName,"_type":typeName,"_source":item})
    helpers.bulk(es,datas,chunk_size=100)