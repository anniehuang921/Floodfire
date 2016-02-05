# coding: utf-8
import pandas as pd
import csv
import os
import datetime 
from elasticsearch import Elasticsearch
from elasticsearch import helpers
es = Elasticsearch()
es.indices.delete(index="platform",ignore=[400,404])
es.indices.create(
    index="platform",
    body={
      'settings': {
        'number_of_shards': 5,
        'number_of_replicas': 1,
        'analysis': {
          "analyzer":{"default":{"type": "smartcn"}}
          }
        },
      'mappings':{
         'facebook':{
            'properties':{
                u'留言數':{'type': 'integer'}
                }
            }   
        }  
      },
    ignore=400
)
def importCSV(indexName,typeName,fileName):
    if not os.path.exists(fileName):
        print ("file not found")
        return
    actions=[]
    if not es.indices.exists(index=indexName,allow_no_indices=True):
        es.indices.create(index=indexName,body={},ignore=400)
    for item in csv.DictReader(open(fileName, 'r')):  
        actions.append({"_index":indexName,"_type":typeName,"_source":item})
    res = helpers.bulk(es,actions,chunk_size=100)
    es.indices.flush(index=[indexName])
    return len(actions)
result=importCSV("platform","facebook","demo_test.csv")
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
es_index=IndicesClient(es)
es_index.get_mapping(index="platform",doc_type="facebook")
need_save = es.search(index='platform',doc_type='facebook', body={"query":{"match_phrase":{"_all":u"蔡英文"}},"sort":{u"留言數":{"order":"desc"}}})
es.count(index='platform',doc_type='facebook', body={"query":{"match_phrase":{"_all":u"蔡英文"}},"sort":{u"留言數":{"order":"desc"}}})
len(need_save['hits']['hits'])
import json
need_save = json.dumps(need_save['hits']['hits'],ensure_ascii=False).encode('utf8')
g = open('save_the_file.csv','wb+')
g.write(need_save)
g.close()
pd.read_csv('save_the_file.csv')
#打開看存檔
g = open('save_the_file.csv','r+')
y = json.load(g)
g.close()
print (y)
g = open('save_the_file.json','wb+')
g.write(need_save)
g.close()
pd.read_json('save_the_file.json')
#打開看存檔
g = open('save_the_file.json','r+')
y = json.load(g)
g.close()
print (y)
