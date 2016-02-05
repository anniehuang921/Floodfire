import csv
import sys
import os
import logging
import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers

reload(sys)
sys.setdefaultencoding('utf-8')
logging.basicConfig()
es = Elasticsearch()

es.indices.create(
    index="platform",
    body={
      'settings': {
        'number_of_shards': 5,
        'number_of_replicas': 1,
        'analysis': {
          "analyzer":{"default":{"type": "smartcn"}}
          }
        }
      },
    ignore=400
)

def importCSV(indexName,typeName,fileName):
    if not os.path.exists(fileName):
        print "file not found"
        return
    actions=[]
    if not es.indices.exists(index=indexName,allow_no_indices=True):
        #print "not found index"
        es.indices.create(index=indexName,body={},ignore=400)
    for item in csv.DictReader(open(fileName, 'rb')):
        actions.append({"_index":indexName,"_type":typeName,"_source":encoding(item)})
    res = helpers.bulk(es,actions,chunk_size=100)
    es.indices.flush(index=[indexName])
    return len(actions)

    def encoding(item):
        for i in item:
            item[i]=str(item[i]).encode('utf-8')
        return item

        if __name__=="__main__":
            starttime = datetime.datetime.now()
            result=importCSV("platform","facebook","demo_test.csv")#index,type,file.csv
            print "import size = "+str(result)
            endtime = datetime.datetime.now()
            print "import cost = "+str(endtime - starttime)

            from elasticsearch import Elasticsearch
            es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

need_save = es.search(index='platform',doc_type='facebook', body={"query":{"match":{"_all":u"蔡英文"}},"sort":{u"留言數":{"order":"asc"}}})

import json
need_save = json.dumps(need_save,ensure_ascii=False).encode('utf8')
g = open('save_the_file.json','wb+')
json.dump(need_save,g)
g.close()
