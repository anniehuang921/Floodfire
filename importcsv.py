# -*- coding:utf-8 -*- 
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
    result=importCSV("class1","student","bat.csv")
    print "import size = "+str(result)
    endtime = datetime.datetime.now()
    print "import cost = "+str(endtime - starttime)