import csv
import sys
import logging
import datetime 
from elasticsearch import Elasticsearch

reload(sys)  
sys.setdefaultencoding('utf-8')
logging.basicConfig()
es = Elasticsearch()

def exportCSV(indexName):
    count = 0
    finish=False
    csvfile = file(indexName+'.csv','wb')
    writer = csv.writer(csvfile)
    starttime = datetime.datetime.now()
    searchRes = es.search(index=indexName,size=100,body={"query": {"match_all": {}}},search_type="scan",scroll="60s")
    while True:
        scrollRes=es.scroll(scroll_id=searchRes["_scroll_id"],scroll="60s",ignore=[400, 404])
        res_list = scrollRes["hits"]["hits"]
        data=[]
        
        if not len(res_list) or finish:
            break
        if count==0:
            writer.writerow(tuple(res_list[0]["_source"].keys()))
        for item in res_list:
            #print tuple(item["_source"].values())
            data.append(tuple(item["_source"].values()))
            count+=1
            if count>=100000:
                finish=True
                break

        writer.writerows(data)
    csvfile.close()
    endtime = datetime.datetime.now()
    print "export size = "+str(count)
    print "export cost = "+str(endtime - starttime)
    

if __name__=="__main__":
   exportCSV("class1")