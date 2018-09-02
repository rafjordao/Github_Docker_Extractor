from pymongo import MongoClient
from bson import json_util

import requests as rq
from bs4 import BeautifulSoup

import MySQLdb
import json
import threading
import re
import sys
import os
import time
import yaml
import json
import queue

print("Executando scrapper...")

myDB = MySQLdb.connect(host="127.0.0.1",port=3306,user="devops",passwd="BHU*nji9",db="devops")
cHandler = myDB.cursor()
cHandler.execute("SELECT * FROM dockercompose")
results = cHandler.fetchall()

print("Banco de dados lido...")

client = MongoClient()
db = client['repos-database']
mongoDockerComposes = db['dockercompose_repos']
headers = {'user-agent':'Mozilla/5.0'}

def dockercomposeExtractor(id,repoName, dockercomposePath, resultQueue):
    print("Entrei no Thread "+str(id))
    try:
        urlDockerCompose = "https://raw.githubusercontent.com/"+repoName+"/master/"+dockercomposePath
        page = rq.get(urlDockerCompose,headers=headers)
        parsed_page = BeautifulSoup(page.text,"html.parser")
        dockercompose=parsed_page.text
        objId = mongoDockerComposes.insert_one(json_util.loads(json.dumps(yaml.load(dockercompose), sort_keys=True, indent=2))).inserted_id
        objDict = {"path":dockercomposePath,
                    "repo":repoName}
        mongoDockerComposes.update(objId,objDict)
    except:
        print("Arquivo não existe")
    finally:
        resultQueue.put((id,"done"))
        print("Saí do Thread "+str(id))

t=[]
q = queue.Queue()
rangeId=[114433,114441,114443,114444,114446,114447,114448,114449,114450]
for i in rangeId:
    t.append(threading.Thread(target=dockercomposeExtractor,args=(i,results[i][1],results[i][2],q,)))
    t[-1].start()
    if(len(t)==9):
        resultT=[]
        while(len(resultT)<9):
            resultT.append(q.get())
        del t[:]
        del resultT[:]
        print("Último id processado foi "+str(i))
        print("\nContagem até o reinício do scrapping: (3s)")
        for s in range(1,4):
            print(str(s)+"s\n")
            time.sleep(1)