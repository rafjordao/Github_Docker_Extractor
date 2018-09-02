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
cHandler.execute("SELECT * FROM repos")
results = cHandler.fetchall()

print("Banco de dados lido...")

client = MongoClient()
db = client['repos-database']
mongoRepos = db['repos']
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

def repoExtract(id,repoName,resultQueue):
    print("Entrei no Thread "+str(id))
    #print(threadName+" : "+results[projectId][0])
    #url = json.loads(rq.get(results[projectId][0]).text)
    repoQuery = mongoRepos.find_one({"name":repoName}) 
    if(repoQuery is None):
        try:
            urlRepo = "https://github.com/"+repoName+"/tree/master"
            urlReadme = "https://raw.githubusercontent.com/"+repoName+"/master/README.md"
            #urlDockerfile = "https://github.com/"+repoName+"/blob/master/"+dockerfilePath
            infos={}
            #try:
            pageRepo = rq.get(urlRepo,headers=headers)
            pageReadme = rq.get(urlReadme,headers=headers)
            language = BeautifulSoup(pageRepo.text,"html.parser").find("ol",attrs={'class':'repository-lang-stats-numbers'})
            if(language!=None):
                languages = language.find_all("a")
                language = {}
                for l in languages:
                    language[l.find("span",attrs={'class':'lang'}).text]=float(re.sub('%','',l.find("span",attrs={'class':'percent'}).text))/100
            infos["languagues"]=dict(language)
            infos["watchers"] = int(re.sub(',','',BeautifulSoup(pageRepo.text,"html.parser").find("ul",attrs={"class":'pagehead-actions'}).select('li:nth-of-type(1) > a:nth-of-type(2)')[0].text.strip()))
            infos["stars"] = int(re.sub(',','',BeautifulSoup(pageRepo.text,"html.parser").find("ul",attrs={"class":'pagehead-actions'}).select('li:nth-of-type(2) > a:nth-of-type(2)')[0].text.strip()))
            infos["forks"] = int(re.sub(',','',BeautifulSoup(pageRepo.text,"html.parser").find("ul",attrs={"class":'pagehead-actions'}).select('li:nth-of-type(3) > a:nth-of-type(2)')[0].text.strip()))
            infos["commits"] = int(re.sub(',','',BeautifulSoup(pageRepo.text,"html.parser").find("ul",attrs={"class":'numbers-summary'}).select('li:nth-of-type(1) > a > span')[0].text.strip()))
            infos["branches"] = int(re.sub(',','',BeautifulSoup(pageRepo.text,"html.parser").find("ul",attrs={"class":'numbers-summary'}).select('li:nth-of-type(2) > a > span')[0].text.strip()))
            infos["releases"] = int(re.sub(',','',BeautifulSoup(pageRepo.text,"html.parser").find("ul",attrs={"class":'numbers-summary'}).select('li:nth-of-type(3) > a > span')[0].text.strip()))
            if BeautifulSoup(pageRepo.text,"html.parser").find("ul",attrs={"class":'numbers-summary'}) is None:
                pageRepo = rq.get(urlRepo,headers=headers)
            infos["contributors"] = int(re.sub(',','',BeautifulSoup(pageRepo.text,"html.parser").find("ul",attrs={"class":'numbers-summary'}).select('li:nth-of-type(4) > a > span')[0].text.strip()))
            infos["license"] = BeautifulSoup(pageRepo.text,"html.parser").find("ul",attrs={"class":'numbers-summary'}).select('li:nth-of-type(5)')[0].text.strip()
            forkedFrom = BeautifulSoup(pageRepo.text,"html.parser").find("span",attrs={"class":'fork-flag'})
            if(not forkedFrom is None):
                forkedFrom = repoExtract(forkedFrom.find('a').text.strip())
            infos["forked_from"] = forkedFrom
            print(infos)
            infos["readme"] = BeautifulSoup(pageReadme.text,"html.parser").text
            infos["url"] = urlRepo
            infos["name"] = repoName
            mongoRepos.insert_one(infos).inserted_id
        except:
            print("Repositório não existe!")
    else:
        print("Repositório já cadastrado!")
    resultQueue.put((id,"done"))
    print("Saí do Thread "+str(id))

t=[]
q = queue.Queue()
initialId=407971
threadsNumber=60
print("Iniciando com id "+str(initialId))
for i in range(initialId,10394748):
    t.append(threading.Thread(target=dockercomposeExtractor,args=(i,results[i][1],q,)))
    t[-1].start()
    if(len(t)==threadsNumber):
        resultT=[]
        while(len(resultT)<threadsNumber):
            resultT.append(q.get())
        del t[:]
        del resultT[:]
        print("Último id processado foi "+str(i))
        print("\nContagem até o reinício do scrapping: (3s)")
        for s in range(1,4):
            print(str(s)+"s\n")
            time.sleep(1)