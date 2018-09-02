from pymongo import MongoClient
from bson import json_util

import requests as rq
from bs4 import BeautifulSoup

import MySQLdb
import json
import threading
import re
import sys
import traceback
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

def repoExtract(id,repoName,resultQueue=None):
    if(id!=0):
        print("Entrei no Thread "+str(id))
    #print(threadName+" : "+results[projectId][0])
    #url = json.loads(rq.get(results[projectId][0]).text)
    repoQuery = mongoRepos.find_one({"name":repoName}) 
    repoId = None
    if(repoQuery is None):
        try:
            urlRepo = "https://github.com/"+repoName+"/tree/master"
            urlReadme = "https://raw.githubusercontent.com/"+repoName+"/master/README.md"
            #urlDockerfile = "https://github.com/"+repoName+"/blob/master/"+dockerfilePath
            infos={}
            #try:
            pageRepo = rq.get(urlRepo,headers=headers)
            pageReadme = rq.get(urlReadme,headers=headers)
            print("Thread "+str(id)+" Status: "+str(pageRepo.status_code))
            bsRepo = BeautifulSoup(pageRepo.text,"html.parser")
            with open("./index"+str(id)+".html",'r+',encoding='utf-8') as fileHandler:
                fileHandler.write(str(bsRepo))
            language = bsRepo.find("ol",attrs={'class':'repository-lang-stats-numbers'})
            if(language!=None):
                languages = language.find_all("a")
                language = {}
                for l in languages:
                    language[l.find("span",attrs={'class':'lang'}).text]=float(re.sub('%','',l.find("span",attrs={'class':'percent'}).text))/100
                language=dict(language)
            infos["languagues"]=language
            infos["watchers"] = int(re.sub(',','',bsRepo.find("ul",attrs={"class":'pagehead-actions'}).select('li:nth-of-type(1) > a:nth-of-type(2)')[0].text.strip()))
            infos["stars"] = int(re.sub(',','',bsRepo.find("ul",attrs={"class":'pagehead-actions'}).select('li:nth-of-type(2) > a:nth-of-type(2)')[0].text.strip()))
            infos["forks"] = int(re.sub(',','',bsRepo.find("ul",attrs={"class":'pagehead-actions'}).select('li:nth-of-type(3) > a:nth-of-type(2)')[0].text.strip()))
            infos["commits"] = int(re.sub(',','',bsRepo.find("ul",attrs={"class":'numbers-summary'}).select('li:nth-of-type(1) > a > span')[0].text.strip()))
            infos["branches"] = int(re.sub(',','',bsRepo.find("ul",attrs={"class":'numbers-summary'}).select('li:nth-of-type(2) > a > span')[0].text.strip()))
            infos["releases"] = int(re.sub(',','',bsRepo.find("ul",attrs={"class":'numbers-summary'}).select('li:nth-of-type(3) > a > span')[0].text.strip()))
            contributors = bsRepo.find("ul",attrs={"class":'numbers-summary'}).select('li:nth-of-type(4) > a > span')
            if not contributors:
                pageContrib = rq.get("https://github.com/"+repoName+"/graphs/contributors",headers=headers)
                contributors = BeautifulSoup(pageContrib.text,"html.parser").find("div",attrs={"id":"contributors"}).select('ol > li')
                contributors = len(contributors)
            else:
                contributors = re.sub(',','',contributors[0].text.strip())
            infos["contributors"] = int(contributors)
            repoLicense = bsRepo.find("ul",attrs={"class":'numbers-summary'}).select('li:nth-of-type(5)')
            if not repoLicense:
                repoLicense=None
            else:
                repoLicense = repoLicense[0].text.strip()
            infos["license"] = repoLicense
            forkedFrom = bsRepo.find("span",attrs={"class":'fork-flag'})
            if(not forkedFrom is None):
                forkedFrom = repoExtract(0,forkedFrom.find('a').text.strip())
            infos["forked_from"] = forkedFrom
            print(infos)
            infos["readme"] = BeautifulSoup(pageReadme.text,"html.parser").text
            infos["url"] = urlRepo
            infos["name"] = repoName
            repoId = mongoRepos.insert_one(infos).inserted_id
        except Exception as inst:
            print(type(inst))
            print(pageRepo.status_code)
            traceback.print_exc()
            print(urlRepo)
            print("Repositório não existe!")
    else:
        print("Repositório já cadastrado!")
    if(id!=0):
        resultQueue.put((id,"done"))
        print("Saí do Thread "+str(id))
    return repoId

t=[]
q = queue.Queue()
initialId=1
threadsNumber=5
print("Iniciando com id "+str(initialId))
for i in range(initialId,len(results)+1):
    t.append(threading.Thread(target=repoExtract,args=(i,results[i-1][1],q,)))
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