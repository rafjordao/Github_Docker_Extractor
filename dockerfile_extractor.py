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

from datetime import datetime

from github import Github, GithubException

print("Executando scrapper...")

myDB = MySQLdb.connect(host="127.0.0.1",port=3306,user="devops",passwd="BHU*nji9",db="devops")
cHandler = myDB.cursor()
cHandler.execute("SELECT * FROM dockerfile")
results = cHandler.fetchall()

print("Banco de dados lido...")

client = MongoClient()
db = client['repos-database']
mongoRepos = db['repos']
mongoDockerFiles = db['dockerfile_repos']
headers = {'user-agent':'Mozilla/5.0'}

tokens = []
with open("./resources/tokens.txt",'r',encoding='utf-8') as tokenFile:
    for token in tokenFile:
        tokens.append(token.strip())

def repoExtractGit(id,repoName,token,dockerfilePath,resultQueue=None):
    repoId = None
    repoQuery = mongoRepos.find_one({"name":repoName})
    try:
        if(repoQuery is None):
            urlRepo = "https://github.com/"+repoName
            pageRepo = rq.get(urlRepo,headers=headers)
            if(not pageRepo.status_code == 404):
                path = re.sub('https://github.com/','',pageRepo.url)
                if(repoName!=path):
                    repoName=path
                repoQuery = mongoRepos.find_one({"name":repoName})
                if(repoQuery is None):
                    g = Github(token)
                    repo = g.get_repo(path)
                    repoId = mongoRepoInsert(repoName,repo)
                    repoQuery = mongoRepos.find_one({"name":repoName})
                    if(repoQuery is None):
                        print("Repositório Vazio!")
                        resultQueue.put((id,"done"))
                        return None
            else:
                print("Repositório Inexistente!")
                resultQueue.put((id,"done"))
                return None
        dockerfileExtract(repoName,dockerfilePath,repoQuery)
        print("Dockerfile "+str(id)+" salvo")
        resultQueue.put((id,"done"))
        return repoId
    except rq.exceptions.ReadTimeout:
        print("ReadTimeout, tentando novamente")
        return repoExtractGit(id,repoName,token,dockerfilePath,resultQueue)
    except GithubException as e:
        print("Ocorreu um erro na requisição do github : "+e.data['message'])
        resultQueue.put((id,"done"))
        print("Sai do Thread "+str(id))
    except rq.exceptions.ConnectionError:
        print("ConnectionErro, tentando novamente")
        return repoExtractGit(id,repoName,token,dockerfilePath,resultQueue)
    return repoId

def mongoRepoInsert(repoName,g):
    try:
        urlRepo = "https://github.com/"+repoName+"/tree/"+g.default_branch
        urlReadme = "https://raw.githubusercontent.com/"+repoName+"/"+g.default_branch+"/README.md"
        pageRepo = rq.get(urlRepo,headers=headers)
        pageReadme = rq.get(urlReadme,headers=headers)
        infos={}
        bsRepo = BeautifulSoup(pageRepo.text,"html.parser")
        language = bsRepo.find("ol",attrs={'class':'repository-lang-stats-numbers'})
        if(language!=None):
            languages = language.find_all("a")
            language = {}
            for l in languages:
                language[l.find("span",attrs={'class':'lang'}).text]=float(re.sub('%','',l.find("span",attrs={'class':'percent'}).text))/100
            language=dict(language)
        infos["languagues"]=language
        infos["watchers"] = int(g.watchers_count)
        infos["stars"] = int(g.stargazers_count)
        infos["forks"] = int(g.forks_count)
        infos["created_at"] = g.created_at
        infos["updated_at"] = g.updated_at
        infos["default_branch"] = g.default_branch
        if(not pageRepo.status_code==404 and not bsRepo.find('div',attrs={'class':'blankslate blankslate-narrow'})):
            infos["commits"] = getTotalByApi(bsRepo.find("ul",attrs={"class":'numbers-summary'}).select('li:nth-of-type(1) > a > span'),repoName,"commits")
            infos["branches"] = getTotalByApi(bsRepo.find("ul",attrs={"class":'numbers-summary'}).select('li:nth-of-type(2) > a > span'),repoName,"branches")
            infos["releases"] = getTotalByApi(bsRepo.find("ul",attrs={"class":'numbers-summary'}).select('li:nth-of-type(3) > a > span'),repoName,"releases")
            repoLicense = bsRepo.find("ul",attrs={"class":'numbers-summary'}).select('li:nth-of-type(5)')
            if not repoLicense:
                repoLicense=None
            else:
                repoLicense = repoLicense[0].text.strip()
        else :
            return None
        infos["license"] = repoLicense
        forkedFrom = None
        if(g.fork and mongoRepos.find_one({"name":g.parent.full_name})):
            forkedFrom = mongoRepos.find_one({"name":g.parent.full_name})
        elif(g.fork):
            print(g.parent.full_name)
            forkedFrom = mongoRepoInsert(g.parent.full_name,g.parent)
        infos["forked_from"] = forkedFrom
        infos["readme"] = BeautifulSoup(pageReadme.text,"html.parser").text
        infos["url"] = urlRepo
        infos["name"] = repoName
        return mongoRepos.insert_one(infos).inserted_id
    except rq.exceptions.ConnectionError:
        print("ConnectionErro, tentando novamente")
        return mongoRepoInsert(repoName,g)


def getTotalByApi(bs,repoName,item):
    if(bs == None):
        return getLastPagination(repoName,item)
    else:
        return int(re.sub(',','',bs[0].text.strip()))

def getLastPagination(repoName,item):
    url = "https://api.github.com/repos/"+repoName+"/"+item+"?per_page=1"
    response = rq.get(url,headers=headers)
    link = response.info().get('Link')
    return int(re.match('.*=(.*)>; rel="last"',link).group(1))

def dockerfileExtract(repoName, dockerfilePath, mongoRepoInstance):
    savedDockerFile = mongoDockerFiles.find_one({"repoName":repoName,"path":dockerfilePath})
    if(savedDockerFile is None):
        urlDockerfile = "https://github.com/"+repoName+"/blob/"+mongoRepoInstance.get('default_branch')+"/"+dockerfilePath
        page = rq.get(urlDockerfile,headers=headers)
        # ========================================================
        parsed_page = BeautifulSoup(page.text,"html.parser").find("table",attrs={'class':'highlight tab-size js-file-line-container'})
        dockerfile = {}
        dockerfileRepo = {}
        dockerfileRepo["repo"]=mongoRepoInstance.get('_id')
        dockerfileRepo["repoName"]=repoName
        dockerfileRepo["path"]=dockerfilePath
        if(parsed_page!=None and parsed_page.find_all("td",attrs={'class':'blob-code blob-code-inner js-file-line'})!=None):
            env=0
            previousDockerLine=""
            dictKey="Config1"
            dockerfile[dictKey]={}
            for td in parsed_page.find_all("td",attrs={'class':'blob-code blob-code-inner js-file-line'}):
                td = td.text.strip()
                if(td.lower().startswith("from ")):
                    #dockerFrom.append(re.match('\w{4} (.*)',td).group(1))
                    env+=1
                    dictKey="Config"+str(env)
                    if dictKey in dockerfile:
                        dockerfile[dictKey]["FROM"]=re.match('\w{4} (.*)',td).group(1)
                    else:
                        dockerfile[dictKey]={"FROM":re.match('\w{4} (.*)',td).group(1)}
                    previousDockerLine = "FROM"
                elif(td.lower().startswith("run ")):
                    #dockerRun.append(re.match('\w{3} (.*)',td).group(1))
                    if "RUN" in dockerfile[dictKey]:
                        dockerfile[dictKey]["RUN"].append(re.match('\w{3} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["RUN"]=[re.match('\w{3} (.*)',td).group(1)]
                    previousDockerLine = "RUN"
                elif(td.lower().startswith("add ")):
                    #dockerAdd.append(re.match('\w{3} (.*)',td).group(1))
                    if "ADD" in dockerfile[dictKey]:
                        dockerfile[dictKey]["ADD"].append(re.match('\w{3} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["ADD"]=[re.match('\w{3} (.*)',td).group(1)]
                    previousDockerLine = "ADD"
                elif(td.lower().startswith("workdir ")):
                    #dockerWorkdir.append(re.match('\w{7} (.*)',td).group(1))
                    if "WORKDIR" in dockerfile[dictKey]:
                        dockerfile[dictKey]["WORKDIR"].append(re.match('\w{7} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["WORKDIR"]=[re.match('\w{7} (.*)',td).group(1)]
                    previousDockerLine = "WORKDIR"
                elif(td.lower().startswith("expose ")):
                    #dockerExpose.append(re.match('\w{6} (.*)',td).group(1))
                    if "EXPOSE" in dockerfile[dictKey]:
                        dockerfile[dictKey]["EXPOSE"].append(re.match('\w{6} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["EXPOSE"]=[re.match('\w{6} (.*)',td).group(1)]
                    previousDockerLine = "EXPOSE"
                elif(td.lower().startswith("copy ")):
                    #dockerCopy.append(re.match('\w{4} (.*)',td).group(1))
                    if "COPY" in dockerfile[dictKey]:
                        dockerfile[dictKey]["COPY"].append(re.match('\w{4} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["COPY"]=[re.match('\w{4} (.*)',td).group(1)]
                    previousDockerLine = "COPY"
                elif(td.lower().startswith("entrypoint ")):
                    #dockerEntrypoint.append(re.match('\w{10} (.*)',td).group(1))
                    if "ENTRYPOINT" in dockerfile[dictKey]:
                        dockerfile[dictKey]["ENTRYPOINT"].append(re.match('\w{10} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["ENTRYPOINT"]=[re.match('\w{10} (.*)',td).group(1)]
                    previousDockerLine = "ENTRYPOINT"
                elif(td.lower().startswith("cmd ")):
                    #dockerCmd.append(re.match('\w{3} (.*)',td).group(1))
                    if "CMD" in dockerfile[dictKey]:
                        dockerfile[dictKey]["CMD"].append(re.match('\w{3} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["CMD"]=[re.match('\w{3} (.*)',td).group(1)]
                    previousDockerLine = "CMD"
                elif(td.lower().startswith("volume ")):
                    #dockerVolume.append(re.match('\w{6} (.*)',td).group(1))
                    if "VOLUME" in dockerfile[dictKey]:
                        dockerfile[dictKey]["VOLUME"].append(re.match('\w{6} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["VOLUME"]=[re.match('\w{6} (.*)',td).group(1)]
                    previousDockerLine = "VOLUME"
                elif(td.lower().startswith("user ")):
                    #dockerUser.append(re.match('\w{4} (.*)',td).group(1))
                    if "USER" in dockerfile[dictKey]:
                        dockerfile[dictKey]["USER"].append(re.match('\w{4} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["USER"]=[re.match('\w{4} (.*)',td).group(1)]
                    previousDockerLine = "USER"
                elif(td.lower().startswith("label ")):
                    #dockerLabel.append(re.match('\w{5} (.*)',td).group(1))
                    if "LABEL" in dockerfile[dictKey]:
                        dockerfile[dictKey]["LABEL"].append(re.match('\w{5} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["LABEL"]=[re.match('\w{5} (.*)',td).group(1)]
                    previousDockerLine = "LABEL"
                elif(td.lower().startswith("arg ")):
                    #dockerArg.append(re.match('\w{3} (.*)',td).group(1))
                    if "ARG" in dockerfile[dictKey]:
                        dockerfile[dictKey]["ARG"].append(re.match('\w{3} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["ARG"]=[re.match('\w{3} (.*)',td).group(1)]
                    previousDockerLine = "ARG"
                elif(td.lower().startswith("env ")):
                    #dockerEnv.append(re.match('\w{3} (.*)',td).group(1))
                    if "ENV" in dockerfile[dictKey]:
                        dockerfile[dictKey]["ENV"].append(re.match('\w{3} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["ENV"]=[re.match('\w{3} (.*)',td).group(1)]
                    previousDockerLine = "ENV"
                elif(td.lower().startswith("onbuild ")):
                    #dockerOnbuild.append(re.match('\w{7} (.*)',td).group(1))
                    if "ONBUILD" in dockerfile[dictKey]:
                        dockerfile[dictKey]["ONBUILD"].append(re.match('\w{7} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["ONBUILD"]=[re.match('\w{7} (.*)',td).group(1)]
                    previousDockerLine = "ONBUILD"
                elif(td.lower().startswith("maintainer ")):
                    #dockerMaintainer = re.match('\w{10} (.*)',td).group(1)
                    if "MAINTAINER" in dockerfile[dictKey]:
                        dockerfile[dictKey]["MAINTAINER"].append(re.match('\w{10} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["MAINTAINER"]=[re.match('\w{10} (.*)',td).group(1)]
                    previousDockerLine = "MAINTAINER"
                elif(not td):
                    previousDockerLine = ""
                elif(previousDockerLine == "RUN"):
                    dockerfile[dictKey]["RUN"][-1]=dockerfile[dictKey]["RUN"][-1]+td
                elif(previousDockerLine == "LABEL"):
                    dockerfile[dictKey]["LABEL"].append(td)
        dockerfileRepo["config"]=dockerfile
        return mongoDockerFiles.insert_one(dockerfileRepo).inserted_id
    else:
        print("Dockerfile já inserido!")
        return savedDockerFile.get("_id")

t=[]
q = queue.Queue()
initialId=445301
threadsNumber=400
tokenQtd=len(tokens)
print("Iniciando com id "+str(initialId))
for i in range(initialId,len(results)+1):
    t.append(threading.Thread(target=repoExtractGit,args=(i,results[i-1][1],tokens[i%tokenQtd],results[i-1][2],q,)))
    t[-1].start()
    if(len(t)==threadsNumber):
        resultT=[]
        while(len(resultT)<threadsNumber):
            resultT.append(q.get())
        del t[:]
        del resultT[:]
        print("Ultimo id processado foi "+str(i))
        print("\nContagem ate o reinicio do scrapping: (3s)")
        for s in range(1,4):
            print(str(s)+"s\n")
            time.sleep(1)