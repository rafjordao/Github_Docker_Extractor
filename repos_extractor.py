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

from github import Github, GithubException

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

tokens = []
with open("./resources/tokens.txt",'r',encoding='utf-8') as tokenFile:
    for token in tokenFile:
        tokens.append(token.strip())

def repoExtractGit(id,repoName,token,resultQueue=None):
    g = Github(token)
    repoId = None
    repoQuery = mongoRepos.find_one({"name":repoName}) 
    if(repoQuery is None):
        urlRepo = "https://github.com/"+repoName
        pageRepo = rq.get(urlRepo,headers=headers)
        if(not pageRepo.status_code == 404):
            path = re.sub('https://github.com/','',pageRepo.url)
            repo = g.get_repo(path)
            if(repoName!=path):
                repoName=path
            repoId = mongoRepoInsert(repoName,repo)
        else:
            print("Repositório Inexistente")
    else:
        print("Repositorio ja cadastrado!")
    resultQueue.put((id,"done"))
    print("Sai do Thread "+str(id))
    return repoId

def mongoRepoInsert(repoName,g):
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
    if(not pageRepo.status_code==404):
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
    if(g.fork):
        forkedFrom = mongoRepoInsert(g.parent.full_name,g.parent)
    infos["forked_from"] = forkedFrom
    infos["readme"] = BeautifulSoup(pageReadme.text,"html.parser").text
    infos["url"] = urlRepo
    infos["name"] = repoName
    return mongoRepos.insert_one(infos).inserted_id


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

# def repoExtract(id,repoName,resultQueue=None):
#     if(id!=0):
#         print("Entrei no Thread "+str(id))
#     repoQuery = mongoRepos.find_one({"name":repoName}) 
#     repoId = None
#     if(repoQuery is None):
#         try:
#             urlRepo = "https://github.com/"+repoName+"/tree/master"
#             urlReadme = "https://raw.githubusercontent.com/"+repoName+"/master/README.md"
#             #urlDockerfile = "https://github.com/"+repoName+"/blob/master/"+dockerfilePath
#             infos={}
#             #try:
#             # pageRepo = rq.get(urlRepo,headers=headers)
#             pageReadme = rq.get(urlReadme,headers=headers)
#             session = dryscrape.Session()
#             session.visit(urlRepo)
#             # session.wait_for_safe(lambda: session.at_xpath("//ul[@class='numbers-summary']/li[4]/a/span"))
#             time.sleep(10)
#             pageRepo = session.body()
#             bsRepo = BeautifulSoup(pageRepo,"html.parser")
#             with open("./index"+str(id)+".html",'r+',encoding='utf-8') as fileHandler:
#                 fileHandler.write(str(bsRepo))
#             language = bsRepo.find("ol",attrs={'class':'repository-lang-stats-numbers'})
#             if(language!=None):
#                 languages = language.find_all("a")
#                 language = {}
#                 for l in languages:
#                     language[l.find("span",attrs={'class':'lang'}).text]=float(re.sub('%','',l.find("span",attrs={'class':'percent'}).text))/100
#                 language=dict(language)
#             infos["languagues"]=language
#             infos["watchers"] = int(re.sub(',','',bsRepo.find("ul",attrs={"class":'pagehead-actions'}).select('li:nth-of-type(1) > a:nth-of-type(2)')[0].text.strip()))
#             infos["stars"] = int(re.sub(',','',bsRepo.find("ul",attrs={"class":'pagehead-actions'}).select('li:nth-of-type(2) > a:nth-of-type(2)')[0].text.strip()))
#             infos["forks"] = int(re.sub(',','',bsRepo.find("ul",attrs={"class":'pagehead-actions'}).select('li:nth-of-type(3) > a:nth-of-type(2)')[0].text.strip()))
#             infos["commits"] = int(re.sub(',','',bsRepo.find("ul",attrs={"class":'numbers-summary'}).select('li:nth-of-type(1) > a > span')[0].text.strip()))
#             infos["branches"] = int(re.sub(',','',bsRepo.find("ul",attrs={"class":'numbers-summary'}).select('li:nth-of-type(2) > a > span')[0].text.strip()))
#             infos["releases"] = int(re.sub(',','',bsRepo.find("ul",attrs={"class":'numbers-summary'}).select('li:nth-of-type(3) > a > span')[0].text.strip()))
#             contributors = bsRepo.find("ul",attrs={"class":'numbers-summary'}).select('li:nth-of-type(4) > a > span')[0].text.strip()
#             if not contributors:
#                 pageContrib = session.visit("https://github.com/"+repoName+"/graphs/contributors")
#                 pageContrib.wait_while()
#                 contributors = BeautifulSoup(pageContrib.text,"html.parser").find("div",attrs={"id":"contributors"}).select('ol > li')
#                 contributors = len(contributors)
#             else:
#                 contributors = re.sub(',','',contributors)
#             infos["contributors"] = int(contributors)
#             repoLicense = bsRepo.find("ul",attrs={"class":'numbers-summary'}).select('li:nth-of-type(5)')
#             if not repoLicense:
#                 repoLicense=None
#             else:
#                 repoLicense = repoLicense[0].text.strip()
#             infos["license"] = repoLicense
#             forkedFrom = bsRepo.find("span",attrs={"class":'fork-flag'})
#             if(not forkedFrom is None):
#                 forkedFrom = repoExtract(0,forkedFrom.find('a').text.strip())
#             infos["forked_from"] = forkedFrom
#             print(infos)
#             infos["readme"] = BeautifulSoup(pageReadme.text,"html.parser").text
#             infos["url"] = urlRepo
#             infos["name"] = repoName
#             repoId = mongoRepos.insert_one(infos).inserted_id
#         except Exception as inst:
#             print(type(inst))
#             #print(pageRepo.status_code)
#             traceback.print_exc()
#             print(urlRepo)
#             print("Repositorio nao existe!")
#     else:
#         print("Repositorio ja cadastrado!")
#     if(id!=0):
#         resultQueue.put((id,"done"))
#         print("Sai do Thread "+str(id))
#     return repoId

t=[]
q = queue.Queue()
initialId=14607
threadsNumber=len(tokens)*2
tokenQtd=len(tokens)
print("Iniciando com id "+str(initialId))
for i in range(initialId,len(results)+1):
    t.append(threading.Thread(target=repoExtractGit,args=(i,results[i-1][1],tokens[i%tokenQtd],q,)))
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