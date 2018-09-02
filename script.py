from pymongo import MongoClient
from bson import json_util

import requests as rq
from bs4 import BeautifulSoup
import json
import re
import sys
import os
import time
import yaml
import threading
import pprint

client = MongoClient()
db = client['repos-database']
mongoRepos = db['repos']
mongoDockerFiles = db['dockerfile_repos']
mongoDockerComposes = db['dockercompose_repos']

headers = {'user-agent':'Mozilla/5.0'}

def repoExtract(repoName):
    #print(threadName+" : "+results[projectId][0])
    #url = json.loads(rq.get(results[projectId][0]).text)
    repoQuery = mongoRepos.find_one({"name":repoName}) 
    if(repoQuery is None):
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
        return mongoRepos.insert_one(infos).inserted_id
    else:
        return repoQuery.get("_id")

def dockerfileExtract(id, repoName, dockerfilePath):
    savedDockerFile = mongoDockerFiles.find_one({"repoName":repoName,"dockerfilePath":dockerfilePath})
    if(savedDockerFile is None):
        urlDockerfile = "https://github.com/"+repoName+"/blob/master/"+dockerfilePath
        page = rq.get(urlDockerfile,headers=headers)
        # ========================================================
        parsed_page = BeautifulSoup(page.text,"html.parser").find("table",attrs={'class':'highlight tab-size js-file-line-container'})
        dockerfile = {}
        dockerfileRepo = {}
        dockerfileRepo["repo"]=id
        dockerfileRepo["repoName"]=repoName
        dockerfileRepo["path"]=dockerfilePath
        if(parsed_page!=None and parsed_page.find_all("td",attrs={'class':'blob-code blob-code-inner js-file-line'})!=None):
            env=0
            previousDockerLine=""
            for td in parsed_page.find_all("td",attrs={'class':'blob-code blob-code-inner js-file-line'}):
                td = td.text.strip()
                if(td.lower().startswith("from ")):
                    #dockerFrom.append(re.match('\w{4} (.*)',td).group(1))
                    env+=1
                    dictKey="Config"+str(env)
                    dockerfile[dictKey]={"FROM":re.match('\w{4} (.*)',td).group(1)}
                    previousDockerLine = "FROM"
                elif(td.lower().startswith("run ")):
                    #dockerRun.append(re.match('\w{3} (.*)',td).group(1))
                    if "RUN" in dockerfile:
                        dockerfile[dictKey]["RUN"].append(re.match('\w{3} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["RUN"]=[re.match('\w{3} (.*)',td).group(1)]
                    previousDockerLine = "RUN"
                elif(td.lower().startswith("add ")):
                    #dockerAdd.append(re.match('\w{3} (.*)',td).group(1))
                    if "ADD" in dockerfile:
                        dockerfile[dictKey]["ADD"].append(re.match('\w{3} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["ADD"]=[re.match('\w{3} (.*)',td).group(1)]
                    previousDockerLine = "ADD"
                elif(td.lower().startswith("workdir ")):
                    #dockerWorkdir.append(re.match('\w{7} (.*)',td).group(1))
                    if "WORKDIR" in dockerfile:
                        dockerfile[dictKey]["WORKDIR"].append(re.match('\w{7} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["WORKDIR"]=[re.match('\w{7} (.*)',td).group(1)]
                    previousDockerLine = "WORKDIR"
                elif(td.lower().startswith("expose ")):
                    #dockerExpose.append(re.match('\w{6} (.*)',td).group(1))
                    if "EXPOSE" in dockerfile:
                        dockerfile[dictKey]["EXPOSE"].append(re.match('\w{6} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["EXPOSE"]=[re.match('\w{6} (.*)',td).group(1)]
                    previousDockerLine = "EXPOSE"
                elif(td.lower().startswith("copy ")):
                    #dockerCopy.append(re.match('\w{4} (.*)',td).group(1))
                    if "COPY" in dockerfile:
                        dockerfile[dictKey]["COPY"].append(re.match('\w{4} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["COPY"]=[re.match('\w{4} (.*)',td).group(1)]
                    previousDockerLine = "COPY"
                elif(td.lower().startswith("entrypoint ")):
                    #dockerEntrypoint.append(re.match('\w{10} (.*)',td).group(1))
                    if "ENTRYPOINT" in dockerfile:
                        dockerfile[dictKey]["ENTRYPOINT"].append(re.match('\w{10} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["ENTRYPOINT"]=[re.match('\w{10} (.*)',td).group(1)]
                    previousDockerLine = "ENTRYPOINT"
                elif(td.lower().startswith("cmd ")):
                    #dockerCmd.append(re.match('\w{3} (.*)',td).group(1))
                    if "CMD" in dockerfile:
                        dockerfile[dictKey]["CMD"].append(re.match('\w{3} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["CMD"]=[re.match('\w{3} (.*)',td).group(1)]
                    previousDockerLine = "CMD"
                elif(td.lower().startswith("volume ")):
                    #dockerVolume.append(re.match('\w{6} (.*)',td).group(1))
                    if "VOLUME" in dockerfile:
                        dockerfile[dictKey]["VOLUME"].append(re.match('\w{6} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["VOLUME"]=[re.match('\w{6} (.*)',td).group(1)]
                    previousDockerLine = "VOLUME"
                elif(td.lower().startswith("user ")):
                    #dockerUser.append(re.match('\w{4} (.*)',td).group(1))
                    if "USER" in dockerfile:
                        dockerfile[dictKey]["USER"].append(re.match('\w{4} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["USER"]=[re.match('\w{4} (.*)',td).group(1)]
                    previousDockerLine = "USER"
                elif(td.lower().startswith("label ")):
                    #dockerLabel.append(re.match('\w{5} (.*)',td).group(1))
                    if "LABEL" in dockerfile:
                        dockerfile[dictKey]["LABEL"].append(re.match('\w{5} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["LABEL"]=[re.match('\w{5} (.*)',td).group(1)]
                    previousDockerLine = "LABEL"
                elif(td.lower().startswith("arg ")):
                    #dockerArg.append(re.match('\w{3} (.*)',td).group(1))
                    if "ARG" in dockerfile:
                        dockerfile[dictKey]["ARG"].append(re.match('\w{3} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["ARG"]=[re.match('\w{3} (.*)',td).group(1)]
                    previousDockerLine = "ARG"
                elif(td.lower().startswith("env ")):
                    #dockerEnv.append(re.match('\w{3} (.*)',td).group(1))
                    if "ENV" in dockerfile:
                        dockerfile[dictKey]["ENV"].append(re.match('\w{3} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["ENV"]=[re.match('\w{3} (.*)',td).group(1)]
                    previousDockerLine = "ENV"
                elif(td.lower().startswith("onbuild ")):
                    #dockerOnbuild.append(re.match('\w{7} (.*)',td).group(1))
                    if "ONBUILD" in dockerfile:
                        dockerfile[dictKey]["ONBUILD"].append(re.match('\w{7} (.*)',td).group(1))
                    else:
                        dockerfile[dictKey]["ONBUILD"]=[re.match('\w{7} (.*)',td).group(1)]
                    previousDockerLine = "ONBUILD"
                elif(td.lower().startswith("maintainer ")):
                    #dockerMaintainer = re.match('\w{10} (.*)',td).group(1)
                    if "MAINTAINER" in dockerfile:
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
        dockerfileRepo["config"]=dockerfile.items()
        return mongoDockerFiles.insert_one(dict(dockerfile)).inserted_id
    else:
        return savedDockerFile.get("_id")

def dockercomposeExtractor(id, repoName, dockercomposePath):
    savedDockerCompose = mongoDockerComposes.find_one({"repoName":repoName,"docker-composePath":dockercomposePath})
    if(savedDockerCompose is None):
        urlDockerCompose = "https://raw.githubusercontent.com/"+repoName+"/master/"+dockercomposePath
        page = rq.get(urlDockerCompose,headers=headers)
        parsed_page = BeautifulSoup(page.text,"html.parser")
        dockercompose=parsed_page.text
        return mongoDockerComposes.insert_one(json_util.loads(json.dumps(yaml.load(dockercompose), sort_keys=True, indent=2))).inserted_id
    else:
        return savedDockerCompose.get("_id")

id = repoExtract("3846masa/mastodon")
dfId = dockerfileExtract(id,"3846masa/mastodon","Dockerfile")

id = repoExtract("yangtao309/nodejs_app")
dcId = dockercomposeExtractor(id,"yangtao309/nodejs_app","docker-compose.yml")
