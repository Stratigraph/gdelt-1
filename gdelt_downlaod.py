import os
import urllib2
import logging
import logging.handlers
from bs4 import BeautifulSoup

# log settings
LOG_FILE = 'log/downgdelt1.0.log'
handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes = 1024*1024, backupCount = 5) 
fmt = '%(asctime)s  - %(message)s'
formatter = logging.Formatter(fmt)
handler.setFormatter(formatter)      
logger = logging.getLogger('v1')    
logger.addHandler(handler)           
logger.setLevel(logging.DEBUG)

LOG_FILE2 = 'log/downgdelt-2.0.log'
handler2 = logging.handlers.RotatingFileHandler(LOG_FILE2, maxBytes=1024 * 1024, backupCount=5)  
fmt2 = '%(asctime)s  - %(message)s'
formatter2 = logging.Formatter(fmt2)  
handler2.setFormatter(formatter2)      
logger2 = logging.getLogger('v2')   
logger2.addHandler(handler)           
logger2.setLevel(logging.DEBUG)

# Gdelt version1 contains three type of files: events,gkg,gkgcounts
# The parameter "datatype" means your choice is events or gkg, gkg contains gkg and gkgcounts
def gdeltdata1(datatype): 
        indexurl = urllib2.urlopen('http://data.gdeltproject.org/' + datatype + '/index.html')
        index = BeautifulSoup(indexurl, "lxml").findAll('a')
        for link in index:
                file = link.get('href')
                filelen = len(file)
                if 'zip' in file:
                        filelocal = '/data/gdeltorgin/' + datatype +'/' + file
                        fileurl = "http://data.gdeltproject.org/" + datatype + '/' + file
                        if os.path.exists(filelocal): 
                                pass
                        else:
                                f = urllib2.urlopen(fileurl)
                                data = f.read()
                                with open (filelocal, "wb") as code:
                                        code.write(data)
                                logger.info("下载文件 %s" % (fileurl,))
                                # copy the download files to another dir or machine for cluster process
                                fileserver = '/data2/gdelt_realtime/' + filelocal[17:]
                                os.system("scp " + filelocal + " " + fileserver)
# Gdelt version1 contains three type of files: events,gkg,mentions
# In version2, we don't need to choose parameter datatype, the function works for all the types.
def gdeltdata2():
    indexurl = urllib2.urlopen(
        'http://data.gdeltproject.org/gdeltv2/masterfilelist.txt')  # 需要下载的文件的主页
    index = []

    for link in indexurl:
    	index.append(link.strip('\n').split(' ')[-1])
    	
    for fileurl in index:
        filelocal = '/data/gdeltorgin/v2/' + fileurl.split('/')[-1] # 文件名
        if os.path.exists(filelocal):  # 如果文件存在则不下载
            pass
        else:
            try:
                urllib.urlretrieve(fileurl,filelocal)
                logger2.info("下载文件 %s" % (fileurl,))
                # copy the download files to another dir or machine for cluster process
                fileserver = '/data2/gdelt_realtime/' + filelocal[17:]
                os.system("scp " + filelocal + " " + fileserver)
        except urllib2.URLError, e:
                logger2.error("下载文件 %s %s" % (fileurl,str(e),))

# main function
if __name__=="__main__":
        gdeltdata1(datatype='events')
        gdeltdata1(datatype='gkg')
        gdeltdata2()
