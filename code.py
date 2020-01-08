import pandas
import requests
import os
import shutil
import threading as Thread
from urllib import request
import glob
from threading import Thread
import time
#Reading CSV
df = pandas.read_csv("yourfile.csv")
#url column name
urls=df['Labeled Data']
#Name Column name
names=df['ID']
datalist=[]
namelist=[]
records=len(df)
threadsno=int(input("Enter no of Threads:"))
recordsperthread=records/threadsno
recordsperthread=round(recordsperthread)
jobs = []
a=0
b=recordsperthread
for j in range (0,threadsno):
    thread = Thread(target=fetch,args=(a,b,j))
    b=b+recordsperthread
    a=a+recordsperthread
    jobs.append(thread)
    # Start the threads 
for j in jobs:
    j.start()

    # Ensure all of the threads have finished
for j in jobs:
    j.join()
print('yes;;;')

#Function to create Folders according to the no of threads and saving the images in folders 
def fetch(a,b,j):
    for d in range(a,b):
        datalist.append(urls[d])
        namelist.append(names[d])
    missing=[]
    if os.path.exists("Thread-"+str(j)+"Record- "+str(a)+"to"+str(b)):
        print('exits')
    else:
        direc=os.mkdir("Thread-"+str(j)+"Record- "+str(a)+"to"+str(b))
    for img in range(len(datalist)):
        try:
            
            f = open("Thread-"+str(j)+"Record- "+str(a)+"to"+str(b)+"/"+namelist[img]+'.jpg', 'wb')
            f.write(request.urlopen(datalist[img]).read())
            f.close()
        except:
            print("Exception at ",img)
            missing.append(names[img])
        pass
