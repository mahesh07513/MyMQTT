from datetime import datetime
from datetime import timedelta
import MySQLdb
import json
#import datetime
import sys
import time
import requests
import string
import random
import urllib2
import schedule

############################ power savings dayonce ################################
import sched, time
s = sched.scheduler(time.time, time.sleep)
def Check_Input_Data(sc):
    now = datetime.now()
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
    cursor = db.cursor()
    sqlq="select * from AC_Input_Units"
    cursor.execute(sqlq)
    results = cursor.fetchall()
    #print results
    #print now.strftime("%H:%M")
    #curtime=now.strftime("%H:%M")
    FMT='%H:%M'
    for row in results:
        curtime=now.strftime("%H:%M")
        #print "actual time : "+curtime+" , dbtime : "+row[7].strftime("%H:%M")
        dbtime=row[7].strftime("%H:%M")
        tdelta = datetime.strptime(curtime, FMT) - datetime.strptime(dbtime, FMT)
        #print "diff is : ", tdelta
        #print "macid :"+str(row[1])+" , name : "+str(row[2])+" , Owner : "+str(row[6])
        duration_in_s = tdelta.total_seconds()
        #print "in seconds ..... : ",divmod(duration_in_s, 60)[0]
        #print "in Hours ........: ",divmod(duration_in_s, 3600)[0]
        if divmod(duration_in_s, 3600)[0] > 1.0:
           #print "macid :"+str(row[1])+" , name : "+str(row[2])+" , Owner : "+str(row[6])
           name = row[2]
           name=name.replace(" ", "")
           macid = row[1]
           owner = row[6]
           if owner=="jts_admin":
              #print "ower is jts admin"
              pass
           else:
              url='https://api.textlocal.in/send/?apikey=irlz29yH7zo-7Yxe1P8dAA7ylIwBatMOwf9xJbukjg&numbers=9949359388&sender=JTSIOT&message=Alert:%20Name:'+name+'%20,MacId:'+macid+',Owner:'+owner+'%20Unit%20is%20not%20working%20from%20last%20One%20hour,%20Please%20check%20Once.' 
              serialized_data = urllib2.urlopen(url).read()
              data = json.loads(serialized_data)
    cursor.close()
    db.close()
    s.enter(3600, 1, Check_Input_Data, (sc,))
def Check_Operate_Data(sc):
    now = datetime.now()
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
    cursor = db.cursor()
    sqlq="select * from AC_Operate_Units"
    cursor.execute(sqlq)
    results = cursor.fetchall()
    #print results
    #print now.strftime("%H:%M")
    #curtime=now.strftime("%H:%M")
    FMT='%H:%M'
    for row in results:
        curtime=now.strftime("%H:%M")
        #print "actual time : "+curtime+" , dbtime : "+row[7].strftime("%H:%M")
        dbtime=row[12].strftime("%H:%M")
        tdelta = datetime.strptime(curtime, FMT) - datetime.strptime(dbtime, FMT)
        #print "diff is : ", tdelta
        #print "macid :"+str(row[1])+" , name : "+str(row[2])+" , Owner : "+str(row[6])
        duration_in_s = tdelta.total_seconds()
        #print "in seconds ..... : ",divmod(duration_in_s, 60)[0]
        #print "in Hours ........: ",divmod(duration_in_s, 3600)[0]
        if divmod(duration_in_s, 3600)[0] > 1.0:
           #print "macid :"+str(row[1])+" , name : "+str(row[2])+" , Owner : "+str(row[6])
           name = row[2]
           name=name.replace(" ", "")
           macid = row[1]
           owner = row[11]
           if owner=="jts_admin":
              #print "ower is jts admin"
              pass
           else:
              url='https://api.textlocal.in/send/?apikey=irlz29yH7zo-7Yxe1P8dAA7ylIwBatMOwf9xJbukjg&numbers=9949359388&sender=JTSIOT&message=Alert:%20Name:'+name+'%20,MacId:'+macid+',Owner:'+owner+'%20Unit%20is%20not%20working%20from%20last%20One%20hour,%20Please%20check%20Once.' 
              serialized_data = urllib2.urlopen(url).read()
              data = json.loads(serialized_data)
    cursor.close()
    db.close()
    s.enter(3600, 1, Check_Operate_Data, (sc,))
def Check_Power_Data(sc):
    now = datetime.now()
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
    cursor = db.cursor()
    sqlq="select * from AC_Power_Units"
    cursor.execute(sqlq)
    results = cursor.fetchall()
    #print results
    #print now.strftime("%H:%M")
    #curtime=now.strftime("%H:%M")
    FMT='%H:%M'
    for row in results:
        curtime=now.strftime("%H:%M")
        #print "actual time : "+curtime+" , dbtime : "+row[7].strftime("%H:%M")
        dbtime=row[8].strftime("%H:%M")
        tdelta = datetime.strptime(curtime, FMT) - datetime.strptime(dbtime, FMT)
        #print "diff is : ", tdelta
        #print "macid :"+str(row[1])+" , name : "+str(row[2])+" , Owner : "+str(row[6])
        duration_in_s = tdelta.total_seconds()
        #print "in seconds ..... : ",divmod(duration_in_s, 60)[0]
        #print "in Hours ........: ",divmod(duration_in_s, 3600)[0]
        if divmod(duration_in_s, 3600)[0] > 1.0:
           #print "macid :"+str(row[1])+" , name : "+str(row[2])+" , Owner : "+str(row[6])
           name = row[2]
           name=name.replace(" ", "")
           macid = row[1]
           owner = row[7]
           if owner=="jts_admin":
              #print "ower is jts admin"
              pass
           else:
              url='https://api.textlocal.in/send/?apikey=irlz29yH7zo-7Yxe1P8dAA7ylIwBatMOwf9xJbukjg&numbers=9949359388&sender=JTSIOT&message=Alert:%20Name:'+name+'%20,MacId:'+macid+',Owner:'+owner+'%20Unit%20is%20not%20working%20from%20last%20One%20hour,%20Please%20check%20Once.' 
              serialized_data = urllib2.urlopen(url).read()
              data = json.loads(serialized_data)
    cursor.close()
    db.close()
    s.enter(3600, 1, Check_Power_Data, (sc,))
#Check_Input_Data()
#Check_Operate_Data()
#Check_Power_Data()
'''
def do_something(sc): 
    print "Doing stuff..."
    # do your stuff
    url='https://api.textlocal.in/send/?apikey=irlz29yH7zo-7Yxe1P8dAA7ylIwBatMOwf9xJbukjg&numbers=9705474827&sender=JTSIOT&message=Alert:%20one%20hour%20Alert'
    serialized_data = urllib2.urlopen(url).read()
    data = json.loads(serialized_data)
    s.enter(3600, 1, do_something, (sc,))
'''
s.enter(3600, 1, Check_Input_Data, (s,))
s.enter(3600, 1, Check_Operate_Data, (s,))
s.enter(3600, 1, Check_Power_Data, (s,))
s.run()

####################################################

