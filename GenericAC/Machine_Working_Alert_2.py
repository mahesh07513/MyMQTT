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
count=0
mac='1234'
level1='8187841817,9949359388,9603222337'
level2='9704229333'
level3='8897129611,8897134034'

'''
def check(sc):
    global count,level1,mac
    count+=1
    print "count is : ",count
    print level1
    #level3=level1[1:-1]
    #print level3
    if count==3:
       print "exceded ...."
       print level2
       count=0
    s.enter(5, 1, check, (sc,))	
    mac1=567
    if mac==mac1:
       print "equal"
    mac=mac1
    #url='https://api.textlocal.in/send/?apikey=irlz29yH7zo-7Yxe1P8dAA7ylIwBatMOwf9xJbukjg&sender=JTSIOT&message=Alert:%20test%20this&numbers='+level1
    #serialized_data = urllib2.urlopen(url).read()
    #data = json.loads(serialized_data)
    #print data
'''
def Check_Input_Data(sc):
    try: 
       global count,level1,mac,level2,level3
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
                 #pass
                 url='https://api.textlocal.in/send/?apikey=irlz29yH7zo-7Yxe1P8dAA7ylIwBatMOwf9xJbukjg&sender=JTSIOT&message=Alert:%20Name:'+name+'%20,MacId:'+macid+',Owner:'+owner+'%20Unit%20is%20not%20working%20from%20last%20One%20hour,%20Please%20check%20Once.&numbers='+level3
                 serialized_data = urllib2.urlopen(url).read()
                 data = json.loads(serialized_data)
              else:
                 #url='https://api.textlocal.in/send/?apikey=irlz29yH7zo-7Yxe1P8dAA7ylIwBatMOwf9xJbukjg&numbers=9949359388&sender=JTSIOT&message=Alert:%20Name:'+name+'%20,MacId:'+macid+',Owner:'+owner+'%20Unit%20is%20not%20working%20from%20last%20One%20hour,%20Please%20check%20Once.' 
                 url='https://api.textlocal.in/send/?apikey=irlz29yH7zo-7Yxe1P8dAA7ylIwBatMOwf9xJbukjg&sender=JTSIOT&message=Alert:%20Name:'+name+'%20,MacId:'+macid+',Owner:'+owner+'%20Unit%20is%20not%20working%20from%20last%20One%20hour,%20Please%20check%20Once.&numbers='+level1
                 serialized_data = urllib2.urlopen(url).read()
                 data = json.loads(serialized_data)
                 count+=1
                 if count==5:
                    url='https://api.textlocal.in/send/?apikey=irlz29yH7zo-7Yxe1P8dAA7ylIwBatMOwf9xJbukjg&sender=JTSIOT&message=Alert:%20Name:'+name+'%20,MacId:'+macid+',Owner:'+owner+'%20Unit%20is%20not%20working%20from%20last%20One%20hour,%20Please%20check%20Once.&numbers='+level2
                    serialized_data = urllib2.urlopen(url).read()
                    data = json.loads(serialized_data)
       cursor.close()
       db.close()
       s.enter(3600, 1, Check_Input_Data, (sc,))
    except (AttributeError, MySQLdb.OperationalError):
        print 'MySQL server has gone away'
        #return mqttc.publish('jts/oyo/error','MySQL server has gone away')
    except MySQLdb.DataError as e:
        print("DataError")
        #print(e)
        #return mqttc.publish('jts/oyo/error',str(e))

    except MySQLdb.InternalError as e:
        print("InternalError")
        #print(e)
        #return mqttc.publish('jts/oyo/error',str(e))
    except MySQLdb.IntegrityError as e:
        print("IntegrityError")
        #print(e)
        #return mqttc.publish('jts/oyo/error',str(e))
    except MySQLdb.OperationalError as e:
        print("OperationalError")
        #print(e)
        #return mqttc.publish('jts/oyo/error',str(e))
    except MySQLdb.NotSupportedError as e:
        print("NotSupportedError")
        #print(e)
        #return mqttc.publish('jts/oyo/error',str(e))
    except MySQLdb.ProgrammingError as e:
        print("ProgrammingError")
        #print(e)
        #return mqttc.publish('jts/oyo/error',str(e))

    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            #return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]))
        except IndexError:
            print "MySQL Error: %s" % str(e)
            #return mqttc.publish('jts/oyo/error',str(e))
    except Exception, e:
        print str(e) 
        #return mqttc.publish('jts/oyo/error',output)
    except :
        print("Unknown error occurred")
################################################################
def Check_Operate_Data(sc):
    try:
       global count,level1,mac,level2,level3
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
                 #pass
                 url='https://api.textlocal.in/send/?apikey=irlz29yH7zo-7Yxe1P8dAA7ylIwBatMOwf9xJbukjg&sender=JTSIOT&message=Alert:%20Name:'+name+'%20,MacId:'+macid+',Owner:'+owner+'%20Unit%20is%20not%20working%20from%20last%20One%20hour,%20Please%20check%20Once.&numbers='+level3
                 serialized_data = urllib2.urlopen(url).read()
                 data = json.loads(serialized_data)
              else:
                 url='https://api.textlocal.in/send/?apikey=irlz29yH7zo-7Yxe1P8dAA7ylIwBatMOwf9xJbukjg&sender=JTSIOT&message=Alert:%20Name:'+name+'%20,MacId:'+macid+',Owner:'+owner+'%20Unit%20is%20not%20working%20from%20last%20One%20hour,%20Please%20check%20Once.&numbers='+level1 
                 serialized_data = urllib2.urlopen(url).read()
                 data = json.loads(serialized_data)
                 count+=1
                 if count==5:
                    url='https://api.textlocal.in/send/?apikey=irlz29yH7zo-7Yxe1P8dAA7ylIwBatMOwf9xJbukjg&sender=JTSIOT&message=Alert:%20Name:'+name+'%20,MacId:'+macid+',Owner:'+owner+'%20Unit%20is%20not%20working%20from%20last%20One%20hour,%20Please%20check%20Once.&numbers='+level2
                    serialized_data = urllib2.urlopen(url).read()
                    data = json.loads(serialized_data)
       cursor.close()
       db.close()
       s.enter(3600, 1, Check_Operate_Data, (sc,))
    except (AttributeError, MySQLdb.OperationalError):
        print 'MySQL server has gone away'
        #return mqttc.publish('jts/oyo/error','MySQL server has gone away')
    except MySQLdb.DataError as e:
        print("DataError")
        #print(e)
        #return mqttc.publish('jts/oyo/error',str(e))

    except MySQLdb.InternalError as e:
        print("InternalError")
        #print(e)
        #return mqttc.publish('jts/oyo/error',str(e))
    except MySQLdb.IntegrityError as e:
        print("IntegrityError")
        #print(e)
        #return mqttc.publish('jts/oyo/error',str(e))
    except MySQLdb.OperationalError as e:
        print("OperationalError")
        #print(e)
        #return mqttc.publish('jts/oyo/error',str(e))
    except MySQLdb.NotSupportedError as e:
        print("NotSupportedError")
        #print(e)
        #return mqttc.publish('jts/oyo/error',str(e))
    except MySQLdb.ProgrammingError as e:
        print("ProgrammingError")
        #print(e)
        #return mqttc.publish('jts/oyo/error',str(e))

    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            #return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]))
        except IndexError:
            print "MySQL Error: %s" % str(e)
            #return mqttc.publish('jts/oyo/error',str(e))
    except Exception, e:
        print str(e)
        #return mqttc.publish('jts/oyo/error',output)
    except :
        print("Unknown error occurred")
########################################################
def Check_Power_Data(sc):
    try:
       global count,level1,mac,level2,level3
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
                 #pass
                 url='https://api.textlocal.in/send/?apikey=irlz29yH7zo-7Yxe1P8dAA7ylIwBatMOwf9xJbukjgsender=JTSIOT&message=Alert:%20Name:'+name+'%20,MacId:'+macid+',Owner:'+owner+'%20Unit%20is%20not%20working%20from%20last%20One%20hour,%20Please%20check%20Once.&numbers='+level3
                 serialized_data = urllib2.urlopen(url).read()
                 data = json.loads(serialized_data)
              else:
                 url='https://api.textlocal.in/send/?apikey=irlz29yH7zo-7Yxe1P8dAA7ylIwBatMOwf9xJbukjg&sender=JTSIOT&message=Alert:%20Name:'+name+'%20,MacId:'+macid+',Owner:'+owner+'%20Unit%20is%20not%20working%20from%20last%20One%20hour,%20Please%20check%20Once.&numbers='+level1 
                 serialized_data = urllib2.urlopen(url).read()
                 data = json.loads(serialized_data)
                 count+=1
                 if count==5:
                    url='https://api.textlocal.in/send/?apikey=irlz29yH7zo-7Yxe1P8dAA7ylIwBatMOwf9xJbukjg&sender=JTSIOT&message=Alert:%20Name:'+name+'%20,MacId:'+macid+',Owner:'+owner+'%20Unit%20is%20not%20working%20from%20last%20One%20hour,%20Please%20check%20Once.&numbers='+level2
                    serialized_data = urllib2.urlopen(url).read()
                    data = json.loads(serialized_data)

       cursor.close()
       db.close()
       s.enter(3600, 1, Check_Power_Data, (sc,))
    except (AttributeError, MySQLdb.OperationalError):
        print 'MySQL server has gone away'
        #return mqttc.publish('jts/oyo/error','MySQL server has gone away')
    except MySQLdb.DataError as e:
        print("DataError")
        #print(e)
        #return mqttc.publish('jts/oyo/error',str(e))

    except MySQLdb.InternalError as e:
        print("InternalError")
        #print(e)
        #return mqttc.publish('jts/oyo/error',str(e))
    except MySQLdb.IntegrityError as e:
        print("IntegrityError")
        #print(e)
        #return mqttc.publish('jts/oyo/error',str(e))
    except MySQLdb.OperationalError as e:
        print("OperationalError")
        #print(e)
        #return mqttc.publish('jts/oyo/error',str(e))
    except MySQLdb.NotSupportedError as e:
        print("NotSupportedError")
        #print(e)
        #return mqttc.publish('jts/oyo/error',str(e))
    except MySQLdb.ProgrammingError as e:
        print("ProgrammingError")
        #print(e)
        #return mqttc.publish('jts/oyo/error',str(e))

    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            #return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]))
        except IndexError:
            print "MySQL Error: %s" % str(e)
            #return mqttc.publish('jts/oyo/error',str(e))
    except Exception, e:
        print str(e)
        #return mqttc.publish('jts/oyo/error',output)
    except :
        print("Unknown error occurred")
#############################################################
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
#s.enter(5, 1, check, (s,))
#s.run()

####################################################

