from datetime import datetime
from datetime import timedelta
#import paho.mqtt.client as mqtt
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
import sched, time
s = sched.scheduler(time.time, time.sleep)


##############################################

'''
def Alert_program(sc): 
    print "Doing stuff..."
    # do your stuff
    #url='https://api.textlocal.in/send/?apikey=irlz29yH7zo-7Yxe1P8dAA7ylIwBatMOwf9xJbukjg&numbers=9705474827&sender=JTSIOT&message=Alert:%20one%20hour%20Alert'
    #serialized_data = urllib2.urlopen(url).read()
    #data = json.loads(serialized_data)
    
    #mqttc.publish('jts/alertTest/mqtt','hello this is mqtt test program')
    s.enter(3, 1, do_something, (sc,))

'''

####################################################
def DB_Alert(sc):
    s.enter(3, 1, DB_Alert, (sc,))
    try:
       db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
       cursor = db.cursor()
       sql = "select count(*) from AC_Input_Units"
       number_of_rows = cursor.execute(sql)
       print(number_of_rows)
       db.close()

    except my.DataError as e:
        print("DataError")
        print(e)

    except my.InternalError as e:
        print("InternalError")
        print(e)

    except my.IntegrityError as e:
        print("IntegrityError")
        print(e)

    except my.OperationalError as e:
        print("OperationalError")
        print(e)

    except my.NotSupportedError as e:
        print("NotSupportedError")
        print(e)

    except my.ProgrammingError as e:
        print("ProgrammingError")
        print(e)

    except :
        print("Unknown error occurred")

s.enter(3, 1, DB_Alert, (s,))
#s.enter(3, 1, Alert_program, (s,))
s.run()





