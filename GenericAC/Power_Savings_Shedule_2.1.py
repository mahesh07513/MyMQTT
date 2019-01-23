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
def Day_Once_Power_Saving():
    try:
       db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
       cursor = db.cursor()
       yesterday = datetime.now() - timedelta(days=1)
       Yesterday=yesterday.strftime('%Y-%m-%d')
       #print "date",Yesterday
       sqlq = "select i.IpId,i.UnitId,o.OpId,o.UnitId,p.PId,p.UnitId from AC_Input_Units as i left join AC_Operate_Units as o USING (UnitId) left join AC_Power_Units as p USING (UnitId)  group by i.UnitId,o.UnitId,p.UnitId  union select i.IpId,i.UnitId,o.OpId,o.UnitId,p.PId,p.UnitId  from AC_Input_Units as i right join AC_Operate_Units as o USING (UnitId) right join AC_Power_Units as p USING (UnitId) group by i.UnitId,o.UnitId,p.UnitId  union select i.IpId,i.UnitId,o.OpId,o.UnitId,p.PId,p.UnitId  from AC_Input_Units as i left join AC_Operate_Units as o USING (UnitId) right join AC_Power_Units as p USING (UnitId) group by i.UnitId,o.UnitId,p.UnitId"
       cursor.execute(sqlq)
       units = cursor.fetchall()
       #print "get all units ",units,len(units)
       unitId=''
       op=True
       po=True

       for row in units:
           #print row
           #print row[1],row[3],row[5]
           if row[1] is not None:
              unit_id=row[1]
           if row[3] is not None:
              unit_id=row[3]
              op=True
           if row[5] is not None:
              unit_id=row[5]
              po=True
           #print "unit is is :", unit_id
           if op==True and po==True:
              sqlq2="SELECT sum(AC_Power_History.Power)/1000 as avgPower FROM AC_Power_History INNER JOIN ( SELECT * FROM AC_Operate_History WHERE MotionE = '0' and OpId='%s' and ChangeDate >= CURDATE() - INTERVAL 1 DAY AND ChangeDate < CURDATE()) foo ON AC_Power_History.ChangeDate = foo.ChangeDate where AC_Power_History.PId='%s' and AC_Power_History.ChangeDate >= CURDATE() - INTERVAL 1 DAY AND AC_Power_History.ChangeDate < CURDATE()" %(row[2],row[4])
              sqlq3="SELECT count(*)as time FROM AC_Power_History INNER JOIN ( SELECT * FROM AC_Operate_History WHERE MotionE = '0' and OpId='%s' and ChangeDate >= CURDATE() - INTERVAL 1 DAY AND ChangeDate < CURDATE()) foo ON AC_Power_History.ChangeDate = foo.ChangeDate where AC_Power_History.PId='%s' and AC_Power_History.ChangeDate >= CURDATE() - INTERVAL 1 DAY AND AC_Power_History.ChangeDate < CURDATE()" %(row[2],row[4])
              sqlq4="SELECT sum(AC_Power_History.Power)/1000 as avgPower FROM AC_Power_History INNER JOIN ( SELECT * FROM AC_Operate_History WHERE MotionE = '1' and OpId='%s' and ChangeDate >= CURDATE() - INTERVAL 1 DAY AND ChangeDate < CURDATE()) foo ON AC_Power_History.ChangeDate = foo.ChangeDate where AC_Power_History.PId='%s' and AC_Power_History.ChangeDate >= CURDATE() - INTERVAL 1 DAY AND AC_Power_History.ChangeDate < CURDATE()" %(row[2],row[4])
              sqlq5="SELECT count(*)as time FROM AC_Power_History INNER JOIN ( SELECT * FROM AC_Operate_History WHERE MotionE = '1' and OpId='%s' and ChangeDate >= CURDATE() - INTERVAL 1 DAY AND ChangeDate < CURDATE()) foo ON AC_Power_History.ChangeDate = foo.ChangeDate where AC_Power_History.PId='%s' and AC_Power_History.ChangeDate >= CURDATE() - INTERVAL 1 DAY AND AC_Power_History.ChangeDate < CURDATE()" %(row[2],row[4])
              cursor.execute(sqlq2)
              sum_ne = cursor.fetchone()
              #print "not exists :  ",sum_ne
              cursor.execute(sqlq3)
              sum_ne_cnt = cursor.fetchone()
              #print "not exists count:  ",sum_ne_cnt[0]
              cursor.execute(sqlq4)
              sum_e = cursor.fetchone()
              #print "exists :  ",sum_e
              cursor.execute(sqlq5)
              sum_e_cnt = cursor.fetchone()
              #print "not exists count:  ",sum_e_cnt
              if sum_ne[0] is None :
                 mne=1
              else:
                 mne=sum_ne[0]
              if sum_ne_cnt[0] is None or sum_ne_cnt[0]==0:
                 mnc=1
              else:
                 mnc=sum_ne_cnt[0]
              if sum_e[0] is None:
                 me=1
              else:
                 me=sum_e[0]
              if sum_e_cnt[0] is None or sum_e_cnt[0]==0:
                 mec=1
              else:
                 mec=sum_e_cnt[0]
              #print "motion not exists : ",float(mne)/float(mnc)
              #print "Motion Exits : ",float(me/mec)
              nmavgpower=float(mne)/float(mnc)
              meavgpower=float(me/mec)
              if nmavgpower == 1.0:
                 NPower=0
              else:
                 NPower=nmavgpower
              if meavgpower == 1.0:
                 Power=0
              else:
                 Power=meavgpower
              #print "Motion Exist : ",Power
              #print "Motion Not Exist : ",NPower         
              #print "Power save : ",Power-NPower
              #print " Power units : ",(Power-NPower)*sum_ne_cnt[0]
              saved_units=(Power-NPower)*sum_ne_cnt[0]
              if saved_units > 0:
                 saved_units=saved_units
              else:
                 saved_units=0
              #print "Unit_id : '%s', Saved Units are : '%s'" %(unit_id,saved_units)
  
              add_rec=cursor.execute("""INSERT INTO AC_Saved_Units(UnitId,Saved,ChangeBy,ChangeDate,StatusActive) VALUES (%s,%s,%s,%s,%s)""",(unit_id,saved_units,'jts_admin',Yesterday,True))
              db.commit()
    except (AttributeError, MySQLdb.OperationalError):
        #mqttc.publish('jts/oyo/error','MySQL server has gone away')
        print "MySQL Error: MySQL server has gone away"
    except MySQLdb.Error, e:
        try:
             print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
             #mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]))
        except IndexError:
            print "MySQL Error: %s" % str(e)
            #mqttc.publish('jts/oyo/error',str(e))              
    except Exception, e:
        print '{"error_code":"3", "error_desc": "Response=Failed to add the temp"}'
        #return mqttc.publish('jts/oyo/error',output)
    except :
        print("Unknown error occurred")
        #return mqttc.publish('jts/oyo/error','Unknown error occurred') 
schedule.every().day.at("01:00").do(Day_Once_Power_Saving)
while True:
    #print "inside true"
    schedule.run_pending()
    time.sleep(1)
####################################################

