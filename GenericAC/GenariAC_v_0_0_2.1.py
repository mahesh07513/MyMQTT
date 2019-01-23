from datetime import datetime
from datetime import timedelta
import paho.mqtt.client as mqtt
import MySQLdb
import json
#import datetime
import sys
import time
import requests
import string
import random
import urllib2
###################### update sensor units value lora ##################
def update_sensor_data_lora(mosq,obj,msg):
    #print "update_sensor_data........ "
    
    output_str = "update_sensor_data - Unable to Authenticate/update_sensor_data... " 
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M')
    #print "time",date
    #date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data0 = json.loads(msg.payload)
       try:
      
          if((data0.get('Data') is None) or ((data0.get('Data') is not  None) and (len(data0['Data']) <= 0))):
              output_str += ", Data is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              return mqttc.publish('jts/oyo/error',output)
          else:
              data1 = data0['Data']

       except Exception, e:
           output = '{"error_code":"3", "error_desc": "Response=Failed to add lora data"}'
           return mqttc.publish('jts/oyo/error',output)
       #data1 = data0['Data']
       #print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}' )
    try:
       db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
       cursor = db.cursor() 
       imac=False
       omac=False
       pmac=False  
       

       if((data1.get('macid') is None) or ((data1.get('macid') is not  None) and (len(data1['macid']) <= 0))):
           output_str += ", macid is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           macid = data1['macid']

           sqlq1 = "SELECT * FROM AC_Operate_Units WHERE OpMacid='"+macid+"'"
           cursor.execute(sqlq1)
           results = cursor.fetchone()
           if results > 0 :
              omac=True
           else:
              sqlq2 = "SELECT * FROM AC_Input_Units WHERE IpMacid='"+macid+"'"
              cursor.execute(sqlq2)
              results1 = cursor.fetchone()
              if results1 > 0 :
                 imac=True
              else:
                 sqlq2 = "SELECT * FROM AC_Power_Units WHERE PMacid='"+macid+"'"
                 cursor.execute(sqlq2)
                 results2 = cursor.fetchone()
                 if results2 > 0 :
                    pmac=True
      
       #print imac,omac,pmac,macid
       if imac==False and omac==False and pmac==False:
          output = '{"error_code":"2", "error_desc": "Response= macid is neither input,operate nor power unit.please Configure it."}'
          return mqttc.publish('jts/oyo/error',output)

       if omac == True:
          #print "this in op mac id " 
          if((data1.get('Fan') is None) or ((data1.get('Fan') is not  None) and (len(data1['Fan']) <= 0))):
              output_str += ", Fan is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              return mqttc.publish('jts/oyo/error',output)
          else:
             fan = data1['Fan'] 
 
          if((data1.get('Status') is None) or ((data1.get('Status') is not  None) and (len(data1['Status']) <= 0))):
              output_str += ", Status is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              return mqttc.publish('jts/oyo/error',output)
          else:
             status = data1['Status'] 

          
          if((data1.get('setTemp') is None) or ((data1.get('setTemp') is not  None) and (len(data1['setTemp']) <= 0))):
              output_str += ", settemp is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              return mqttc.publish('jts/oyo/error',output)
          else:
             settemp = data1['setTemp']

          if((data1.get('V') is None) or ((data1.get('V') is not  None) and (len(data1['V']) <= 0))):
              output_str += ", V is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              return mqttc.publish('jts/oyo/error',output)
          else:
             voltage = data1['V']

          if((data1.get('P') is None) or ((data1.get('P') is not  None) and (len(data1['P']) <= 0))):
              output_str += ", P is  mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              return mqttc.publish('jts/oyo/error',output)
          else:
             power = data1['P']

          if((data1.get('ME') is None) or ((data1.get('ME') is not  None) and (len(data1['ME']) <= 0))):
              output_str += ", ME is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              return mqttc.publish('jts/oyo/error',output)
          else:
              me = data1['ME'] 

          if((data1.get('I') is None) or ((data1.get('I') is not  None) and (len(data1['I']) <= 0))):
              output_str += ", I is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              return mqttc.publish('jts/oyo/error',output)
          else:
              current = data1['I']

          sqlq1 = "SELECT OpId,ChangeBy,UnitId FROM AC_Operate_Units WHERE OpMacid='"+macid+"'"
          #sqlq = "SELECT * FROM Oyo_Users"
          #print "Checking input mac id ....... :",sqlq1
          cursor.execute(sqlq1)
          results = cursor.fetchone()
          #print "checking in opreate unit ..... :",results
          if len(results) > 0:
             unitid=results[0]
             #cid=results[1]
             #pid=results[2]
             #fid=results[3]
             #rid=results[4]
             changeby=results[1]
             uid=results[2]
             sql1="UPDATE AC_Operate_Units SET Fan='%s',Status='%s',SetTemp='%s',Voltage='%s',Power='%s',Current='%s',MotionE='%s',ChangeDate='%s',ChangeBy='%s' WHERE OpId='%s'" %(fan,status,settemp,voltage,power,current,me,date,changeby,unitid)
             #upd_rec=cursor.execute ("""UPDATE AC_Operate_Units SET Fan='%s',Status='%s',SetTemp='%s',Voltage='%s',Power='%s',Current='%s',MotionE='%s',ChangeDate='%s',ChangeBy='%s' WHERE OpId='%s'""", (fan,status,settemp,voltage,power,current,me,date,changeby,unitid))
             upd_rec=cursor.execute(sql1)
             db.commit()
             #print upd_rec
             add_rec2=cursor.execute("""INSERT INTO AC_Operate_History(OpId,OpMacid,Fan,Status,SetTemp,Voltage,Power,Current,MotionE,ChangeBy,ChangeDate,UnitId) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(unitid,macid,fan,status,settemp,voltage,power,current,me,changeby,date,uid))
             db.commit()
             #print add_rec2
             if len(str(upd_rec)) > 0 and len(str(add_rec2)):
                #print "inside"
                output = '{"error_code":"0", "Response":"Succesfully updated Operate unit details"}' 
                
                
                return mqttc.publish('jts/oyo/error',output)
             else:
                output = '{"error_code":"2", "error_desc": "Response=Unable to update Operate unit details"}'
                
                
                return mqttc.publish('jts/oyo/error',output)
    
          else:
             #print 'mac id not avilable , so configure....Thanks.'
             output = '{"error_code":"2", "error_desc": "Response= macid not exists.please Configure it."}'
             return mqttc.publish('jts/oyo/error',output)         

 
       if imac == True :
          #print "this in ip mac id "
          if((data1.get('temp') is None) or ((data1.get('temp') is not  None) and (len(data1['temp']) <= 0))):
              output_str += ", temp is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              return mqttc.publish('jts/oyo/error',output)
          else:
             
             if data1.get('temp')=='-127.0':
                output_str += ", temp is  wrong"
                output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
                return mqttc.publish('jts/oyo/error',output)
             else:
                temp = data1['temp']
             
             #temp = data1['temp']
       
          if(data1.get('motion') is None):
              output_str += ", motion is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              return mqttc.publish('jts/oyo/error',output)
          else:
             motion = data1['motion']
 
          sqlq1 = "SELECT IpId,ChangeBy,IpName,UnitId FROM AC_Input_Units WHERE IpMacid='"+macid+"'"
          cursor.execute(sqlq1)
          results = cursor.fetchone()
          #print "dsfdsfds : ",results,temp
          if len(results) > 0:
             unitid=results[0]
             changeby=results[1]
             refrige=results[2]
             uid=results[3]
             #print refrige
             #if temp > 30 and refrige=="Refrigerator" and changeby=="production":
                #print "inside sms "
                #url = 'https://api.textlocal.in/send/?apikey=irlz29yH7zo-7Yxe1P8dAA7ylIwBatMOwf9xJbukjg&numbers=9133156641&sender=JTSIOT&message=Alert:%20Your%20Temparature%20is%20too%20high/low,please%20check.'
	        #serialized_data = urllib2.urlopen(url).read()
                #data = json.loads(serialized_data)
             #   pass
             #cid=results[1]
             #pid=results[2]
             #fid=results[3]
             #rid=results[4]
             #print "came ............"
             sqlq2="UPDATE AC_Input_Units SET Temp='%s',Motion='%s' ,ChangeDate='%s' WHERE IpId='%s'" %(temp,motion,date,unitid)
             #print sqlq2
             #upd_rec=cursor.execute ("""UPDATE AC_Input_Units SET Temp=%s,Motion=%s WHERE IpId=%s""", (temp,motion,unitid))
             upd_rec=cursor.execute(sqlq2)
       	     db.commit()
             #print 'update ' ,upd_rec
             add_rec2=cursor.execute("""INSERT INTO AC_Input_History(IpId,IpMacid,Temp,Motion,ChangeBy,ChangeDate,UnitId) VALUES (%s,%s,%s,%s,%s,%s,%s)""",(unitid,macid,temp,motion,changeby,date,uid))
             db.commit()
             #print 'add his ',add_rec2
             if len(str(upd_rec)) > 0 and len(str(add_rec2)) > 0:
                #print "inside if"
                output = '{"error_code":"0", "Response":"Succesfully updated temp"}'
                
                
                return mqttc.publish('jts/oyo/error',output)
             else:
                output = '{"error_code":"2", "error_desc": "Response=Unable to update temp"}'
                
                
                return mqttc.publish('jts/oyo/error',output)
             
             
          else:
             #print 'mac id not avilable , so configure....Thanks.'
             output = '{"error_code":"2", "error_desc": "Response= input unit not exists.please Configure it."}'
             return mqttc.publish('jts/oyo/error',output)

       if pmac == True:
          

          if((data1.get('V') is None) or ((data1.get('V') is not  None) and (len(data1['V']) <= 0))):
              output_str += ", V is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              return mqttc.publish('jts/oyo/error',output)
          else:
             voltage = data1['V']

          if((data1.get('P') is None) or ((data1.get('P') is not  None) and (len(data1['P']) <= 0))):
              output_str += ", P is  mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              return mqttc.publish('jts/oyo/error',output)
          else:
             power = data1['P']


          if((data1.get('I') is None) or ((data1.get('I') is not  None) and (len(data1['I']) <= 0))):
              output_str += ", I is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              return mqttc.publish('jts/oyo/error',output)
          else:
              current = data1['I']
          '''
          if((data1.get('Time') is None) or ((data1.get('Time') is not  None) and (len(data1['Time']) <= 0))):
              date=date
              
              #output_str += ", I is mandatory"
              #output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              #return mqttc.publish('jts/oyo/error',output)
          else:
              date1 = data1['Time']
          ''' 
          sqlq1 = "SELECT PId,ChangeBy,UnitId FROM AC_Power_Units WHERE PMacid='"+macid+"'"
          #sqlq = "SELECT * FROM Oyo_Users"
          #print "Checking input mac id ....... :",sqlq1
          cursor.execute(sqlq1)
          results = cursor.fetchone()
          #print "checking in opreate unit ..... :",results
          if len(results) > 0:
             unitid=results[0]
             #cid=results[1]
             #pid=results[2]
             #fid=results[3]
             #rid=results[4]
             changeby=results[1]
             uid=results[2]
             upd_rec=cursor.execute ("""UPDATE AC_Power_Units SET Voltage='%s',Power='%s',Current='%s',ChangeDate='%s',ChangeBy='%s' WHERE PId='%s'""", (voltage,power,current,date,changeby,unitid))
             db.commit()
             add_rec2=cursor.execute("""INSERT INTO AC_Power_History(PId,PMacid,Voltage,Power,Current,ChangeBy,ChangeDate,UnitId) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",(unitid,macid,voltage,power,current,changeby,date,uid))
             db.commit()
             if len(str(upd_rec)) > 0 and len(str(add_rec2)):
                output = '{"error_code":"0", "Response":"Succesfully updated Power"}'
                topic1 = 'jts/GenAC/v_0_0_2/Data/%s' %(macid) 
                output1 = '{"sm":"Server","dm":"%s","topic":"%s","Data":{"error_code":"0", "Response":"Succesfully updated Power"}}' %(macid,topic1)
		topic1 = 'jts/GenAC/v_0_0_2/Data/%s' %(macid)
       		mqttc.publish(topic1,output1)                       
                return mqttc.publish('jts/oyo/error',output)
             else:
                output = '{"error_code":"2", "error_desc": "Response=Unable to update Power"}'
                
                
                return mqttc.publish('jts/oyo/error',output)
    
          else:
             #print 'mac id not avilable , so configure....Thanks.'
             output = '{"error_code":"2", "error_desc": "Response= macid not exists.please Configure it."}'
             return mqttc.publish('jts/oyo/error',output)         

    except (AttributeError, MySQLdb.OperationalError):
        return mqttc.publish('jts/oyo/error','MySQL server has gone away') 
    except MySQLdb.DataError as e:
        #print("DataError")
        #print(e)
        return mqttc.publish('jts/oyo/error',str(e))

    except MySQLdb.InternalError as e:
        #print("InternalError")
        #print(e)
	return mqttc.publish('jts/oyo/error',str(e))
    except MySQLdb.IntegrityError as e:
        #print("IntegrityError")
        #print(e)
	return mqttc.publish('jts/oyo/error',str(e))
    except MySQLdb.OperationalError as e:
        #print("OperationalError")
        #print(e)
	return mqttc.publish('jts/oyo/error',str(e))
    except MySQLdb.NotSupportedError as e:
        #print("NotSupportedError")
        #print(e)
	return mqttc.publish('jts/oyo/error',str(e))
    except MySQLdb.ProgrammingError as e:
        #print("ProgrammingError")
        #print(e)
	return mqttc.publish('jts/oyo/error',str(e))
   
    except MySQLdb.Error, e:
        try:
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]))
        except IndexError:
            #print "MySQL Error: %s" % str(e)
            return mqttc.publish('jts/oyo/error',str(e)) 
    except Exception, e:
        output = '{"error_code":"3", "error_desc": "Response=Failed to add the temp"}'
        return mqttc.publish('jts/oyo/error',output)
    except :
        #print("Unknown error occurred")
        return mqttc.publish('jts/oyo/error','Unknown error occurred')






################## publish response #################################################
def on_publish(client, userdata, result):
        #print "data published \n"
        pass

def on_connect(client, userdata, flags, rc):
    #print("Connected with result code "+str(rc))
    mqttc.subscribe("jts/GenAC/v_0_0_2/Data/#")
def on_disconnect(client, userdata, rc):
    if rc != 0:
        print "Unexpected MQTT disconnection. Will auto-reconnect"
        #mqttc.subscribe("jts/oyo/#")

######################### mqtt methods ####################################
mqttc = mqtt.Client()
########################### version 2 API's ##############################

mqttc.message_callback_add('jts/GenAC/v_0_0_2/Data/2s',update_sensor_data_lora)
mqttc.on_publish = on_publish
mqttc.username_pw_set('esp', 'ptlesp01')
mqttc.connect("cld003.jts-prod.in", 1883, 60)
mqttc.on_disconnect = on_disconnect
mqttc.on_connect = on_connect
mqttc.subscribe("jts/GenAC/v_0_0_2/Data/#")
mqttc.loop_forever()





