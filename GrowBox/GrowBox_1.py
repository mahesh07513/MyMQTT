import paho.mqtt.client as mqtt
import MySQLdb
import json
#import datetime
import sys
#import time
from datetime import datetime
import requests
####################################################


###################### getTemparature from OP units for Grow Box ##################
def grow_data(mosq,obj,msg):
    #print "Temp........ "
    db = MySQLdb.connect("localhost","growbox","ptlgrowbox01","GrowBoxDB")
    cursor = db.cursor()
    output_str = "Unit_Details - Unable to Authenticate/Unit_Details... " 
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/growbox/response','{"error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}' )
    try:
       
       if((data1.get('ipmacid') is None) or ((data1.get('ipmacid') is not  None) and (len(data1['ipmacid']) <= 0))):
           output_str += ", ipmacid is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/growbox/response',output)
       else:
           macid = data1['ipmacid']

       if((data1.get('chT1') is None) or ((data1.get('chT1') is not  None) and (len(data1['chT1']) <= 0))):
           output_str += ", chT1 is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/growbox/response',output)
       else:
           chT1 = data1['chT1'] 
 
       if((data1.get('chT2') is None) or ((data1.get('chT2') is not  None) and (len(data1['chT2']) <= 0))):
           output_str += ", chT2 is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/growbox/response',output)
       else:
           chT2 = data1['chT2'] 

       if((data1.get('chT3') is None) or ((data1.get('chT3') is not  None) and (len(data1['chT3']) <= 0))):
           output_str += ", chT3 is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/growbox/response',output)
       else:
           chT3 = data1['chT3']

       if((data1.get('chH1') is None) or ((data1.get('chH1') is not  None) and (len(data1['chH1']) <= 0))):
           output_str += ", chH1 is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/growbox/response',output)
       else:
           chH1 = data1['chH1']

       if((data1.get('chH2') is None) or ((data1.get('chH2') is not  None) and (len(data1['chH2']) <= 0))):
           output_str += ", chH2 is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/growbox/response',output)
       else:
           chH2 = data1['chH2']      

       if((data1.get('chH3') is None) or ((data1.get('chH3') is not  None) and (len(data1['chH3']) <= 0))):
           output_str += ", chH3 is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/growbox/response',output)
       else:
           chH3 = data1['chH3']
          
       if((data1.get('chillC') is None) or ((data1.get('chillC') is not  None) and (len(data1['chillC']) <= 0))):
           output_str += ", chillC is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/growbox/response',output)
       else:
           chillC = data1['chillC']

       sqlq1 = "SELECT GBId FROM GB_GrowData WHERE Macid='%s'" %(macid)
       cursor.execute(sqlq1)
       results = cursor.fetchone()
       #print "checking in opreate unit ..... :",results
       if results > 0:
          upd_rec=cursor.execute ("""UPDATE GB_GrowData SET Chamber1Temp=%s,Chamber2Temp=%s,Chamber3Temp=%s,Chamber1Humi=%s,Chamber2Humi=%s,Chamber3Humi=%s,ChillerTemp=%s,ChangeDate=%s WHERE GBId=%s""", (chT1,chT2,chT3,chH1,chH2,chH3,chillC,date,results[0]))
          db.commit()
          add_rec2=cursor.execute("""INSERT INTO GB_GrowData_History(Macid,Chamber1Temp,Chamber2Temp,Chamber3Temp,Chamber1Humi,Chamber2Humi,Chamber3Humi,ChillerTemp,GBId,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(macid,chT1,chT2,chT3,chH1,chH2,chH3,chillC,results[0],date))
          db.commit()
          if upd_rec > 0 and add_rec2 > 0 :
             output = '{"error_code":"0", "Response":"Succesfully updated Values"}' 
             cursor.close()
             db.close()
             return mqttc.publish('jts/growbox/response',output)
          else:
             output = '{"error_code":"2", "error_desc": "Response=Unable to update Values"}'
             cursor.close()
             db.close()
             return mqttc.publish('jts/growbox/response',output)
       else:
           #print "inserting ........."
           add_rec1=cursor.execute("""INSERT INTO GB_GrowData(Macid,Chamber1Temp,Chamber2Temp,Chamber3Temp,Chamber1Humi,Chamber2Humi,Chamber3Humi,ChillerTemp,ChangeDate,StatusActive) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(macid,chT1,chT2,chT3,chH1,chH2,chH3,chillC,date,True))
           db.commit()
           if add_rec1 > 0:
              sqlq1 = "SELECT GBId FROM GB_GrowData WHERE Macid='"+macid+"'"
              cursor.execute(sqlq1)
              results3 = cursor.fetchone()
              if results3 > 0 :
                 add_rec2=cursor.execute("""INSERT INTO GB_GrowData_History(Macid,Chamber1Temp,Chamber2Temp,Chamber3Temp,Chamber1Humi,Chamber2Humi,Chamber3Humi,ChillerTemp,GBId,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(macid,chT1,chT2,chT3,chH1,chH2,chH3,chillC,results3[0],date))
                 db.commit()
                 if add_rec2>0:
                    output = '{"error_code":"0", "Response":"Succesfully updated data"}'
                    cursor.close()
                    db.close()
                    return mqttc.publish('jts/growbox/response',output) 
               
           
       
    except MySQLdb.Error, e:
        try:
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish('jts/growbox/response',str(e.args[0])+str(e.args[1]))
        except IndexError:
            #print "MySQL Error: %s" % str(e)
            return mqttc.publish('jts/growbox/response',str(e)) 
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"error_code":"3", "error_desc": "Response=Failed to add the temp"}'
        return mqttc.publish('jts/growbox/response',output)


###################### getTemparature from OP units for BLE ##################
def ble_data(mosq,obj,msg):
    #print " ble Temp........ "
    db = MySQLdb.connect("localhost","growbox","ptlgrowbox01","GrowBoxDB")
    cursor = db.cursor()
    output_str = "Unit_Details - Unable to Authenticate/Unit_Details... " 
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/ble/response','{"error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}' )
    try:
       
       if((data1.get('NodeMac') is None) or ((data1.get('NodeMac') is not  None) and (len(data1['NodeMac']) <= 0))):
           output_str += ", NodeMac is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/ble/response',output)
       else:
           macid = data1['NodeMac']

       if((data1.get('Temp') is None) or ((data1.get('Temp') is not  None) and (len(data1['Temp']) <= 0))):
           output_str += ", Temp is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/ble/response',output)
       else:
           temp = data1['Temp'] 
 
       if((data1.get('NodeRssi') is None) or ((data1.get('NodeRssi') is not  None) and (len(data1['NodeRssi']) <= 0))):
           output_str += ", NodeRssi is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/ble/response',output)
       else:
           Rssi = data1['NodeRssi'] 

       sqlq1 = "SELECT BId FROM BLE_Data WHERE Macid='%s'" %(macid)
       cursor.execute(sqlq1)
       results = cursor.fetchone()
       #print "checking in opreate unit ..... :",results
       if results > 0:
          upd_rec=cursor.execute ("""UPDATE BLE_Data SET Temp=%s,Rssi=%s,ChangeDate=%s WHERE BId=%s""", (temp,Rssi,date,results[0]))
          db.commit()
          add_rec2=cursor.execute("""INSERT INTO BLE_Data_History(Macid,Temp,Rssi,BId,ChangeDate) VALUES (%s,%s,%s,%s,%s)""",(macid,temp,Rssi,results[0],date))
          db.commit()
          if upd_rec > 0 and add_rec2 > 0 :
             output = '{"error_code":"0", "Response":"Succesfully updated Values"}' 
             cursor.close()
             db.close()
             return mqttc.publish('jts/ble/response',output)
          else:
             output = '{"error_code":"2", "error_desc": "Response=Unable to update Values"}'
             cursor.close()
             db.close()
             return mqttc.publish('jts/ble/response',output)
       else:
           #print "inserting ........."
           add_rec1=cursor.execute("""INSERT INTO BLE_Data(Macid,Temp,Rssi,ChangeDate,StatusActive) VALUES (%s,%s,%s,%s,%s)""",(macid,temp,Rssi,date,True))
           db.commit()
           if add_rec1 > 0:
              sqlq1 = "SELECT BId FROM BLE_Data WHERE Macid='"+macid+"'"
              cursor.execute(sqlq1)
              results3 = cursor.fetchone()
              if results3 > 0 :
                 add_rec2=cursor.execute("""INSERT INTO BLE_Data_History(Macid,Temp,Rssi,BId,ChangeDate) VALUES (%s,%s,%s,%s,%s)""",(macid,temp,Rssi,results3[0],date))
                 db.commit()
                 if add_rec2>0:
                    output = '{"error_code":"0", "Response":"Succesfully updated data"}'
                    cursor.close()
                    db.close()
                    return mqttc.publish('jts/ble/response',output) 
 
       
    except MySQLdb.Error, e:
        try:
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish('jts/ble/response',str(e.args[0])+str(e.args[1]))
        except IndexError:
            #print "MySQL Error: %s" % str(e)
            return mqttc.publish('jts/ble/response',str(e)) 
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"error_code":"3", "error_desc": "Response=Failed to add the temp"}'
        return mqttc.publish('jts/ble/response',output)





################## publish response #################################################
def on_publish(client, userdata, result):
        #print "data published \n"
        pass


def on_disconnect(client, userdata, rc):
    if rc != 0:
        print "Unexpected MQTT disconnection. Will auto-reconnect"
        mqttc.subscribe("/test/e2s/#")

######################### mqtt methods ####################################
mqttc = mqtt.Client()
#################### esp calls ####################################

mqttc.message_callback_add('/test/e2s/data',grow_data)
mqttc.message_callback_add('/test/e2s/ble',ble_data)

mqttc.on_publish = on_publish
mqttc.username_pw_set('esp', 'ptlesp01')
mqttc.connect("cld003.jts-prod.in", 1883, 60)
mqttc.subscribe("/test/e2s/#")
mqttc.loop_forever()
#mqttc.username_pw_set('esp', 'ptlesp01')





