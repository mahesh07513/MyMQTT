import paho.mqtt.client as mqtt
import MySQLdb
import json
#import datetime
import sys
#import time
from datetime import datetime
import requests
####################################################


###################### getTemparature from OP units ##################
def Unit_Details(mosq,obj,msg):
    print "Temp........ "
    db = MySQLdb.connect("localhost","urbankisan","ptljtsurbankisan","UrbanKisanDB")
    cursor = db.cursor()
    output_str = "Unit_Details - Unable to Authenticate/Unit_Details... " 
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/eMeter/response','{"error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}' )
    try:
       
       if((data1.get('macid') is None) or ((data1.get('macid') is not  None) and (len(data1['macid']) <= 0))):
           output_str += ", macid is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/eMeter/response',output)
       else:
           macid = data1['macid']

       if((data1.get('temp') is None) or ((data1.get('temp') is not  None) and (len(data1['temp']) <= 0))):
           output_str += ", temp is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/eMeter/response',output)
       else:
           temp = data1['temp'] 
 
       if((data1.get('wL') is None) or ((data1.get('wL') is not  None) and (len(data1['wL']) <= 0))):
           output_str += ", wL is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/eMeter/response',output)
       else:
           waterlevel = data1['wL'] 

          
       if((data1.get('pH') is None) or ((data1.get('pH') is not  None) and (len(data1['pH']) <= 0))):
           output_str += ", pH is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/eMeter/response',output)
       else:
           ph = data1['pH']

       if((data1.get('Ec') is None) or ((data1.get('Ec') is not  None) and (len(data1['Ec']) <= 0))):
           output_str += ", Ec is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/eMeter/response',output)
       else:
           Ec = data1['Ec']

       if((data1.get('ppM') is None) or ((data1.get('ppM') is not  None) and (len(data1['ppM']) <= 0))):
           output_str += ", ppM is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/eMeter/response',output)
       else:
           ppM = data1['ppM'] 

       sqlq1 = "SELECT UnitId FROM UK_Unit_Params WHERE MacId='%s'" %(macid)
       cursor.execute(sqlq1)
       results = cursor.fetchone()
       print "checking in opreate unit ..... :",results
       if results > 0:
          upd_rec=cursor.execute ("""UPDATE UK_Unit_Params SET Temp=%s,WaterLevel=%s,Ph=%s,EC=%s,Ppm=%s,ChangeDate=%s WHERE UnitId=%s""", (temp,waterlevel,ph,Ec,ppM,date,results[0]))
          db.commit()
          add_rec2=cursor.execute("""INSERT INTO UK_Unit_History(UnitId,MacId,Temp,WaterLevel,Ph,EC,Ppm,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",(results[0],macid,temp,waterlevel,ph,Ec,ppM,date))
          db.commit()
          if upd_rec > 0 and add_rec2 > 0 :
             output = '{"error_code":"0", "Response":"Succesfully updated Values"}' 
             cursor.close()
             db.close()
             return mqttc.publish('jts/eMeter/response',output)
          else:
             output = '{"error_code":"2", "error_desc": "Response=Unable to update Values"}'
             cursor.close()
             db.close()
             return mqttc.publish('jts/eMeter/response',output)
       else:
           #print "inserting ........."
           add_rec1=cursor.execute("""INSERT INTO UK_Unit_Params(MacId,Temp,WaterLevel,Ph,EC,Ppm,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s)""",(macid,temp,waterlevel,ph,Ec,ppM,date))
           db.commit()
           if add_rec1 > 0:
              sqlq1 = "SELECT UnitId FROM UK_Unit_Params WHERE MacId='"+macid+"'"
              cursor.execute(sqlq1)
              results3 = cursor.fetchone()
              if results3 > 0 :
                 add_rec2=cursor.execute("""INSERT INTO UK_Unit_History(UnitId,MacId,Temp,WaterLevel,Ph,EC,Ppm,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",(results3[0],macid,temp,waterlevel,ph,Ec,ppM,date))
                 db.commit()
                 if add_rec2>0:
                    output = '{"error_code":"0", "Response":"Succesfully added temp"}'
                    cursor.close()
                    db.close()
                    return mqttc.publish('jts/eMeter/response',output) 
               
           
       
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish('jts/eMeter/response',str(e.args[0])+str(e.args[1]))
        except IndexError:
            print "MySQL Error: %s" % str(e)
            return mqttc.publish('jts/eMeter/response',str(e)) 
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"error_code":"3", "error_desc": "Response=Failed to add the temp"}'
        return mqttc.publish('jts/eMeter/response',output)





################## publish response #################################################
def on_publish(client, userdata, result):
        print "data published \n"


def on_disconnect(client, userdata, rc):
    if rc != 0:
        print "Unexpected MQTT disconnection. Will auto-reconnect"
        mqttc.subscribe("jts/uKissan/#")

######################### mqtt methods ####################################
mqttc = mqtt.Client()
#################### esp calls ####################################

mqttc.message_callback_add('jts/uKissan/status',Unit_Details)


mqttc.on_publish = on_publish
mqttc.username_pw_set('esp', 'ptlesp01')
mqttc.connect("cld003.jts-prod.in", 1883, 60)
mqttc.subscribe("jts/uKissan/#")
mqttc.loop_forever()
#mqttc.username_pw_set('esp', 'ptlesp01')





