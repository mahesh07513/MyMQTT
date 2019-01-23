import paho.mqtt.client as mqtt
import MySQLdb
import json
#import datetime
import sys
#import time
from datetime import datetime
import requests
####################################################
def get_data(mosq,obj,msg):
    db = MySQLdb.connect("localhost","root","root","myPrototype")
    cursor = db.cursor()
    output_str = "get_data - Unable to Authenticate/get_data... "
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('/test/Mob/data','{"error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')

    if(not data1):
        output_str += ", details are mandatory"
        output = '{"function":"get_historys","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
        return mqttc.publish('/test/Mob/data',output)



    if(data1.get('function') is None):
        output_str += ",function is mandatory"
        output = '{"function":"get_historys","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('/test/Mob/data',output)

    function    = data1['function']
    try:
        sqlq2 = "SELECT avg(AccTop)-3*STDDEV(AccTop),avg(AccTop)+3*STDDEV(AccTop),avg(AccBottom)-3*STDDEV(AccBottom),avg(AccBottom)+3*STDDEV(AccBottom) FROM SeatProto WHERE SeatProto.TimeStamp >= ( CURDATE() - INTERVAL 3 DAY )"
        #print sqlq2
        cursor.execute(sqlq2)
        get_rec = cursor.fetchall()
        #print get_rec
        if(get_rec > 0):
           topminus=get_rec[0][0]
           topplus=get_rec[0][1]
           botminus=get_rec[0][2]
           botplus=get_rec[0][3]
           #between 23 and 25
           #sqlq3="select sum(count(AccTop)) from SeatProto WHERE AccTop NOT between %s and %s and SeatProto.TimeStamp >= ( CURDATE() - INTERVAL 3 DAY) GROUP BY MINUTE(TimeStamp) " %(topminus,topplus) 
           sqlq3 = "SELECT count(AccTop) FROM SeatProto where AccTop NOT between %s and %s and SeatProto.TimeStamp >= ( CURDATE() - INTERVAL 3 DAY ) " %(topminus,topplus)
           print sqlq3
           cursor.execute(sqlq3)
           get_rec1 = cursor.fetchall()
           #sqlq4="select sum(count(AccBottom)) from SeatProto WHERE AccBottom NOT between %s and %s and SeatProto.TimeStamp >= ( CURDATE() - INTERVAL 3 DAY) GROUP BY MINUTE(TimeStamp) " %(botminus,botplus)
           sqlq4 = "SELECT count(AccBottom) FROM SeatProto where AccBottom NOT between %s and %s and SeatProto.TimeStamp >= ( CURDATE() - INTERVAL 3 DAY )" %(botminus,botplus)
           print sqlq4
           cursor.execute(sqlq4)
           get_rec2 = cursor.fetchall()
           
           if(len(get_rec1) > 0 and len(get_rec2) > 0):
              top=get_rec1[0][0]
              bottom=get_rec2[0][0]
              output='{"function":"get_data","Top":"%s","Bottom":"%s"}' %(top,bottom)
              return mqttc.publish('/test/Mob/data',output)
        #data = json.loads(web.data())
        #value = data["name"]
        #return output
        else:
           output = '{"function":"get_data,"error_code":"3", "error_desc": "Response=Failed to get the data records, NO_DATA_FOUND"}'
           return mqttc.publish('/test/Mob/data',output)    
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_data","error_code":"3", "error_desc": "Response=Failed to get the data"}' 
        return mqttc.publish('/test/Mob/data',output)
################################ get_historys #########################
def get_history(mosq,obj,msg):
    db = MySQLdb.connect("localhost","root","root","myPrototype")
    cursor = db.cursor()
    output_str = "get_history - Unable to Authenticate/get_history... "
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('/test/Mob/data','{"error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')

    if(not data1):
        output_str += ", details are mandatory"
        output = '{"function":"get_historys","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
        return mqttc.publish('/test/Mob/data',output)



    if(data1.get('function') is None):
        output_str += ",function is mandatory"
        output = '{"function":"get_historys","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('/test/Mob/data',output)

    function    = data1['function']
   
    
    try:
 
       sqlq1 = "SELECT avg(AccTop)-3*STDDEV(AccTop),avg(AccTop)+3*STDDEV(AccTop),avg(AccBottom)-3*STDDEV(AccBottom),avg(AccBottom)+3*STDDEV(AccBottom) FROM SeatProto WHERE SeatProto.TimeStamp >= ( CURDATE() - INTERVAL 3 DAY )"
       #print sqlq1
       cursor.execute(sqlq1)
       get_rec = cursor.fetchall()
       #print get_rec
       #sqlq2="select MacID,AccTop,AccBottom,Theft,TimeStamp from SeatProto WHERE d AccTop NOT between %s and %s and AccBottom NOT between %s and %s ORDER BY ID " %(get_rec[0][0],get_rec[0][1],get_rec[0][2],get_rec[0][3])
       #sqlq2="select MacID,AccTop,AccBottom,Theft,TimeStamp from SeatProto WHERE SeatProto.TimeStamp >= ( CURDATE() - INTERVAL 3 DAY ) and AccTop NOT between %s and %s and AccBottom NOT between %s and %s ORDER BY ID " %(get_rec[0][0],get_rec[0][1],get_rec[0][2],get_rec[0][3])
       #sqlq2 = "SELECT MacID,AccTop,AccBottom,Theft,TimeStamp FROM SeatProto "
       #print sqlq2
       #cursor.execute(sqlq2)
       #get_historys_rec = cursor.fetchall()
       #print len(get_historys_rec)
       sql12="select MacID,count(AccTop),TimeStamp from SeatProto WHERE AccTop NOT between %s and %s GROUP BY MINUTE(TimeStamp) ORDER BY ID" %(get_rec[0][0],get_rec[0][1])
       #print sql12
       cursor.execute(sql12)
       get_rec1 = cursor.fetchall()
       #print get_rec1
       sql13="select MacID,count(AccBottom),TimeStamp from SeatProto WHERE AccBottom NOT between %s and %s GROUP BY MINUTE(TimeStamp) ORDER BY ID" %(get_rec[0][2],get_rec[0][3])
       #print sql13
       cursor.execute(sql13)
       get_rec2 = cursor.fetchall()
       #print get_rec2
       if(len(get_rec1) > 0 or len(get_rec2) > 0):
        #{
          output = '{"function":"get_history","error_code":"0",  \n "Top_History":' 
          output += '['
          counter = 0
          for rec in get_rec1:
          #{
             #print "came",rec
             counter += 1
             if(counter == 1):
               output += '{"macID":"%s","CTop":"%s","Time":"%s"}' %(rec[0] ,rec[1],rec[2])
             else:
               output += ',\n {"macID":"%s","CTop":"%s","Time":"%s"}' %(rec[0] ,rec[1],rec[2])
          #}
          output += '],'
          #print len(get_rec2)
          output +='\n "Bottom_History":' 
          #print output
          output += '['
          counter = 0
          for rec in get_rec2:
          #{
             #print "came 2 ",rec
             counter += 1
             if(counter == 1):
               output += '{"macID":"%s","CBottom":"%s","Time":"%s"}' %(rec[0] ,rec[1],rec[2])
             else:
               output += ',\n {"macID":"%s","CBottom":"%s","Time":"%s"}' %(rec[0] ,rec[1],rec[2])
          #}
          output += ']\n'

        
          output += '}'
          print output
          cursor.close()
          db.close()
          return mqttc.publish('/test/Mob/data',output)
       else:
           output = '{"function":"get_history","error_code":"3", "error_desc": "Response=Failed to get the history records, NO_DATA_FOUND"}' 
           return mqttc.publish('/test/Mob/data',output)
      
          
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_history","error_code":"3", "error_desc": "Response=Failed to get the history"}'
        return mqttc.publish('/test/Mob/data',output)

#####################add data #################################################
def add_data(mosq,obj,msg):
    print "add_data......."
    db = MySQLdb.connect("localhost","root","root","myPrototype")
    cursor = db.cursor()
    output_str = "add_data - Unable to Authenticate/add_data... " 
    #print "this is register string : ",str(msg.payload)
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('/test/error/','{"function":"add_data","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       

       if((data1.get('ipmacid') is None) or ((data1.get('ipmacid') is not  None) and (len(data1['ipmacid']) <= 0))):
           output_str += ", ipmacid is mandatory"
           output = '{"function":"add_data","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           return mqttc.publish('/test/error/',output)
       else:
           ipmacid = data1['ipmacid'] 
           		 
       if((data1.get('topAcc') is None) or ((data1.get('topAcc') is not  None) and (len(data1['topAcc']) <= 0))):
           output_str += ", topAcc is mandatory"
           output = '{"function":"add_data","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           return mqttc.publish('/test/error/',output)
       else:
           topAcc = data1['topAcc'] 
           
       if((data1.get('bottomAcc') is None) or ((data1.get('bottomAcc') is not  None) and (len(data1['bottomAcc']) <= 0))):
           output_str += ", bottomAcc is mandatory"
           output = '{"function":"add_data","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           return mqttc.publish('/test/error/',output)
       else:
           bottomAcc = data1['bottomAcc']
           
       if((data1.get('Theft') is None) or ((data1.get('Theft') is not  None) and (len(data1['Theft']) <= 0))):
           output_str += ", Theft is mandatory"
           output = '{"function":"add_data","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
           return mqttc.publish('/test/error/',output)
       else:
           Theft = data1['Theft']
          

       
       
       add_rec1=cursor.execute("""INSERT INTO SeatProto(MacID,AccTop,AccBottom,Theft,TimeStamp) VALUES (%s,%s,%s,%s,%s)""",(ipmacid,topAcc,bottomAcc,Theft,date))
       db.commit()
       if add_rec1 > 0:
          #print 'data inserted'
          return mqttc.publish('/test/error/','{"error_code":"0","Response":"Successfully added"}')
       else:
          #print 'unable to insert'
          return mqttc.publish('/test/error/','{"error_code":"2","Response":"unable to add data"}')
             

    except MySQLdb.Error, e:
        try:
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish('/test/error/',str(e.args[0])+str(e.args[1]))
        except IndexError:
            #print "MySQL Error: %s" % str(e)
            return mqttc.publish('/test/error/',str(e)) 
       
    except Exception, e:  
        cursor.close()
        db.close()
        output = '{"function":"add_data","error_code":"3", "error_desc": "Response=Failed to add the data"}' %(sid) 
        return mqttc.publish('/test/error/',output)

################## publish response #################################################
def on_publish(client, userdata, result):
        pass
        #print "data published \n"

def on_connect(client, userdata, flags, rc):
    #print("Connected with result code "+str(rc))
    mqttc.subscribe("jts/Ser/#")
def on_disconnect(client, userdata, rc):
    if rc != 0:
        print "Unexpected MQTT disconnection. Will auto-reconnect"
        

######################### mqtt methods ####################################
mqttc = mqtt.Client()
mqttc.message_callback_add('/test/Ser/Data',get_history)
mqttc.message_callback_add('/test/Ser/get_data',get_data)
mqttc.message_callback_add('/test/e2s/data',add_data)
mqttc.on_publish = on_publish
mqttc.on_disconnect = on_disconnect
mqttc.on_connect = on_connect
mqttc.username_pw_set('esp', 'ptlesp01')
mqttc.connect("cld003.jts-prod.in", 1883, 60)
mqttc.subscribe("/test/#")
mqttc.loop_forever()





