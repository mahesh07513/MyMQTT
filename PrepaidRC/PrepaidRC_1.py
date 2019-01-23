import paho.mqtt.client as mqtt
import MySQLdb
import json
#import datetime
import sys
#import time
from datetime import datetime
import requests
################ add meters ###########################
def add_meter(mosq,obj,msg):
    print "add_meter......."
    #print "this is add_meters : ",str(msg.payload)
    db = MySQLdb.connect("localhost","prepaid","ptljtsprepaid","PrepaidRCDB")
    cursor = db.cursor()
    output_str = "add_meter - Unable to Authenticate/add_meter... " 
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/prepaid/response','{"function":"add_meter","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')

    if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"add_meter","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/prepaid/response',output)

    sid = data1['session_id']


    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"function":"add_meter","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/prepaid/response',output)

    if(data1.get('password') is None):
       output_str += ",passsword is mandatory"
       output = '{"function":"add_meter","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
       return mqttc.publish('jts/prepaid/response',output)

    username    = data1['username']
    password    = data1['password']
    sqlq = "SELECT * FROM Prepaid_Users WHERE Username='"+username+"' AND Password='"+password+"'"
    #sqlq = "SELECT * FROM Oyo_Users"
    print "Checking Credentials : : ",sqlq
    cursor.execute(sqlq)
    results = cursor.fetchone()
    print "Checking Data exists or not : ",results
    if results > 0:
       pass
       print 'Login Data Existed'
    else:
       #print 'Login Data not authorized so quiting ........Thanks'
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to add the meter"
       output = '{"function":"add_meter","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
       return mqttc.publish('jts/prepaid/response',output)
    try:

           
       if((data1.get('macid') is None) or ((data1.get('macid') is not  None) and (len(data1['macid']) <= 0))):
           output_str += ", macid is mandatory"
           output = '{"function":"add_meter","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           return mqttc.publish('jts/prepaid/response',output)
       else:
           macid = data1['macid'] 

       if((data1.get('Usage') is None) or ((data1.get('Usage') is not  None) and (len(data1['Usage']) <= 0))):
           output_str += ", Usage is mandatory"
           output = '{"function":"add_meter","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           return mqttc.publish('jts/prepaid/response',output)
       else:
           usage = data1['Usage'] 
 
       

       sqlq1 = "SELECT * FROM Prepaid_Meters WHERE MacID='"+macid+"'"
       #sqlq = "SELECT * FROM Oyo_Users"
       print "Checking input mac id ....... :",sqlq1
       cursor.execute(sqlq1)
       results = cursor.fetchone()
       print "checking in opreate unit ..... :",results
       if results > 0:
          output_str = "meter Already Exits"
          output = '{"function":"add_meter","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
          return mqttc.publish('jts/prepaid/response',output)
       else:
          print "inserting ........."
#          add_rec1=cursor.execute("""INSERT INTO Prepaid_Meters(MacID,ChangeBy,ChangeDate) VALUES (%s,%s,%s)""",(macid,username,date))
          add_rec1=cursor.execute("""INSERT INTO Prepaid_Meters(MacID,Units,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s)""",(macid,usage,username,date))
          db.commit()
          if add_rec1>0:
             output = '{"function":"add_meter","session_id":"%s","error_code":"0", "Response":"Succesfully added meter"}' %(sid)
             cursor.close()
             db.close()
             return mqttc.publish('jts/prepaid/response',output) 
    except MySQLdb.Error, e:
        try:
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish('jts/prepaid/response',str(e.args[0])+str(e.args[1])) 
        except IndexError:
            #print "MySQL Error: %s" % str(e)
            return mqttc.publish('jts/prepaid/response',str(e))
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"add_meter","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to add the meter"}' %(sid)
        return mqttc.publish('jts/prepaid/response',output)

##################### get balance ##############################################
def get_balance(mosq,obj,msg):
    print "get_balnce......."
    #print "this is add_meters : ",str(msg.payload)
    db = MySQLdb.connect("localhost","prepaid","ptljtsprepaid","PrepaidRCDB")
    cursor = db.cursor()
    output_str = "get_balance - Unable to Authenticate/get_balance... " 
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/prepaid/response','{"function":"get_balance","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')

    if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"get_balnce","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/prepaid/response',output)

    sid = data1['session_id']


    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"function":"get_balance","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/prepaid/response',output)

    if(data1.get('password') is None):
       output_str += ",passsword is mandatory"
       output = '{"function":"get_balance","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
       return mqttc.publish('jts/prepaid/response',output)
    cat=''
    username    = data1['username']
    password    = data1['password']
    sqlq = "SELECT * FROM Prepaid_Users WHERE Username='"+username+"' AND Password='"+password+"'"
    #sqlq = "SELECT * FROM Oyo_Users"
    print "Checking Credentials : : ",sqlq
    cursor.execute(sqlq)
    results = cursor.fetchone()
    print "Checking Data exists or not : ",results[7]
    if results > 0:
       cat=results[7]
       print 'Login Data Existed'
    else:
       #print 'Login Data not authorized so quiting ........Thanks'
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to add the meter"
       output = '{"function":"get_balance","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
       return mqttc.publish('jts/prepaid/response',output)
    try:

           
       if((data1.get('UserId') is None) or ((data1.get('UserId') is not  None) and (len(data1['UserId']) <= 0))):
           output_str += ", UserId is mandatory"
           output = '{"function":"get_balance","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           return mqttc.publish('jts/prepaid/response',output)
       else:
           uid = data1['UserId'] 
       
       print "cat is ",cat
       sqlq1 = "select Prepaid_Payment.Paid,Prepaid_Meters.Units,(select Prepaid_Cat_Fares.U0T50 from Prepaid_Cat_Fares where Prepaid_Cat_Fares.CatId=Prepaid_Payment.CatId)as fare50,(select Prepaid_Cat_Fares.U51T100 from Prepaid_Cat_Fares where Prepaid_Cat_Fares.CatId=Prepaid_Payment.CatId)as fare100 from Prepaid_Payment,Prepaid_Meters,Prepaid_Cat_Fares where Prepaid_Payment.UserId=Prepaid_Meters.UserId and Prepaid_Cat_Fares.CatId=%s" %(cat)
       #sqlq = "SELECT * FROM Oyo_Users"
       print "Checking input mac id ....... :",sqlq1
       cursor.execute(sqlq1)
       results = cursor.fetchone()
       print "checking in opreate unit ..... :",results
       if results > 0:
          paid=results[0]
          usage=results[1]
          fare1=results[2]
          fare2=results[3]
          print paid,usage,fare1,fare2
          
          if float(usage)<float(50):
             cost1=float(usage)*float(fare1)
	     bal=float(paid)-float(cost1)
             
          else:
             cost1=float(usage)*float(fare2)
             bal=float(paid)-float(cost1) 
          output = '{"error_code":"0", "session_id":"%s","Units":"%s","Balance":"%s"}' %(sid,usage,bal)
          return mqttc.publish('jts/prepaid/response',output)
       else:
          print "inserting ........."
          return mqttc.publish('jts/prepaid/response',output) 
    except MySQLdb.Error, e:
        try:
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish('jts/prepaid/response',str(e.args[0])+str(e.args[1])) 
        except IndexError:
            #print "MySQL Error: %s" % str(e)
            return mqttc.publish('jts/prepaid/response',str(e))
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"add_meter","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to add the meter"}' %(sid)
        return mqttc.publish('jts/prepaid/response',output)








################## publish response #################################################
def on_publish(client, userdata, result):
        print "data published \n"


def on_connect(client, userdata, flags, rc):
    #print("Connected with result code "+str(rc))
    mqttc.subscribe("jts/oyo/#")
def on_disconnect(client, userdata, rc):
    if rc != 0:
        print "Unexpected MQTT disconnection. Will auto-reconnect"
        #mqttc.subscribe("jts/oyo/#")
######################### mqtt methods ####################################
mqttc = mqtt.Client()
#################### esp calls ####################################
mqttc.message_callback_add('jts/prepaid/add_meter',add_meter)
mqttc.message_callback_add('jts/prepaid/get_balance',get_balance)

mqttc.on_publish = on_publish
mqttc.on_disconnect = on_disconnect
mqttc.on_connect = on_connect
mqttc.username_pw_set('esp', 'ptlesp01')
mqttc.connect("cld003.jts-prod.in", 1883, 60)
mqttc.subscribe("jts/prepaid/#")
mqttc.loop_forever()
