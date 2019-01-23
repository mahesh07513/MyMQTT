import paho.mqtt.client as mqtt
from datetime import datetime
from datetime import timedelta
import paho.mqtt.client as mqtt
import MySQLdb
import json
import sys
import time
import requests
import string
import random
import urllib2
import ast
############### MQTT Data ##############################
host = 'cld003.jts-prod.in'
m_uname = 'esp'
m_pass = 'ptlesp01'
pub_topic = 'jts/GenAC/v_0_0_2/2s/error'
sub_topic = 'jts/GenAC/v_0_0_2/2s/dev'
sm1 = 'Server'
################# DB Data #######################
db_host = "localhost"
db_uname = "jtsac"
db_pass = "ptljtsac"
db_name = "JtsAcDB"
######################################################
class DB:
  conn = None

  def connect(self):
    self.conn = MySQLdb.connect(db_host,db_uname,db_pass,db_name)

  def query(self, sql):
    try:
      cursor = self.conn.cursor()
      cursor.execute(sql)
      #print "cursor" 
    except (AttributeError, MySQLdb.OperationalError):
      self.connect()
      cursor = self.conn.cursor()
      cursor.execute(sql)
    return cursor

####################################################
def id_generator(size=9, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))
########### Handle Request ######################
def on_message(client, userdata, msg):
    print msg.payload
    try:
        data = json.loads(msg.payload)
    except ValueError:
       return mqttc.publish(pub_topic,'{"error_code":"-2","error_desc":"Response=invalid input, no proper JSON request"}') 

    try:
        if(data.get('session_id') is not None):
           sid=data['session_id']
           if((data.get('req') is None) or ((data.get('req') is not  None) and (len(data['req']) <= 0))):
               output_str = " req is mandatory"
               output = '{"session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
               return mqttc.publish(pub_topic,output)
           else:
               request = data['req']
           if((data.get('sm') is None) or ((data.get('sm') is not  None) and (len(data['sm']) <= 0))):
               output_str = " sm is mandatory"
               output = '{"session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
               return mqttc.publish(pub_topic,output)
           else:
               sm = data['sm']
           if((data.get('dm') is None) or ((data.get('dm') is not  None) and (len(data['dm']) <= 0))):
               output_str = " dm is mandatory"
               output = '{"session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
               return mqttc.publish(pub_topic,output)
           else:
               dm = data['dm'] 
           if((data.get('data') is None) or ((data.get('data') is not  None) and (len(data['data']) <= 0))):
               output_str = " data is mandatory"
               output = '{"session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
               return mqttc.publish(pub_topic,output)
           else:
               data1 = data['data']
           ############ users ############
           if request=="validate_login":
              validate_login(data,request,sm,dm,sid)
           elif request=="get_roles":
              print "in get_roles"
           elif request=="add_user":
              print "in add_user"
           elif request=="get_clients":
              print "in get_clients"
           elif request=="get_images":
              print "in get_images"
           ######### City ##############
           elif request=="get_city":
              print "in get_city"
           elif request=="add_city":
              print "in add_city"
           elif request=="delete_city":
              print "in delete_city"
           elif request=="update_city":
              print "in update_city"
           ######## Premise ############
           elif request=="get_city_premise":
              print "in get_city_premise"
           elif request=="add_premise":
              print "in add_premise"
           elif request=="delete_premise":
              print "in delete_premise"
           elif request=="update_premise":
              print "in update_premise"
           ####### Floors ############
           elif request=="add_floor":
              print "in add_floor"
           elif request=="update_floor":
              print "in update_floor"
           elif request=="get_floors":
              print "in get_floors"
           elif request=="delete_floor":
              print "in delete_floor"
           ###### Rooms ################
           elif request=="get_rooms":
              print "in get_rooms"
           elif request=="add_room":
              print "in add_room"
           elif request=="update_room":
              print "in update_room"
           elif request=="delete_room":
              print "in delete_room"
           ####### Units ##############
           elif request=="add_unit":
              print "in add_unit"
           elif request=="add_new_unit":
              print "in add_new_unit"
           elif request=="get_units_all":
              print "in get_units_all"
           elif request=="delete_unit":
              print "in delete_unit"
           elif request=="get_units_test":
              print "in get_units_test"
           elif request=="get_unit_details":
              print "in get_unit_details"
           elif request=="get_power":
              print "in get_power"
           elif request=="get_temp_history":
              print "in get_temp_history"
           elif request=="delete_unit_utype":
              print "in delete_unit_utype"
           elif request=="get_graph_history":
              print "in get_graph_history"
           elif request=="get_saved_units":
              print "in get_saved_units"
           elif request=="get_graph_history_mobile":
              print "in get_graph_history_mobile"
           elif request=="get_room_units":
              print "in get_graph_history_mobile"
           elif request=="temp":
              print "in temp"
           elif request=="set_temp":
              print "in set_temp"
           elif request=="lora_temp":
              print "in lora_temp"

           else:
              print "no request"           
        else:
           output_str = " Session_id is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str)
           return mqttc.publish(pub_topic,output)   
    except Exception,e:
        print "exception is : ",str(e)
        return mqttc.publish(pub_topic,'{"error_code":"2","error_desc":"Response=Exception Occured in request"}')    

##################################################
def validate_login(data,req,source,destination,sid):
    print "in validation fun",str(data)
    try:
        data3 = json.dumps(data)
        data1 = json.loads(data3)
        req=str(req)
        source=str(source)
        destination=str(destination)
        sid=str(sid)
    except ValueError,e:
       #print "json e is :",str(e)
       return mqttc.publish(pub_topic,'{"funtion":"%s","sm":"%s","dm":"%s","session_id":"%s","data":{"error_code":"-3","error_desc":"Response=invalid data param input, no proper JSON request"}}' %(req,sm1,destination,sid))
    try:
       output_str = 'Unable to process your request'
       db=DB()
       '''
       sqlq1 = "SELECT ClientName FROM AC_Clients " 
       cursor.execute(sqlq1)
       results2 = cursor.fetchone()
       print "db is ",results2
       '''
     
       if(data1.get('username') is None):
          output_str += ",username is mandatory"
          output = '{"function":"%s","sm":"%s","dm":"%s","session_id":"%s","data":{"error_code":"2", "error_desc": "Response=%s"}}' %(req,destination,sm1,sid,output_str)
          return mqttc.publish(pub_topic,output)
       
       username    = data1['username'] 
       if(data1.get('password') is None):
          output_str += ",password is mandatory"
          output = '{"function":"%s","sm":"%s","dm":"%s","session_id":"%s","data":{"error_code":"2", "error_desc": "Response=%s"}}' %(req,sm1,destination,sid,output_str)
          return mqttc.publish(pub_topic,output)
       
       password    = data1['password']
       #print "data"
       sqlq = "SELECT ImgId,Version,ClientId FROM AC_Users WHERE Username='%s' AND Password='%s'" %(username,password)
       #print sqlq
       #sqlq = "SELECT * FROM Oyo_Users"
       #print "Checking Credentials : : ",sqlq
       cursor=db.query(sqlq)
       results = cursor.fetchone()
       print "Checking Data exists or not : ",results
       if len(results) > 0:
          imgid=results[0]
          version=results[1]
          cid=results[2]
          sqlq1 = "SELECT Image FROM AC_Images WHERE ImgId='%s'" %(imgid)
          cursor.execute(sqlq1)
          results1 = cursor.fetchone()
          imgpath='http://cld003.jts-prod.in:5904/GenericACApp/media/'
          #print results1
          if len(results1) > 0:
             imagname=results1[0]
             imgpath=imgpath+imagname
          else:
             imagname='Jochebed.png'
             imgpath=imgpath+imagname
          #print 'Login Data Existed'
          sqlq1 = "SELECT ClientName FROM AC_Clients WHERE ClientId='%s'" %(cid)
          cursor.execute(sqlq1)
          results2 = cursor.fetchone()
          if len(results2) > 0:
             cname=results2[0]
          else:
             cname='Jochebed Tech Solutions'
          output = '{"function":"%s","sm":"%s","dm":"%s","session_id":"%s","data":{"error_code":"0", "Response":"Successfully Authenticated for user: %s","img_path":"%s","version":"%s","cname":"%s"}}' %(req,sm1,destination,sid,username,imgpath,version,cname)
          #print output
          return mqttc.publish(pub_topic,output)
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          #print "in else"
          output = '{"function":"%s","sm":"%s","dm":"%s","session_id":"%s","data":{"error_code":"2", "error_desc": "Response=%s"}}' %(req,sm1,destination,sid,output_str)
          return mqttc.publish(pub_topic,output)
    
    except (AttributeError, MySQLdb.OperationalError):
        return mqttc.publish(pub_topic,'MySQL server has gone away')
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish(pub_topic,str(e.args[0])+str(e.args[1]))
        except IndexError:
            print "MySQL Error: %s" % str(e)
            return mqttc.publish(pub_topic,str(e))
    except Exception, e:
        #print "in ex",str(e) 
        output = '{"function":"%s","sm":"%s","dm":"%s","session_id":"%s","data":{"error_code":"3", "error_desc": "Response=Failed to login"}}' %(req,sm1,destination,sid)
        return mqttc.publish(pub_topic,output)
    

##################################################
def get_roles(data,req):
    print "in get_roles fun",str(data)
    try:
        data1 = str(data)
    except ValueError,e:
       print "json e is :",str(e)
       return mqttc.publish(pub_topic,'{"funtion":"%s","error_code":"-3","error_desc":"Response=invalid data param input, no proper JSON request"}' %(req))
    try:
       output_str = 'Unable to process your request'
       db = MySQLdb.connect(db_host,db_uname,db_pass,db_name)
       cursor = db.cursor()
       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"validate_login","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish(pub_topic,output)
       else:
          sid = data1['session_id']
       if(data1.get('username') is None):
          output_str += ",username is mandatory"
          output = '{"function":"validate_login","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          return mqttc.publish(pub_topic,output)
       else:
          username    = data1['username'] 
       if(data1.get('password') is None):
          output_str += ",password is mandatory"
          output = '{"function":"validate_login","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          return mqttc.publish(pub_topic,output)
       else:
          password    = data1['password']

       sqlq = "SELECT ImgId,Version FROM AC_Users WHERE Username='%s' AND Password='%s'" %(username,password)
       #sqlq = "SELECT * FROM Oyo_Users"
       #print "Checking Credentials : : ",sqlq
       cursor.execute(sqlq)
       results = cursor.fetchone()
       #print "Checking Data exists or not : ",results
       if len(results) > 0:
          imgid=results[0]
          version=results[1]
          sqlq1 = "SELECT Image FROM AC_Images WHERE ImgId='%s'" %(imgid)
          cursor.execute(sqlq1)
          results1 = cursor.fetchone()
          imgpath='http://cld003.jts-prod.in:5904/GenericACApp/media/'
          #print results1
          if len(results1) > 0:
             imagname=results1[0]
             imgpath=imgpath+imagname
          else:
             imagname='Jochebed.png'
             imgpath=imgpath+imagname
          #print 'Login Data Existed'
          output = '{"function":"validate_login","session_id":"%s","error_code":"0", "Response":"Successfully Authenticated for user: %s","img_path":"%s","version":"%s"}' %(sid,username,imgpath,version)
          #print output
          return mqttc.publish(pub_topic,output)
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          #print "in else"
          output = '{"function":"validate_login","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          return mqttc.publish(pub_topic,output)
    
    except (AttributeError, MySQLdb.OperationalError):
        return mqttc.publish(pub_topic,'MySQL server has gone away')
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish(pub_topic,str(e.args[0])+str(e.args[1]))
        except IndexError:
            print "MySQL Error: %s" % str(e)
            return mqttc.publish(pub_topic,str(e))
    except Exception, e:
        print "in ex",str(e) 
        output = '{"function":"validate_login","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to login"}' %sid
        return mqttc.publish(pub_topic,output)
    finally:
        print "in final"
        cursor.close()
        db.close()
#####################################################

#####################################################
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    mqttc.subscribe(sub_topic)
def on_disconnect(client, userdata, rc):
    if rc != 0:
        print "Unexpected MQTT disconnection. Will auto-reconnect"

try:
    mqttc = mqtt.Client()
    mqttc.username_pw_set(m_uname, m_pass)
    mqttc.connect(host, 1883, 60)
    mqttc.on_message=on_message
    mqttc.on_disconnect = on_disconnect
    mqttc.on_connect = on_connect
    mqttc.subscribe(sub_topic)
    mqttc.loop_forever()

except Exception,e:
    print "In exception : ",str(e)
