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
'''
try:
   db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
   cursor = db.cursor()
except (AttributeError, MySQLdb.OperationalError):
        mqttc.publish('jts/oyo/error','MySQL server has gone away')
except MySQLdb.Error, e:
   try:
       #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
       mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]))
   except IndexError:
       #print "MySQL Error: %s" % str(e)
       mqttc.publish('jts/oyo/error',str(e))
'''
####################################################
def id_generator(size=9, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


####################### add_user #################################
def add_user(mosq,obj,msg):
    #print "add_user......."
    #print "this is add_users : ",str(msg.payload)
    
    output_str = "add_user - Unable to Authenticate/add_user... " 
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       pass
       ##return mqttc.publish('jts/oyo/error','{"function":"add_user","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:
       db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
       cursor = db.cursor()
       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"add_user","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          ##return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']
       if(data1.get('username') is None):
          output_str += ",username is mandatory"
          output = '{"function":"add_user","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output)

       if(data1.get('password') is None):
          output_str += ",passsword is mandatory"
          output = '{"function":"add_user","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output)

       username    = data1['username']
       password    = data1['password']
       sqlq = "SELECT * FROM AC_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
       #sqlq = "SELECT * FROM Oyo_Users"
       #print "Checking Credentials : : ",sqlq
       cursor.execute(sqlq)
       results = cursor.fetchone()
       #print "Checking Data exists or not : ",results
       if results > 0:
          pass
          #print 'Login Data Existed'
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to add the user"
          output = '{"function":"add_user","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output)
    
       if((data1.get('Uname') is None) or ((data1.get('Uname') is not  None) and (len(data1['Uname']) <= 0))):
           output_str += ", Uname is mandatory"
           output = '{"function":"add_user","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           uname = data1['Uname'] 

       if((data1.get('Pass') is None) or ((data1.get('Pass') is not  None) and (len(data1['Pass']) <= 0))):
           output_str += ", Pass is mandatory"
           output = '{"function":"add_user","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           pass1 = data1['Pass'] 
 
       if((data1.get('Role') is None) or ((data1.get('Role') is not  None) and (len(data1['Role']) <= 0))):
           output_str += ", Role is mandatory"
           output = '{"function":"add_user","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           role = data1['Role']
 
       if((data1.get('client_id') is None) or ((data1.get('client_id') is not  None) and (len(data1['client_id']) <= 0))):
           output_str += ",client_id is mandatory"
           output = '{"function":"add_user","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           cid = data1['client_id']

       if((data1.get('image_id') is None) or ((data1.get('image_id') is not  None) and (len(data1['image_id']) <= 0))):
           output_str += ",image_id is mandatory"
           output = '{"function":"add_user","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           imgid = data1['image_id']

       sqlq1 = "SELECT * FROM AC_Users WHERE Username='"+uname+"' AND Password='"+pass1+"'"
       #sqlq = "SELECT * FROM Oyo_Users"
       #print "Checking input mac id ....... :",sqlq1
       cursor.execute(sqlq1)
       results = cursor.fetchone()
       #print "checking in opreate unit ..... :",results
       if results > 0:
          output_str = "User Already Exits"
          output = '{"function":"add_user","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
          ##return mqttc.publish('jts/oyo/error',output)
       else:
          #print "inserting ........."
          add_rec1=cursor.execute("""INSERT INTO AC_Users(Username,Password,ChangeBy,Role,ClientId,ImgId,ChangeDate,StatusActive) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",(uname,pass1,username,role,cid,imgid,date,True))
          db.commit()
          if add_rec1>0:
             output = '{"function":"add_user","session_id":"%s","error_code":"0", "Response":"Succesfully added user"}' %(sid)
             
             cursor.close()
             db.close()      
             ##return mqttc.publish('jts/oyo/error',output) 
    except (AttributeError, MySQLdb.OperationalError):
        pass
        ##return mqttc.publish('jts/oyo/error','MySQL server has gone away')
    except MySQLdb.Error, e:
        try:
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            ##return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1])) 
            pass
        except IndexError:
            print "MySQL Error: %s" % str(e)
            ##return mqttc.publish('jts/oyo/error',str(e))
            pass
    except Exception, e:
        
        
        output = '{"function":"add_user","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to add the User"}' %(sid)
        ##return mqttc.publish('jts/oyo/error',output)

######################## add cities ###############################################
def add_city(mosq,obj,msg):
    #print "add_city......."
    
    output_str = "add_city - Unable to Authenticate/add_city... "
    #print "this is register string : ",str(msg.payload)
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       pass
       ##return mqttc.publish('jts/oyo/error','{"function":"add_city","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:
       db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
       cursor = db.cursor()
       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"add_city","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          ##return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']


       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"add_city","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"add_city","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM AC_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to add unit"
          output = '{"function":"add_city","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('city_name') is None) or ((data1.get('city_name') is not  None) and (len(data1['city_name']) <= 0))):
           output_str += ", city_name is mandatory"
           output = '{"function":"add_city","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           #return mqttc.publish('jts/oyo/error',output)
       else:
           cname = data1['city_name']

       
       #sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"' AND Longitude='"+long1+"' AND Latitude='"+lat+"'"
       sqlq1 = "SELECT * FROM AC_City WHERE CityDesc='%s' and ChangeBy='%s'" %(cname,username)
       cursor.execute(sqlq1)
       results = cursor.fetchone()
       
       if results > 0:
          #print "unit existed"
          output_str = "The city already Registered"
          output = '{"function":"add_city","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          #return mqttc.publish('jts/oyo/error',output)
          
       else:
          #print "exitig........"
          #add_rec=cursor.execute("""INSERT INTO DtD_Units(UnitDesc,Longitude,Latitude,Operation,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s)""",(name,float(long1),float(lat),'OFF',username,date))
          add_rec=cursor.execute("""INSERT INTO AC_City(CityDesc,ChangeBy,ChangeDate,StatusActive) VALUES (%s,%s,%s,%s)""",(cname,username,date,True))
          db.commit()
          if add_rec > 0:
             #print "inserted"
             output = '{"function":"add_city","session_id":"%s","error_code":"0", "Response":"Successfully added the city : %s"}'%(sid,cname)
             ##return mqttc.publish('jts/oyo/error',output)
             cursor.close()
             db.close()            
          else:
             #print "insert filed"
             output = '{"function":"add_city","session_id":"%s","error_code":"-2", "error_desc": "Response= Unable to add city"}'%(sid)
             ##return mqttc.publish('jts/oyo/error',output)
    except (AttributeError, MySQLdb.OperationalError):
        pass
        #return mqttc.publish('jts/oyo/error','MySQL server has gone away')
    except MySQLdb.Error, e:
        try:
            pass
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            ##return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1])) 
        except IndexError:
            pass
            #print "MySQL Error: %s" % str(e)
            ##return mqttc.publish('jts/oyo/error',str(e))
    except Exception, e:
        
        
        output = '{"function":"add_city","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to add the city"}' %(sid)
        ##return mqttc.publish('jts/oyo/error',output)
    
        

############### update city  #######################################
def update_city(mosq,obj,msg):
    #print "update......."
    
    output_str = "update_city - Unable to Authenticate/update_city... "
    #print "this is register string : ",str(msg.payload)
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       pass
       ##return mqttc.publish('jts/oyo/error','{"function":"update_city","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:
       
       db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
       cursor = db.cursor()
       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"update_city","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          ##return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']


       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"update_city","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"update_city","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM AC_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to update_city"
          output = '{"function":"update_city","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
           output_str += ", city_id is mandatory"
           output = '{"function":"update_city","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           #return mqttc.publish('jts/oyo/error',output)
       else:
           cid = data1['city_id']

       if((data1.get('city_name') is None) or ((data1.get('city_name') is not  None) and (len(data1['city_name']) <= 0))):
           output_str += ", city_name is mandatory"
           output = '{"function":"update_city","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           cname = data1['city_name']
       
       #sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"' AND Longitude='"+long1+"' AND Latitude='"+lat+"'"
       upd_rec=cursor.execute ("""UPDATE AC_City SET CityDesc=%s,ChangeBy=%s,ChangeDate=%s WHERE CityId=%s""", (cname,username,date,cid))
       db.commit() 
       
       if upd_rec > 0:
          output = '{"function":"update_city","session_id":"%s","error_code":"0", "Response":"Successfully updated the city : %s"}'%(sid,cid)
          ##return mqttc.publish('jts/oyo/error',output)
          cursor.close()
          db.close()
       else:
          output_str = "City not existed"
          output = '{"function":"update_city","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output) 

          
    except (AttributeError, MySQLdb.OperationalError):
        pass
        ##return mqttc.publish('jts/oyo/error','MySQL server has gone away')      
    except MySQLdb.Error, e:
        try:
            pass
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            ##return mqttc.publish('jts/oyo/error',e.args[0], e.args[1])
        except IndexError:
            #print "MySQL Error: %s" % str(e)
            ##return mqttc.publish('jts/oyo/error',str(e))
            pass
    except Exception, e:
        
        
        output = '{"function":"update_city","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to update the city"}' %(sid)
        ##return mqttc.publish('jts/oyo/error',output)
    
        
        

######################## add premise ###############################################
def add_premise(mosq,obj,msg):
    #print "add premise......."
    
    output_str = "add_premise - Unable to Authenticate/add_premise... "
    #print "this is register string : ",str(msg.payload)
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       pass
       ##return mqttc.publish('jts/oyo/error','{"function":"add_premise","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:
       db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
       cursor = db.cursor()
       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"add_premise","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          ##return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']

        
       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"add_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"add_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM AC_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to add premise"
          output = '{"function":"add_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('premise_name') is None) or ((data1.get('premise_name') is not  None) and (len(data1['premise_name']) <= 0))):
           output_str += ", premise_name is mandatory"
           output = '{"function":"add_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           pname = data1['premise_name']

       if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
           output_str += ", city_id is mandatory"
           output = '{"function":"add_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           #return mqttc.publish('jts/oyo/error',output)
       else:
           cid = data1['city_id']

       
       #sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"' AND Longitude='"+long1+"' AND Latitude='"+lat+"'"
       sqlq1 = "SELECT * FROM AC_Premise WHERE PremiseDesc='"+pname+"' and CityId='"+cid+"'"
       cursor.execute(sqlq1)
       results = cursor.fetchone()
       pid=''
       if results > 0:
          #print "unit existed",results[0]
          output = '{"function":"add_premise","session_id":"%s","error_code":"2", "error_desc": "Response=Premise already exists"}' %(sid)
          #return mqttc.publish('jts/oyo/error',output)  
       else:
          #print "exitig........"
          add_rec=cursor.execute("""INSERT INTO AC_Premise(CityId,PremiseDesc,ChangeBy,ChangeDate,StatusActive) VALUES (%s,%s,%s,%s,%s)""",(cid,pname,username,date,True))
          db.commit()
          if add_rec > 0:
             #print "inserted"
             output = '{"function":"add_premise","session_id":"%s","error_code":"0", "Response":"Successfully added the premise : %s"}'%(sid,pname)
             #return mqttc.publish('jts/oyo/error',output)
             cursor.close()
             db.close()
          else:
             output_str = "The premise not mapping to city"
             output = '{"function":"add_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             #return mqttc.publish('jts/oyo/error',output)
    except (AttributeError, MySQLdb.OperationalError):
        pass
        #return mqttc.publish('jts/oyo/error','MySQL server has gone away')         
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            #return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]))
        except IndexError:
            print "MySQL Error: %s" % str(e)
            #return mqttc.publish('jts/oyo/error',str(e))
    except Exception, e:
        
        
        output = '{"function":"add_premise","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to add the premise"}' %(sid)
        #return mqttc.publish('jts/oyo/error',output)
    

############### update premise  #######################################
def update_premise(mosq,obj,msg):
    #print "update premise......."
    
    output_str = "update_premise - Unable to Authenticate/update_premise... "
    #print "this is update string : ",str(msg.payload)
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       pass
       ##return mqttc.publish('jts/oyo/error','{"function":"update_premise","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
       cursor = db.cursor()
       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"update_premise","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          ##return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']


       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"update_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"update_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM AC_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to update_Premise"
          output = '{"function":"update_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('premise_id') is None) or ((data1.get('premise_id') is not  None) and (len(data1['premise_id']) <= 0))):
           output_str += ", premise_id is mandatory"
           output = '{"function":"update_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           pid = data1['premise_id']

       if((data1.get('premise_name') is None) or ((data1.get('premise_name') is not  None) and (len(data1['premise_name']) <= 0))):
           output_str += ", premise_name is mandatory"
           output = '{"function":"update_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           pname = data1['premise_name']
      
       #sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"' AND Longitude='"+long1+"' AND Latitude='"+lat+"'"
       upd_rec=cursor.execute ("""UPDATE AC_Premise SET PremiseDesc=%s,ChangeBy=%s,ChangeDate=%s WHERE PremiseId=%s""", (pname,username,date,pid))
       db.commit() 
       
       if upd_rec > 0:
          output = '{"function":"update_premise","session_id":"%s","error_code":"0", "Response":"Successfully updated the premise : %s"}'%(sid,pid)
          ##return mqttc.publish('jts/oyo/error',output)
          cursor.close()
          db.close()
       else:
          output_str = "premise not existed"
          output = '{"function":"update_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output) 

          
    except (AttributeError, MySQLdb.OperationalError):
        pass
        ##return mqttc.publish('jts/oyo/error','MySQL server has gone away')      
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            ##return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]))
        except IndexError:
            print "MySQL Error: %s" % str(e)
            ##return mqttc.publish('jts/oyo/error',str(e))
    except Exception, e:
        output = '{"function":"update_premise","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to update the premise"}' %(sid)
        ##return mqttc.publish('jts/oyo/error',output)
    
###################### add_floor ############################        
def add_floor(mosq,obj,msg):
    #print "add_floor......."
    
    output_str = "add_floor - Unable to Authenticate/add_floor... "
    #print "this is register string : ",str(msg.payload)
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       pass
       ##return mqttc.publish('jts/oyo/error','{"function":"add_floor","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:
       db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
       cursor = db.cursor()
       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"add_floor","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          ##return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']


       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"add_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"add_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM AC_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to add premise"
          output = '{"function":"add_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('floor_name') is None) or ((data1.get('floor_name') is not  None) and (len(data1['floor_name']) <= 0))):
           output_str += ", floor_name is mandatory"
           output = '{"function":"add_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           fname = data1['floor_name']

       if((data1.get('premise_id') is None) or ((data1.get('premise_id') is not  None) and (len(data1['premise_id']) <= 0))):
           output_str += ", premise_id is mandatory"
           output = '{"function":"add_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           pid = data1['premise_id']

       if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
           output_str += ", city_id is mandatory"
           output = '{"function":"add_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           cid = data1['city_id'] 
       #sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"' AND Longitude='"+long1+"' AND Latitude='"+lat+"'"
       sqlq1 = "SELECT * FROM AC_Floors WHERE FloorDesc='"+fname+"' and CityId='"+cid+"' and PremiseId='"+pid+"' and ChangeBy='"+username+"'"
       cursor.execute(sqlq1)
       results = cursor.fetchone()
       fid=''
       if results > 0:
          #print "unit existed"
          output_str = "The floor is already exists"
          output = '{"function":"add_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output)

          
       else:
          #print "inserted"
          add_rec1=cursor.execute("""INSERT INTO AC_Floors(CityId,PremiseId,FloorDesc,ChangeBy,ChangeDate,StatusActive) VALUES (%s,%s,%s,%s,%s,%s)""",(cid,pid,fname,username,date,True))
          db.commit()
          if add_rec1 > 0:
             output = '{"function":"add_floor","session_id":"%s","error_code":"0", "Response":"Successfully added the floor : %s"}'%(sid,fname)
             ##return mqttc.publish('jts/oyo/error',output)
             cursor.close()
             db.close()
          else:
             output_str = "The floor not mapping to premise"
             output = '{"function":"add_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
            
    except (AttributeError, MySQLdb.OperationalError):
        pass
        ##return mqttc.publish('jts/oyo/error','MySQL server has gone away')        
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            ##return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]))
        except IndexError:
            print "MySQL Error: %s" % str(e)
            ##return mqttc.publish('jts/oyo/error',str(e))
    except Exception, e:
        output = '{"function":"add_floor","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to add the floor"}'
        ##return mqttc.publish('jts/oyo/error',output)
    
        
        

######################### update floor ####################################
def update_floor(mosq,obj,msg):
    #print "update_floor......."
    
    output_str = "update_floor - Unable to Authenticate/update_floor... "
    #print "this is update string : ",str(msg.payload)
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       pass
       ##return mqttc.publish('jts/oyo/error','{"function":"update_floor","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:
       db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
       cursor = db.cursor()
       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"update_floor","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          ##return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']


       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"update_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"update_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM AC_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to update_city"
          output = '{"function":"update_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('floor_id') is None) or ((data1.get('floor_id') is not  None) and (len(data1['floor_id']) <= 0))):
           output_str += ", floor_id is mandatory"
           output = '{"function":"update_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           fid = data1['floor_id']

       if((data1.get('floor_name') is None) or ((data1.get('floor_name') is not  None) and (len(data1['floor_name']) <= 0))):
           output_str += ", floor_name is mandatory"
           output = '{"function":"update_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           fname = data1['floor_name']
      
       #sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"' AND Longitude='"+long1+"' AND Latitude='"+lat+"'"
       upd_rec=cursor.execute ("""UPDATE AC_Floors SET FloorDesc=%s,ChangeBy=%s,ChangeDate=%s WHERE FloorId=%s""", (fname,username,date,fid))
       db.commit() 
       
       if upd_rec > 0:
          output = '{"function":"update_floor","session_id":"%s","error_code":"0", "Response":"Successfully updated the floor "}' %(sid)
          ##return mqttc.publish('jts/oyo/error',output)
          cursor.close()
          db.close()
       else:
          output_str = "premise not existed"
          output = '{"function":"update_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output) 
 
          
    except (AttributeError, MySQLdb.OperationalError):
        pass
        ##return mqttc.publish('jts/oyo/error','MySQL server has gone away')         
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            ##return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]))
        except IndexError:
            print "MySQL Error: %s" % str(e)
            ##return mqttc.publish('jts/oyo/error',str(e))
    except Exception, e:
        output = '{"function":"update_floor","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to update the floor"}' %(sid)
        ##return mqttc.publish('jts/oyo/error',output)
       
################### add room ####################################################
def add_room(mosq,obj,msg):
    #print "add_rooom......."
    
    output_str = "add_room - Unable to Authenticate/add_room... "
    #print "this is register string : ",str(msg.payload)
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       pass
       ##return mqttc.publish('jts/oyo/error','{"function":"add_room","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:
       db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
       cursor = db.cursor()
       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"add_room","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          ##return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']


       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"add_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"add_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM AC_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to add premise"
          output = '{"function":"add_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('room_name') is None) or ((data1.get('room_name') is not  None) and (len(data1['room_name']) <= 0))):
           output_str += ", room_name is mandatory"
           output = '{"function":"add_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           rname = data1['room_name']

       if((data1.get('floor_id') is None) or ((data1.get('floor_id') is not  None) and (len(data1['floor_id']) <= 0))):
           output_str += ", floor_id is mandatory"
           output = '{"function":"add_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           fid = data1['floor_id']

       if((data1.get('premise_id') is None) or ((data1.get('premise_id') is not  None) and (len(data1['premise_id']) <= 0))):
           output_str += ", premise_id is mandatory"
           output = '{"function":"add_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           pid = data1['premise_id']

       if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
           output_str += ", city_id is mandatory"
           output = '{"function":"add_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           cid = data1['city_id']
       
       #sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"' AND Longitude='"+long1+"' AND Latitude='"+lat+"'"
       sqlq1 = "SELECT * FROM AC_Rooms WHERE RoomDesc='"+rname+"' and CityId='"+cid+"' and PremiseId='"+pid+"' and FloorId='"+fid+"' and ChangeBy='"+username+"'"
       cursor.execute(sqlq1)
       results = cursor.fetchone()
       
       if results > 0:
          #print "unit existed"
          output_str = "The room already exists"
          output = '{"function":"add_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output)
          
       else:
          #print "exitig........"
          add_rec=cursor.execute("""INSERT INTO AC_Rooms(CityId,PremiseId,RoomDesc,FloorId,ChangeBy,ChangeDate,StatusActive) VALUES (%s,%s,%s,%s,%s,%s,%s)""",(cid,pid,rname,fid,username,date,True))
          db.commit()
          if add_rec > 0:
             #print "inserted"
             output = '{"function":"add_room","session_id":"%s","error_code":"0", "Response":"Successfully added the room: %s"}'%(sid,rname)
             ##return mqttc.publish('jts/oyo/error',output)
             cursor.close()
             db.close()
          else:
             output_str = "The room not mapping to floor"
             output = '{"session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
    except (AttributeError, MySQLdb.OperationalError):
        pass
        ##return mqttc.publish('jts/oyo/error','MySQL server has gone away')         
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            ##return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]))
        except IndexError:
            print "MySQL Error: %s" % str(e)
            ##return mqttc.publish('jts/oyo/error',str(e))
    except Exception, e:
        output = '{"function":"add_room","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to add the room"}' %(sid)
        ##return mqttc.publish('jts/oyo/error',output)
######################## update room ####################################
def update_room(mosq,obj,msg):
    #print "update_room......."
    
    output_str = "update_room - Unable to Authenticate/update_room... "
    #print "this is update string : ",str(msg.payload)
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       pass
       ##return mqttc.publish('jts/oyo/error','{"function":"update_room","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:
       db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
       cursor = db.cursor()
       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"update_room","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          ##return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']


       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"update_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"update_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM AC_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to update_room"
          output = '{"function":"update_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('room_id') is None) or ((data1.get('room_id') is not  None) and (len(data1['room_id']) <= 0))):
           output_str += ", room_id is mandatory"
           output = '{"function":"update_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           rid = data1['room_id']

       if((data1.get('room_name') is None) or ((data1.get('room_name') is not  None) and (len(data1['room_name']) <= 0))):
           output_str += ", room_name is mandatory"
           output = '{"function":"update_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           rname = data1['room_name']
      
       #sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"' AND Longitude='"+long1+"' AND Latitude='"+lat+"'"
       upd_rec=cursor.execute ("""UPDATE AC_Rooms SET RoomDesc=%s,ChangeBy=%s,ChangeDate=%s WHERE RoomId=%s""", (rname,username,date,rid))
       db.commit() 
       
       if upd_rec > 0:
          output = '{"function":"update_room","session_id":"%s","error_code":"0", "Response":"Successfully updated the room "}' %(sid)
          ##return mqttc.publish('jts/oyo/error',output)
          cursor.close()
          db.close()
       else:
          output_str = "room not existed"
          output = '{"function":"update_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output) 

          
    except (AttributeError, MySQLdb.OperationalError):
        pass
        ##return mqttc.publish('jts/oyo/error','MySQL server has gone away')      
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            ##return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]))
        except IndexError:
            print "MySQL Error: %s" % str(e)
            ##return mqttc.publish('jts/oyo/error',str(e))
    except Exception, e:
        output = '{"function":"update_room","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to update the room"}' %(sid)
        ##return mqttc.publish('jts/oyo/error',output)
    
         

################ add_input_unit ############################
def add_unit(mosq,obj,msg):
    #print "add_input_unit......."
    
    output_str = "add_unit - Unable to Authenticate/add_unit... " 
    #print "this is register string : ",str(msg.payload)
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       pass
       ##return mqttc.publish('jts/oyo/error','{"function":"add_unit","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:
       db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
       cursor = db.cursor() 
       uinput=False
       uoperate=False
       upower=False

       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"add_unit","error_code":"2", "error_desc": "Response=%s"}' %output_str
          ##return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']

       if(data1.get('username') is None):
          output_str += ",username is mandatory"
          output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output)
       
       if(data1.get('password') is None):
          output_str += ",password is mandatory"
          output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output)

       username    = data1['username']
       password    = data1['password']
       sqlq = "SELECT * FROM AC_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
       #sqlq = "SELECT * FROM Oyo_Users"
       #print "Checking Credentials : : ",sqlq
       cursor.execute(sqlq)
       results = cursor.fetchone()

       #print "Checking Data exists or not : ",results
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          
          output_str += ",Your not Autherize to Register a Device"
          output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output)

       if((data1.get('utype') is None) or ((data1.get('utype') is not  None) and (len(data1['utype']) <= 0))):
           output_str += ", utype is mandatory"
           output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           utype = data1['utype']
           if utype=="Input":
              uinput=True
           if utype=="Operate":
              uoperate=True
           if utype=="Power":
              upower=True
       if uinput==True:
          if((data1.get('ipmacid') is None) or ((data1.get('ipmacid') is not  None) and (len(data1['ipmacid']) <= 0))):
             output_str += ", ipmacid is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             ipmacid = data1['ipmacid'] 
           		 
          if((data1.get('ipname') is None) or ((data1.get('ipname') is not  None) and (len(data1['ipname']) <= 0))):
             output_str += ", ipname is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             ipname = data1['ipname'] 
           
          if((data1.get('abrivation') is None) or ((data1.get('abrivation') is not  None) and (len(data1['abrivation']) <= 0))):
             output_str += ", abrivation is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             abrivation = data1['abrivation']

          if((data1.get('floor_id') is None) or ((data1.get('floor_id') is not  None) and (len(data1['floor_id']) <= 0))):
             output_str += ", floor_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             floor = data1['floor_id']
           
          if((data1.get('room_id') is None) or ((data1.get('room_id') is not  None) and (len(data1['room_id']) <= 0))):
             output_str += ", room_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             room = data1['room_id']
          
          if((data1.get('premise_id') is None) or ((data1.get('premise_id') is not  None) and (len(data1['premise_id']) <= 0))):
             output_str += ", premise_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             premise = data1['premise_id']

          if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
             output_str += ", city_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             city = data1['city_id']

          if((data1.get('unit_id') is None) or ((data1.get('unit_id') is not  None) and (len(data1['unit_id']) <= 0))):
             output_str += ", unit_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             uid = data1['unit_id']
       
          #DB
       
          sqlq2="SELECT * FROM AC_Input_Units WHERE IpMacid='%s' and ChangeBy='%s'" %(ipmacid,username)
          cursor.execute(sqlq2)
          results2 = cursor.fetchone()
          #print "mapping data from db: ",results2
          if results2 > 0:
             #print 'maping data exits......... so quiting ..Thanks'
             
             
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=input unit exits"}' %(sid)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             #print 'start mapping .......'
             add_rec1=cursor.execute("""INSERT INTO AC_Input_Units(CityId,FloorId,RoomId,PremiseId,UnitId,IpmacId,IpName,Abrivation,ChangeBy,ChangeDate,StatusActive) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(city,floor,room,premise,uid,ipmacid,ipname,abrivation,username,date,True))
             db.commit()
             if add_rec1 > 0:
                #print 'input mac  rec mapped in DB.'
                output = '{"function":"add_unit","session_id":"%s","error_code":"0", "Response":"Successfully added Input Unit "}' %(sid)
	        ##return mqttc.publish('jts/oyo/error',output)
                cursor.close()
                db.close()
             else:
                
                
                output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=Falied to add a input Units"}' %(sid)
                ##return mqttc.publish('jts/oyo/error',output)

       if uoperate==True:
          if((data1.get('opmacid') is None) or ((data1.get('opmacid') is not  None) and (len(data1['opmacid']) <= 0))):
             output_str += ", opmacid is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             opmacid = data1['opmacid'] 
           		 
          if((data1.get('opname') is None) or ((data1.get('opname') is not  None) and (len(data1['opname']) <= 0))):
             output_str += ", opname is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             opname = data1['opname'] 

          if((data1.get('abrivation') is None) or ((data1.get('abrivation') is not  None) and (len(data1['abrivation']) <= 0))):
             output_str += ", abrivation is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             abrivation = data1['abrivation']
           
          if((data1.get('floor_id') is None) or ((data1.get('floor_id') is not  None) and (len(data1['floor_id']) <= 0))):
             output_str += ", floor_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             floor = data1['floor_id']
           
          if((data1.get('room_id') is None) or ((data1.get('room_id') is not  None) and (len(data1['room_id']) <= 0))):
             output_str += ", room_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             room = data1['room_id']
          
          if((data1.get('premise_id') is None) or ((data1.get('premise_id') is not  None) and (len(data1['premise_id']) <= 0))):
             output_str += ", premise_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             premise = data1['premise_id']

          if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
             output_str += ", city_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             city = data1['city_id']       
         
          if((data1.get('unit_id') is None) or ((data1.get('unit_id') is not  None) and (len(data1['unit_id']) <= 0))):
             output_str += ", unit_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             uid = data1['unit_id']
          #DB
          
          sqlq12="SELECT IpMacid FROM AC_Input_Units WHERE UnitId='%s' and ChangeBy='%s'" %(uid,username)
          cursor.execute(sqlq12)
          results12 = cursor.fetchall() 
          if len(results12) > 0 :
             pass
          else:
             output_str += ", please Configure atleast 1 input unit"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          sqlq2="SELECT * FROM AC_Operate_Units WHERE OpMacid='%s' and ChangeBy='%s'" %(opmacid,username)
          cursor.execute(sqlq2)
          results2 = cursor.fetchone()
          #print "mapping data from db: ",results2
          if results2 > 0:
             #print 'maping data exits......... so quiting ..Thanks'
             
             
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=operate unit exits"}' %(sid)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             #print 'start mapping .......'
             add_rec1=cursor.execute("""INSERT INTO AC_Operate_Units(CityId,FloorId,RoomId,PremiseId,UnitId,OpMacid,OpName,Abrivation,ChangeBy,ChangeDate,StatusActive) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(city,floor,room,premise,uid,opmacid,opname,abrivation,username,date,True))
             db.commit()
             if add_rec1 > 0:
                #print 'operate  mac rec mapped in DB.'
                for ipmacid in results12:
                    output1 = '{"function":"add_unit","error_code":"0", "Response":"Succes","opmacid":"%s"}'%(opmacid)
                    #mqttc.publish('jts/oyo/'+ipmacid,output1)

                output = '{"function":"add_unit","session_id":"%s","error_code":"0", "Response":"Successfully added Operate Unit "}' %(sid)
	        ##return mqttc.publish('jts/oyo/error',output)
                cursor.close()
                db.close()
             else:
                
                
                output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=Falied to add operate Units"}' %(sid)
                ##return mqttc.publish('jts/oyo/error',output)
       
       if upower==True:

          if((data1.get('pmacid') is None) or ((data1.get('pmacid') is not  None) and (len(data1['pmacid']) <= 0))):
             output_str += ", pmacid is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             pmacid = data1['pmacid'] 
           		 
          if((data1.get('pname') is None) or ((data1.get('pname') is not  None) and (len(data1['pname']) <= 0))):
             output_str += ", pname is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             pname = data1['pname'] 
         
          if((data1.get('abrivation') is None) or ((data1.get('abrivation') is not  None) and (len(data1['abrivation']) <= 0))):
             output_str += ", abrivation is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             abrivation = data1['abrivation']
           
          if((data1.get('floor_id') is None) or ((data1.get('floor_id') is not  None) and (len(data1['floor_id']) <= 0))):
             output_str += ", floor_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             floor = data1['floor_id']
           
          if((data1.get('room_id') is None) or ((data1.get('room_id') is not  None) and (len(data1['room_id']) <= 0))):
             output_str += ", room_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             room = data1['room_id']
          
          if((data1.get('premise_id') is None) or ((data1.get('premise_id') is not  None) and (len(data1['premise_id']) <= 0))):
             output_str += ", premise_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             premise = data1['premise_id']

          if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
             output_str += ", city_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             city = data1['city_id']
       
          if((data1.get('unit_id') is None) or ((data1.get('unit_id') is not  None) and (len(data1['unit_id']) <= 0))):
             output_str += ", unit_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             uid = data1['unit_id']
          #DB
       
          sqlq2="SELECT * FROM AC_Power_Units WHERE PMacid='%s' and ChangeBy='%s'" %(pmacid,username)
          cursor.execute(sqlq2)
          results2 = cursor.fetchone()
          #print "mapping data from db: ",results2
          if results2 > 0:
             #print 'maping data exits......... so quiting ..Thanks'
             
             
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=power unit exits"}' %(sid)
             ##return mqttc.publish('jts/oyo/error',output)
          else:
             #print 'start mapping .......'
             add_rec1=cursor.execute("""INSERT INTO AC_Power_Units(CityId,FloorId,RoomId,PremiseId,UnitId,PMacid,PName,Abrivation,ChangeBy,ChangeDate,StatusActive) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(city,floor,room,premise,uid,pmacid,pname,abrivation,username,date,True))
             db.commit()
             if add_rec1 > 0:
                #print 'power  mac rec mapped in DB.'
                output = '{"function":"add_unit","session_id":"%s","error_code":"0", "Response":"Successfully added Power Unit "}' %(sid)
	        ##return mqttc.publish('jts/oyo/error',output)
                cursor.close()
                db.close()
             else:
                
                
                output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=Falied to add power Units"}' %(sid)
                ##return mqttc.publish('jts/oyo/error',output)
 

    except (AttributeError, MySQLdb.OperationalError):
        pass
        ##return mqttc.publish('jts/oyo/error','MySQL server has gone away')
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            ##return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]) )
        except IndexError:
            print "MySQL Error: %s" % str(e)
            ##return mqttc.publish('jts/oyo/error',str(e)) 
       
    except Exception, e:  
        output = '{"function":"add_unit","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to add the unit"}' %(sid) 
        ##return mqttc.publish('jts/oyo/error',output)

######################## add_new_unit #########################
def add_new_unit(mosq,obj,msg):
    #print "add_new_unit......."
    
    output_str = "add_new_unit - Unable to Authenticate/add_new_unit... " 
    #print "this is  len of payload: ",len(str(msg.payload))
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       pass
       ##return mqttc.publish('jts/oyo/error','{"function":"add_new_unit","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:
       
       db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
       cursor = db.cursor()
       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"add_new_unit","error_code":"2", "error_desc": "Response=%s"}' %output_str
          ##return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']

       if(data1.get('username') is None):
          output_str += ",username is mandatory"
          output = '{"function":"add_new_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output)
       
       if(data1.get('password') is None):
          output_str += ",password is mandatory"
          output = '{"function":"add_new_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output)

       username    = data1['username']
       password    = data1['password']
       sqlq = "SELECT * FROM AC_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
       #sqlq = "SELECT * FROM Oyo_Users"
       #print "Checking Credentials : : ",sqlq
       cursor.execute(sqlq)
       results = cursor.fetchone()
       #print "Checking Data exists or not : ",results
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          
          output_str += ",Your not Autherize to Register a Device"
          output = '{"function":"add_new_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output)
       dummyvalue=id_generator()     
       add_rec1=cursor.execute("""INSERT INTO AC_Units(UnitDesc,ChangeBy,ChangeDate,StatusActive) VALUES (%s,%s,%s,%s)""",(str(dummyvalue),username,date,True))
       db.commit() 
       if add_rec1 > 0:
          sqlq = "SELECT UnitId FROM AC_Units WHERE UnitDesc='"+dummyvalue+"'"
          cursor.execute(sqlq)
          results = cursor.fetchone()
          if results > 0 :
             output = '{"error_code":"0","function":"add_new_unit","session_id":"%s","Response":"Succesfully Generated id for new unit","Unit_id":"%s"}' %(sid,results[0])
             
             cursor.close()
             db.close()            
             ##return mqttc.publish('jts/oyo/error',output)  
          else:
             output_str += ",Unable to get id"
             output = '{"function":"add_new_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             ##return mqttc.publish('jts/oyo/error',output)
       else:
          output_str += ",Unable to generate id"
          output = '{"function":"add_new_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output)


    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            ##return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]) )
        except IndexError:
            print "MySQL Error: %s" % str(e)
            ##return mqttc.publish('jts/oyo/error',str(e)) 
       
    except Exception, e:  
        output = '{"function":"add_new_unit","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to generate unit id"}' %(sid) 
        ##return mqttc.publish('jts/oyo/error',output)
################### update sensor units value ##################
def update_sensor_data(mosq,obj,msg):
    #print "update_sensor_data........ "
    
    output_str = "update_sensor_data - Unable to Authenticate/update_sensor_data... " 
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M')
    #print "time",date
    #date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       pass
       ##return mqttc.publish('jts/oyo/error','{"error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}' )
    try:
       db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
       cursor = db.cursor()
       imac=False
       omac=False
       pmac=False  
       

       if((data1.get('macid') is None) or ((data1.get('macid') is not  None) and (len(data1['macid']) <= 0))):
           output_str += ", macid is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           ##return mqttc.publish('jts/oyo/error',output)
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
      

       if imac==False and omac==False and pmac==False:
          output = '{"error_code":"2", "error_desc": "Response= macid is neither input,operate nor power unit.please Configure it."}'
          ##return mqttc.publish('jts/oyo/error',output)

       if omac == True:
          #print "this in op mac id " 
          if((data1.get('Fan') is None) or ((data1.get('Fan') is not  None) and (len(data1['Fan']) <= 0))):
              output_str += ", Fan is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
          else:
             fan = data1['Fan'] 
 
          if((data1.get('Status') is None) or ((data1.get('Status') is not  None) and (len(data1['Status']) <= 0))):
              output_str += ", Status is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
          else:
             status = data1['Status'] 

          
          if((data1.get('setTemp') is None) or ((data1.get('setTemp') is not  None) and (len(data1['setTemp']) <= 0))):
              output_str += ", settemp is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
          else:
             settemp = data1['setTemp']

          if((data1.get('V') is None) or ((data1.get('V') is not  None) and (len(data1['V']) <= 0))):
              output_str += ", V is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
          else:
             voltage = data1['V']

          if((data1.get('P') is None) or ((data1.get('P') is not  None) and (len(data1['P']) <= 0))):
              output_str += ", P is  mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
          else:
             power = data1['P']

          if((data1.get('ME') is None) or ((data1.get('ME') is not  None) and (len(data1['ME']) <= 0))):
              output_str += ", ME is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
          else:
              me = data1['ME'] 

          if((data1.get('I') is None) or ((data1.get('I') is not  None) and (len(data1['I']) <= 0))):
              output_str += ", I is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
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
             upd_rec=cursor.execute ("""UPDATE AC_Operate_Units SET Fan=%s,Status=%s,SetTemp=%s,Voltage=%s,Power=%s,Current=%s,MotionE=%s,ChangeDate=%s,ChangeBy=%s WHERE OpId=%s""", (fan,status,settemp,voltage,power,current,me,date,changeby,unitid))
             db.commit()
             #print upd_rec
             add_rec2=cursor.execute("""INSERT INTO AC_Operate_History(OpId,OpMacid,Fan,Status,SetTemp,Voltage,Power,Current,MotionE,ChangeBy,ChangeDate,UnitId) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(unitid,macid,fan,status,settemp,voltage,power,current,me,changeby,date,uid))
             db.commit()
             #print add_rec2
             if len(str(upd_rec)) > 0 and len(str(add_rec2)):
                #print "inside"
                output = '{"error_code":"0", "Response":"Succesfully updated Operate unit details"}' 
                
                cursor.close()
                db.close()      
                ##return mqttc.publish('jts/oyo/error',output)
             else:
                output = '{"error_code":"2", "error_desc": "Response=Unable to update Operate unit details"}'
                
                
                ##return mqttc.publish('jts/oyo/error',output)
    
          else:
             #print 'mac id not avilable , so configure....Thanks.'
             output = '{"error_code":"2", "error_desc": "Response= macid not exists.please Configure it."}'
             ##return mqttc.publish('jts/oyo/error',output)         

 
       if imac == True :
          #print "this in ip mac id "
          if((data1.get('temp') is None) or ((data1.get('temp') is not  None) and (len(data1['temp']) <= 0))):
              output_str += ", temp is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
          else:
             
             if data1.get('temp')=='-127.0':
                output_str += ", temp is  wrong"
                output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
                ##return mqttc.publish('jts/oyo/error',output)
             else:
                temp = data1['temp']
             
             #temp = data1['temp']
       
          if(data1.get('motion') is None):
              output_str += ", motion is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
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
                cursor.close()
                db.close()   
                
                ##return mqttc.publish('jts/oyo/error',output)
             else:
                output = '{"error_code":"2", "error_desc": "Response=Unable to update temp"}'
                
                
                ##return mqttc.publish('jts/oyo/error',output)
             
             
          else:
             #print 'mac id not avilable , so configure....Thanks.'
             output = '{"error_code":"2", "error_desc": "Response= input unit not exists.please Configure it."}'
             ##return mqttc.publish('jts/oyo/error',output)

       if pmac == True:
          

          if((data1.get('V') is None) or ((data1.get('V') is not  None) and (len(data1['V']) <= 0))):
              output_str += ", V is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
          else:
             voltage = data1['V']

          if((data1.get('P') is None) or ((data1.get('P') is not  None) and (len(data1['P']) <= 0))):
              output_str += ", P is  mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
          else:
             power = data1['P']


          if((data1.get('I') is None) or ((data1.get('I') is not  None) and (len(data1['I']) <= 0))):
              output_str += ", I is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
          else:
              current = data1['I']
          '''
          if((data1.get('Time') is None) or ((data1.get('Time') is not  None) and (len(data1['Time']) <= 0))):
              date=date
              
              #output_str += ", I is mandatory"
              #output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
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
             upd_rec=cursor.execute ("""UPDATE AC_Power_Units SET Voltage=%s,Power=%s,Current=%s,ChangeDate=%s,ChangeBy=%s WHERE PId=%s""", (voltage,power,current,date,changeby,unitid))
             db.commit()
             add_rec2=cursor.execute("""INSERT INTO AC_Power_History(PId,PMacid,Voltage,Power,Current,ChangeBy,ChangeDate,UnitId) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",(unitid,macid,voltage,power,current,changeby,date,uid))
             db.commit()
             if len(str(upd_rec)) > 0 and len(str(add_rec2)):
                output = '{"error_code":"0", "Response":"Succesfully updated temp"}' 
                
                
                ##return mqttc.publish('jts/oyo/error',output)
             else:
                output = '{"error_code":"2", "error_desc": "Response=Unable to update temp"}'
                
                
                ##return mqttc.publish('jts/oyo/error',output)
    
          else:
             #print 'mac id not avilable , so configure....Thanks.'
             output = '{"error_code":"2", "error_desc": "Response= macid not exists.please Configure it."}'
             ##return mqttc.publish('jts/oyo/error',output)         

    
    except (AttributeError, MySQLdb.OperationalError):
        pass
        ##return mqttc.publish('jts/oyo/error','MySQL server has gone away')
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            ##return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]))
        except IndexError:
            print "MySQL Error: %s" % str(e)
            ##return mqttc.publish('jts/oyo/error',str(e)) 
    except Exception, e:
        
        
        output = '{"error_code":"3", "error_desc": "Response=Failed to add the temp"}'
        ##return mqttc.publish('jts/oyo/error',output)

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
       if((data0.get('Data') is None) or ((data0.get('Data') is not  None) and (len(data0['Data']) <= 0))):
           output_str += ", Data is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           data1 = data0['Data']
       #data1 = data0['Data']
       #print data1
    except ValueError:
       pass
       ##return mqttc.publish('jts/oyo/error','{"error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}' )
    try:
       db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
       cursor = db.cursor()
       imac=False
       omac=False
       pmac=False  
       

       if((data1.get('macid') is None) or ((data1.get('macid') is not  None) and (len(data1['macid']) <= 0))):
           output_str += ", macid is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           ##return mqttc.publish('jts/oyo/error',output)
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
      

       if imac==False and omac==False and pmac==False:
          output = '{"error_code":"2", "error_desc": "Response= macid is neither input,operate nor power unit.please Configure it."}'
          ##return mqttc.publish('jts/oyo/error',output)

       if omac == True:
          #print "this in op mac id " 
          if((data1.get('Fan') is None) or ((data1.get('Fan') is not  None) and (len(data1['Fan']) <= 0))):
              output_str += ", Fan is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
          else:
             fan = data1['Fan'] 
 
          if((data1.get('Status') is None) or ((data1.get('Status') is not  None) and (len(data1['Status']) <= 0))):
              output_str += ", Status is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
          else:
             status = data1['Status'] 

          
          if((data1.get('setTemp') is None) or ((data1.get('setTemp') is not  None) and (len(data1['setTemp']) <= 0))):
              output_str += ", settemp is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
          else:
             settemp = data1['setTemp']

          if((data1.get('V') is None) or ((data1.get('V') is not  None) and (len(data1['V']) <= 0))):
              output_str += ", V is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
          else:
             voltage = data1['V']

          if((data1.get('P') is None) or ((data1.get('P') is not  None) and (len(data1['P']) <= 0))):
              output_str += ", P is  mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
          else:
             power = data1['P']

          if((data1.get('ME') is None) or ((data1.get('ME') is not  None) and (len(data1['ME']) <= 0))):
              output_str += ", ME is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
          else:
              me = data1['ME'] 

          if((data1.get('I') is None) or ((data1.get('I') is not  None) and (len(data1['I']) <= 0))):
              output_str += ", I is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
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
             upd_rec=cursor.execute ("""UPDATE AC_Operate_Units SET Fan=%s,Status=%s,SetTemp=%s,Voltage=%s,Power=%s,Current=%s,MotionE=%s,ChangeDate=%s,ChangeBy=%s WHERE OpId=%s""", (fan,status,settemp,voltage,power,current,me,date,changeby,unitid))
             db.commit()
             #print upd_rec
             add_rec2=cursor.execute("""INSERT INTO AC_Operate_History(OpId,OpMacid,Fan,Status,SetTemp,Voltage,Power,Current,MotionE,ChangeBy,ChangeDate,UnitId) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(unitid,macid,fan,status,settemp,voltage,power,current,me,changeby,date,uid))
             db.commit()
             #print add_rec2
             if len(str(upd_rec)) > 0 and len(str(add_rec2)):
                #print "inside"
                output = '{"error_code":"0", "Response":"Succesfully updated Operate unit details"}' 
                
                
                ##return mqttc.publish('jts/oyo/error',output)
             else:
                output = '{"error_code":"2", "error_desc": "Response=Unable to update Operate unit details"}'
                
                
                ##return mqttc.publish('jts/oyo/error',output)
    
          else:
             #print 'mac id not avilable , so configure....Thanks.'
             output = '{"error_code":"2", "error_desc": "Response= macid not exists.please Configure it."}'
             ##return mqttc.publish('jts/oyo/error',output)         

 
       if imac == True :
          #print "this in ip mac id "
          if((data1.get('temp') is None) or ((data1.get('temp') is not  None) and (len(data1['temp']) <= 0))):
              output_str += ", temp is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
          else:
             
             if data1.get('temp')=='-127.0':
                output_str += ", temp is  wrong"
                output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
                ##return mqttc.publish('jts/oyo/error',output)
             else:
                temp = data1['temp']
             
             #temp = data1['temp']
       
          if(data1.get('motion') is None):
              output_str += ", motion is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
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
                
                
                ##return mqttc.publish('jts/oyo/error',output)
             else:
                output = '{"error_code":"2", "error_desc": "Response=Unable to update temp"}'
                
                
                ##return mqttc.publish('jts/oyo/error',output)
             
             
          else:
             #print 'mac id not avilable , so configure....Thanks.'
             output = '{"error_code":"2", "error_desc": "Response= input unit not exists.please Configure it."}'
             ##return mqttc.publish('jts/oyo/error',output)

       if pmac == True:
          

          if((data1.get('V') is None) or ((data1.get('V') is not  None) and (len(data1['V']) <= 0))):
              output_str += ", V is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
          else:
             voltage = data1['V']

          if((data1.get('P') is None) or ((data1.get('P') is not  None) and (len(data1['P']) <= 0))):
              output_str += ", P is  mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
          else:
             power = data1['P']


          if((data1.get('I') is None) or ((data1.get('I') is not  None) and (len(data1['I']) <= 0))):
              output_str += ", I is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
          else:
              current = data1['I']
          '''
          if((data1.get('Time') is None) or ((data1.get('Time') is not  None) and (len(data1['Time']) <= 0))):
              date=date
              
              #output_str += ", I is mandatory"
              #output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              ##return mqttc.publish('jts/oyo/error',output)
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
             upd_rec=cursor.execute ("""UPDATE AC_Power_Units SET Voltage=%s,Power=%s,Current=%s,ChangeDate=%s,ChangeBy=%s WHERE PId=%s""", (voltage,power,current,date,changeby,unitid))
             db.commit()
             add_rec2=cursor.execute("""INSERT INTO AC_Power_History(PId,PMacid,Voltage,Power,Current,ChangeBy,ChangeDate,UnitId) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",(unitid,macid,voltage,power,current,changeby,date,uid))
             db.commit()
             if len(str(upd_rec)) > 0 and len(str(add_rec2)):
                output = '{"error_code":"0", "Response":"Succesfully updated temp"}' 
                
                
                ##return mqttc.publish('jts/oyo/error',output)
             else:
                output = '{"error_code":"2", "error_desc": "Response=Unable to update temp"}'
                
                
                ##return mqttc.publish('jts/oyo/error',output)
    
          else:
             #print 'mac id not avilable , so configure....Thanks.'
             output = '{"error_code":"2", "error_desc": "Response= macid not exists.please Configure it."}'
             ##return mqttc.publish('jts/oyo/error',output)         

    
    except (AttributeError, MySQLdb.OperationalError):
        pass
        ##return mqttc.publish('jts/oyo/error','MySQL server has gone away')    
    except MySQLdb.Error, e:
        try:
            #pass
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            ##return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]))
        except IndexError:
            print "MySQL Error: %s" % str(e)
            ##return mqttc.publish('jts/oyo/error',str(e)) 
    except Exception, e:
        
        
        output = '{"error_code":"3", "error_desc": "Response=Failed to add the temp"}'
        ##return mqttc.publish('jts/oyo/error',output)


####################### set temp by city/Premise #################################
def set_temp(mosq,obj,msg):
    output_str = "set_temp - Unable to Authenticate/set_temp... " 
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       pass
       ##return mqttc.publish('jts/oyo/error','{"function":"set_temp","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:
       db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
       cursor = db.cursor()
       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"set_temp","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          ##return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']
       if(data1.get('username') is None):
          output_str += ",username is mandatory"
          output = '{"function":"set_temp","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output)
       if(data1.get('password') is None):
          output_str += ",passsword is mandatory"
          output = '{"function":"set_temp","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output)

       username    = data1['username']
       password    = data1['password']
       sqlq = "SELECT * FROM AC_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
       #sqlq = "SELECT * FROM Oyo_Users"
       #print "Checking Credentials : : ",sqlq
       cursor.execute(sqlq)
       results = cursor.fetchone()
       #print "Checking Data exists or not : ",results
       if results > 0:
          pass
          #print 'Login Data Existed'
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to set temp"
          output = '{"function":"set_temp","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          ##return mqttc.publish('jts/oyo/error',output)
    
       TempCity=False
       TempPremise=False
           
        
       if((data1.get('setTemp') is None) or ((data1.get('setTemp') is not  None) and (len(data1['setTemp']) <= 0))):
           output_str += ", setTemp is mandatory"
           output = '{"function":"set_temp","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           settemp = data1['setTemp']

       if((data1.get('type') is None) or ((data1.get('type') is not  None) and (len(data1['type']) <= 0))):
           output_str += ", type is mandatory"
           output = '{"function":"set_temp","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           ##return mqttc.publish('jts/oyo/error',output)
       else:
           type1 = data1['type'] 
           if type1=="City":
              TempCity=True
           if type1=="Premise":
              TempPremise=True
       if TempCity==True:

          if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
              output_str += ", city_id is mandatory"
              output = '{"function":"set_temp","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
              ##return mqttc.publish('jts/oyo/error',output)
          else:
              cid = data1['city_id']
          
          sqlq1 = "update AC_City set setTemp='%s' where CityId='%s'" %(settemp,cid)
          results = cursor.execute(sqlq1)
          db.commit()
          #print results
          if results > 0:
             sqlq11 = "select PremiseId from AC_Premise where CityId='%s'" %(cid)
             cursor.execute(sqlq11)
             results11 = cursor.fetchall()
             #print results11
             if results11 > 0:
                for pid in results11:
                    sqlq1 = "update AC_Premise set setTemp='%s' where PremiseId='%s'" %(settemp,pid[0])
                    cursor.execute(sqlq1)
                    db.commit()
             sqlq1 = "select OpMacid from AC_Operate_Units where CityId='%s'" %(cid)
             cursor.execute(sqlq1)
             results1 = cursor.fetchall()
             #print results1
             if results1 > 0:
                for mac in results1:
                    #print mac[0]
                    output= '{"sm":"Server","dm":"%s","topic":"jts/GenAC/v_0_0_2/Data/%s","Data":{"setTemp":"%s"}}' %(mac[0],mac[0],settemp)
                    #print output
                    topic='jts/GenAC/v_0_0_2/Data/%s' %(mac[0])
                    #mqttc.publish(topic,output)

             output = '{"function":"set_temp","session_id":"%s","error_code":"0", "Response":"Successfully updated"}' %(sid)
             ##return mqttc.publish('jts/oyo/error',output)   

       if TempPremise==True:

          if((data1.get('premise_id') is None) or ((data1.get('premise_id') is not  None) and (len(data1['premise_id']) <= 0))):
               output_str += ", premise_id is mandatory"
               output = '{"function":"set_temp","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
               ##return mqttc.publish('jts/oyo/error',output)
          else:
               pid = data1['premise_id']

          sqlq1 = "update AC_Premise set setTemp='%s' where PremiseId='%s'" %(settemp,pid)
          results = cursor.execute(sqlq1)
          db.commit()
          #print results
          if results > 0:
             sqlq1 = "select OpMacid from AC_Operate_Units where PremiseId='%s'" %(pid)
             cursor.execute(sqlq1)
             results1 = cursor.fetchall()
             #print results1
             if results1 > 0:
                for mac in results1:
                    #print mac[0]
                    output= '{"sm":"Server","dm":"%s","topic":"jts/GenAC/v_0_0_2/Data/%s","Data":{"setTemp":"%s"}}' %(mac[0],mac[0],settemp)
                    #print output
                    topic='jts/GenAC/v_0_0_2/Data/%s' %(mac[0])
                    mqttc.publish(topic,output)

             output = '{"function":"set_temp","session_id":"%s","error_code":"0", "Response":"Successfully updated"}' %(sid)
             ##return mqttc.publish('jts/oyo/error',output)
    except (AttributeError, MySQLdb.OperationalError):
        pass
        #return mqttc.publish('jts/oyo/error','MySQL server has gone away') 
    except MySQLdb.Error, e:
        try:
            pass
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            ##return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1])) 
        except IndexError:
            print "MySQL Error: %s" % str(e)
            #return mqttc.publish('jts/oyo/error',str(e))
    except Exception, e:
        output = '{"function":"set_temp","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to add the User"}' %(sid)
        #return mqttc.publish('jts/oyo/error',output)





################## publish response #################################################
def on_publish(client, userdata, result):
        #print "data published \n"
        pass

def on_connect(client, userdata, flags, rc):
    #print("Connected with result code "+str(rc))
    mqttc.subscribe("jts/#")
def on_disconnect(client, userdata, rc):
    if rc != 0:
        print "Unexpected MQTT disconnection. Will auto-reconnect"
        #mqttc.subscribe("jts/oyo/#")

######################### mqtt methods ####################################
mqttc = mqtt.Client()

#################### esp calls ####################################
#mqttc.message_callback_add('jts/oyo/temp',temp1)
#mqttc.message_callback_add('jts/oyo/op',operations)
######## web calls###############################
#mqttc.message_callback_add('jts/oyo/validate_login',login)
#mqttc.message_callback_add('jts/oyo/get_roles',get_roles)
mqttc.message_callback_add('jts/oyo/add_user',add_user)
#mqttc.message_callback_add('jts/oyo/get_clients',get_clients)
#mqttc.message_callback_add('jts/oyo/get_images',get_images)


#mqttc.message_callback_add('jts/oyo/get_city',get_city)
mqttc.message_callback_add('jts/oyo/add_city',add_city)
#mqttc.message_callback_add('jts/oyo/delete_city',delete_city)
mqttc.message_callback_add('jts/oyo/update_city',update_city)

#mqttc.message_callback_add('jts/oyo/get_city_premise',get_premise)
mqttc.message_callback_add('jts/oyo/add_premise',add_premise)
#mqttc.message_callback_add('jts/oyo/delete_premise',delete_premise)
mqttc.message_callback_add('jts/oyo/update_premise',update_premise)

mqttc.message_callback_add('jts/oyo/add_floor',add_floor)
mqttc.message_callback_add('jts/oyo/update_floor',update_floor)
#mqttc.message_callback_add('jts/oyo/get_floors',get_floors)
#mqttc.message_callback_add('jts/oyo/delete_floor',delete_floor)

#mqttc.message_callback_add('jts/oyo/get_rooms',get_rooms)
mqttc.message_callback_add('jts/oyo/add_room',add_room)
mqttc.message_callback_add('jts/oyo/update_room',update_room)
#mqttc.message_callback_add('jts/oyo/delete_room',delete_room)

mqttc.message_callback_add('jts/oyo/add_unit',add_unit)
mqttc.message_callback_add('jts/oyo/add_new_unit',add_new_unit)
#mqttc.message_callback_add('jts/oyo/get_units_all',get_units)
#mqttc.message_callback_add('jts/oyo/delete_unit',delete_unit)
#mqttc.message_callback_add('jts/oyo/get_units_test',get_units_test)
#mqttc.message_callback_add('jts/oyo/get_unit_details',get_unit_details)
#mqttc.message_callback_add('jts/oyo/get_power',get_power)
#mqttc.message_callback_add('jts/oyo/get_temp_history',get_temp_history)
#mqttc.message_callback_add('jts/oyo/delete_unit_utype',delete_unit_utype)
#mqttc.message_callback_add('jts/oyo/get_graph_history',get_graph_history)
#mqttc.message_callback_add('jts/oyo/get_saved_units',get_saved_units)
#mqttc.message_callback_add('jts/oyo/get_graph_history_mobile',get_graph_history_mobile)
#mqttc.message_callback_add('jts/oyo/add_roles',add_roles)
#mqttc.message_callback_add('jts/oyo/mod_roles',mod_roles)
#mqttc.message_callback_add('jts/oyo/del_roles',del_roles)
#mqttc.message_callback_add('jts/oyo/test01',test01)

#mqttc.message_callback_add('jts/oyo/get_room_units',get_room_units)
mqttc.message_callback_add('jts/oyo/temp',update_sensor_data)
########################### version 2 API's ##############################
mqttc.message_callback_add('jts/GenAC/v_0_0_2/Data/2s',update_sensor_data_lora)
mqttc.message_callback_add('jts/GenAC/v_0_0_2/Data/2s/set_temp',set_temp)

mqttc.on_publish = on_publish
mqttc.username_pw_set('esp', 'ptlesp01')
mqttc.connect("cld003.jts-prod.in", 1883, 60)
mqttc.on_disconnect = on_disconnect
mqttc.on_connect = on_connect
mqttc.subscribe("jts/#")
mqttc.loop_forever()
#mqttc.username_pw_set('esp', 'ptlesp01')




