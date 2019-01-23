import paho.mqtt.client as mqtt
import MySQLdb
import json
#import datetime
import sys
#import time
from datetime import datetime
import requests
###########################################################
'''
############### get sunset and sunrise ###################
def sunreq(mosq,obj,msg):
    output_str = "register - Unable to Authenticate/register... "
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('macid') is None) or ((data1.get('macid') is not  None) and (len(data1['macid']) <= 0))):
           output_str += ", macid is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           macid = data1['macid']

       if((data1.get('Lat') is None) or ((data1.get('Lat') is not  None) and (len(data1['Lat']) <= 0))):
           output_str += ", Lat is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           lat = data1['Lat']

       if((data1.get('Lon') is None) or ((data1.get('Lon') is not  None) and (len(data1['Lon']) <= 0))):
           output_str += ", Lon is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           lon = data1['Lon']

       r = requests.get('http://api.sunrise-sunset.org/json?lat=%s&lng=%s' %(lat,lon))
       res = r.text   
       res = json.loads(res)
       sunrise = res['results']['sunrise'] 
       sunset = res['results']['sunset']
       ctb = res['results']['civil_twilight_begin'] 
       cte = res['results']['civil_twilight_end']
       output = '{"sr":"%s", "ss":"%s","ctb":"%s","cte":"%s"}'%(sunrise,sunset,ctb,cte)
       print output
       return mqttc.publish('jts/dtd/'+macid,output)

    except Exception, e:
        
        output = '{"error_code":"3", "error_desc": "Response=Failed to add the Register"}'
        return mqttc.publish('jts/oyo/error',output)
'''
##################### login #############################################
def login(mosq,obj,msg):
    db = MySQLdb.connect("localhost","dusktodawn","ptldusktodawn","dtdDB")
    cursor = db.cursor()
    output_str = "login - Unable to Authenticate/login... "
    data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"function":"login","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')

    try:
       if(data1.get('username') is None):
          output_str += ",username is mandatory"
          output = '{"function":"login","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/dtd/response',output)

       if(data1.get('password') is None):
          output_str += ",password is mandatory"
          output = '{"function":"login","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/dtd/response',output)

       username    = data1['username']
       password    = data1['password']

       sqlq = "SELECT * FROM DtD_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       #sqlq = "SELECT * FROM Oyo_Users"
       #print "Checking Credentials : : ",sqlq
       cursor.execute(sqlq)
       results = cursor.fetchone()
       #print "Checking Data exists or not : ",results
       if results > 0:
          #print 'Login Data Existed'
          output = '{"function":"login","error_code":"0", "Response":"Successfully Authenticated for user: %s"}' %username
          return mqttc.publish('jts/dtd/response',output)
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          cursor.close()
          db.close()
          output = '{"function":"login","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/dtd/response',output)

    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"login","error_code":"3", "error_desc": "Response=Failed to login"}'
        return mqttc.publish('jts/dtd/response',output)

################# User Registration ###############################
def register(mosq,obj,msg):
    db = MySQLdb.connect("localhost","dusktodawn","ptldusktodawn","dtdDB")
    cursor = db.cursor()
    output_str = "register - Unable to Authenticate/register... "
    #print "this is register string : ",str(msg.payload)
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/dtd/response','{"function":"register","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"register","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"register","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           password = data1['password']

       if((data1.get('address') is None) or ((data1.get('address') is not  None) and (len(data1['address']) <= 0))):
           output_str += ", address is mandatory"
           output = '{"function":"register","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           address = data1['address']

       if((data1.get('company') is None) or ((data1.get('company') is not  None) and (len(data1['company']) <= 0))):
           output_str += ", company is mandatory"
           output = '{"function":"register","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           company = data1['company']

       if((data1.get('mobile') is None) or ((data1.get('mobile') is not  None) and (len(data1['mobile']) <= 0))):
           output_str += ", mobile is mandatory"
           output = '{"function":"register","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           mobile = data1['mobile']

       if((data1.get('email') is None) or ((data1.get('email') is not  None) and (len(data1['email']) <= 0))):
           output_str += ", email is mandatory"
           output = '{"function":"register","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           email = data1['email']
       
       sqlq1 = "SELECT * FROM DtD_Users WHERE Username='"+username+"'"
       cursor.execute(sqlq1)
       results = cursor.fetchone()
       
       if results > 0:
          output_str = "The user already Registered"
          output = '{"function":"register","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/dtd/response',output)
          
       else:
          add_rec=cursor.execute("""INSERT INTO DtD_Users(Username,Password,Address,Company,Mobile,Email,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s)""",(username,password,address,company,mobile,email,date))
          db.commit()
          if add_rec > 0:
             output = '{"function":"register","error_code":"0", "Response":"Successfull added the user : %s"}'%username
             return mqttc.publish('jts/dtd/response',output)
             
          else:
             output = '{"function":"register","error_code":"-2", "error_desc": "Response= Unable to add user"}'
             return mqttc.publish('jts/dtd/response',output)

    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"register","error_code":"3", "error_desc": "Response=Failed to add the user"}'
        return mqttc.publish('jts/dtd/response',output)
    finally:
        cursor.close()
        db.close()
#################### add units ####################################
def add_units1(mosq,obj,msg):
    db = MySQLdb.connect("localhost","dusktodawn","ptldusktodawn","dtdDB")
    cursor = db.cursor()
    output_str = "add_units - Unable to Authenticate/add_units... "
    #print "this is register string : ",str(msg.payload)
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/dtd/response','{"function":"add_units1","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"add_units1","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"add_units1","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM DtD_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to add unit"
          output = '{"function":"add_units1","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/dtd/response',output)      
       ''' 
       if((data1.get('long') is None) or ((data1.get('long') is not  None) and (len(data1['long']) <= 0))):
           output_str += ", long is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           long1 = data1['long']

       if((data1.get('lat') is None) or ((data1.get('lat') is not  None) and (len(data1['lat']) <= 0))):
           output_str += ", lat is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           lat = data1['lat']
       '''
       if((data1.get('name') is None) or ((data1.get('name') is not  None) and (len(data1['name']) <= 0))):
           output_str += ", name is mandatory"
           output = '{"function":"add_units1","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           name = data1['name']

       if((data1.get('macid') is None) or ((data1.get('macid') is not  None) and (len(data1['macid']) <= 0))):
           output_str += ", macid is mandatory"
           output = '{"function":"add_units1","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           macid = data1['macid']       
       #sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"' AND Longitude='"+long1+"' AND Latitude='"+lat+"'"
       sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"'"
       cursor.execute(sqlq1)
       results = cursor.fetchone()
       
       if results > 0:
          #print "unit existed"
          output_str = "The unit already Registered"
          output = '{"function":"add_units1","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/dtd/response',output)
          
       else:
          #print "exitig........"
          #add_rec=cursor.execute("""INSERT INTO DtD_Units(UnitDesc,Longitude,Latitude,Operation,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s)""",(name,float(long1),float(lat),'OFF',username,date))
          add_rec=cursor.execute("""INSERT INTO DtD_Units(UnitMac,UnitDesc,Operation,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s)""",(macid,name,'OFF',username,date))
          db.commit()
          if add_rec > 0:
             #print "inserted"
             output = '{"function":"add_units1","error_code":"0", "Response":"Successfully added the unit : %s"}'%name
             return mqttc.publish('jts/dtd/response',output)
             
          else:
             #print "insert filed"
             output = '{"function":"add_units1","error_code":"-2", "error_desc": "Response= Unable to add unit"}'
             return mqttc.publish('jts/dtd/response',output)
    except MySQLdb.Error, e:
        try:
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
        except IndexError:
            #print "MySQL Error: %s" % str(e)
            return "MySQL Error: %s" % str(e)
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"add_units1","error_code":"3", "error_desc": "Response=Failed to add the unit"}'
        return mqttc.publish('jts/dtd/response',output)
    finally:
        cursor.close()
        db.close()
################# up date unit details ##########################
def update_unit_details(mosq,obj,msg):
    db = MySQLdb.connect("localhost","dusktodawn","ptldusktodawn","dtdDB")
    cursor = db.cursor()
    output_str = "update_unit_details - Unable to Authenticate/update_unit_details... "
    #print "this is register string : ",str(msg.payload)
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/dtd/response','{"function":"update_unit_details","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"update_unit_details","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"update_unit_details","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM DtD_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to add unit"
          output = '{"function":"update_unit_details","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/dtd/response',output)      
        
       if((data1.get('long') is None) or ((data1.get('long') is not  None) and (len(data1['long']) <= 0))):
           output_str += ", long is mandatory"
           output = '{"function":"update_unit_details","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           long1 = data1['long']

       if((data1.get('lat') is None) or ((data1.get('lat') is not  None) and (len(data1['lat']) <= 0))):
           output_str += ", lat is mandatory"
           output = '{"function":"update_unit_details","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           lat = data1['lat']
       
       if((data1.get('unit_id') is None) or ((data1.get('unit_id') is not  None) and (len(data1['unit_id']) <= 0))):
           output_str += ", unit_id is mandatory"
           output = '{"function":"update_unit_details","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           unitid = data1['unit_id']

       if((data1.get('name') is None) or ((data1.get('name') is not  None) and (len(data1['name']) <= 0))):
           output_str += ", name is mandatory"
           output = '{"function":"update_unit_details","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           name = data1['name']

       if((data1.get('macid') is None) or ((data1.get('macid') is not  None) and (len(data1['macid']) <= 0))):
           output_str += ", macid is mandatory"
           output = '{"function":"update_unit_details","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           mac = data1['macid']

           #sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"' AND Longitude='"+long1+"' AND Latitude='"+lat+"'"
       sqlq1 = "SELECT * FROM DtD_Units WHERE UnitId='"+unitid+"'"
       cursor.execute(sqlq1)
       results = cursor.fetchone()
       
       if results > 0:
          #print "unit existed"
          #updq="UPDATE DtD_Units SET Longitude=%s,Latitude=%s,ChangeDate=%s WHERE UnitId=%s"
          #val=(long1,lat,date,name)
          upd_rec=cursor.execute ("""UPDATE DtD_Units SET UnitDesc=%s,UnitMac=%s,Longitude=%s,Latitude=%s,ChangeDate=%s WHERE UnitId=%s""", (name,mac,long1,lat,date,unitid))
          db.commit()         
          #print upd_rec
          if upd_rec > 0:
             #print "inserted"
             output = '{"function":"update_unit_details","error_code":"0", "Response":"Successfully update the unit"}'
             return mqttc.publish('jts/dtd/response',output)

          else:
             #print "insert filed"
             output = '{"function":"update_unit_details","error_code":"-2", "error_desc": "Response= Unable to update the unit"}'
             return mqttc.publish('jts/dtd/response',output)
                       
       else:
          #print "exitig........"
          output = '{"function":"update_unit_details","error_code":"-2", "error_desc": "Response= Unable to update the unit"}'
          return mqttc.publish('jts/dtd/response',output)
    except MySQLdb.Error, e:
        try:
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
        except IndexError:
            #print "MySQL Error: %s" % str(e)
            return "MySQL Error: %s" % str(e)
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"update_unit_details","error_code":"3", "error_desc": "Response=Failed to update the unit"}'
        return mqttc.publish('jts/dtd/response',output)
    finally:
        cursor.close()
        db.close()


################# get units ######################################
def get_units(mosq,obj,msg):
    db = MySQLdb.connect("localhost","dusktodawn","ptldusktodawn","dtdDB")
    cursor = db.cursor()
    output_str = "get_units - Unable to Authenticate/get_units... "
    #print "this is register string : ",str(msg.payload)
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/dtd/response','{"function":"get_units","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"get_units","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"get_units","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM DtD_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to add unit"
          output = '{"function":"get_units","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/dtd/response',output)
   
       #sqlq2 = "SELECT UnitId,UnitDesc,Longitude,Latitude,Operation FROM DtD_Units"
       sqlq2 = "SELECT UnitId,UnitDesc,UnitMac,Operation FROM DtD_Units"
       #print sqlq2
       cursor.execute(sqlq2)
       get_units_rec = cursor.fetchall()
       #print get_units_rec
       if(len(get_units_rec) > 0):
        #{
      
          output = '{"function":"get_units","error_code":"0", "Response":"Successfully got %d units", \n "get_units":' %len(get_units_rec)
          output += '['
          counter = 0
          for rec in get_units_rec:
          #{
             counter += 1
             if(counter == 1):
               #print "in counter"
               output += '{"unit_id":"%s","unit_desc":"%s","macid":"%s","operation":"%s"}' %(rec[0] ,rec[1],rec[2],rec[3])
             else:
               #print "in else counter"
               output += ',\n {"unit_id":"%s","unit_desc":"%s","macid":"%s","operation":"%s"}' %(rec[0] ,rec[1],rec[2],rec[3])
          #}
          output += ']\n'
          output += '}'
          return mqttc.publish('jts/dtd/response',output)
       else:
           output = '{"function":"get_units","error_code":"3", "error_desc": "Response=Failed to get the unit records, NO_DATA_FOUND"}'
           return mqttc.publish('jts/dtd/response',output)
     
    except MySQLdb.Error, e:
        try:
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
        except IndexError:
            #print "MySQL Error: %s" % str(e)
            return "MySQL Error: %s" % str(e)
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_units","error_code":"3", "error_desc": "Response=Failed to get the units"}'
        return mqttc.publish('jts/dtd/response',output)
    finally:
        cursor.close()
        db.close()

################# get unit details ################################
def get_unit_details(mosq,obj,msg):
    db = MySQLdb.connect("localhost","dusktodawn","ptldusktodawn","dtdDB")
    cursor = db.cursor()
    output_str = "get_unit_details- Unable to Authenticate/get_unit_details... "
    #print "this is register string : ",str(msg.payload)
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/dtd/response','{"function":"get_unit_details","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"get_unit_details","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"get_unit_details","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM DtD_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to add unit"
          output = '{"function":"get_unit_details","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/dtd/response',output)

       if((data1.get('unit_id') is None) or ((data1.get('unit_id') is not  None) and (len(data1['unit_id']) <= 0))):
           output_str += ", unit_id is mandatory"
           output = '{"function":"get_unit_details","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           name = data1['unit_id'] 

       sqlq2 = "SELECT UnitDesc,UnitMac,Longitude,Latitude,Operation FROM DtD_Units WHERE UnitId='"+name+"'"
       #sqlq2 = "SELECT UnitId,UnitDesc,Operation FROM DtD_Units"
       #print sqlq2
       cursor.execute(sqlq2)
       get_units_rec = cursor.fetchall()
       #print get_units_rec
       if(len(get_units_rec) > 0):
        #{
      
          output = '{"function":"get_unit_details","error_code":"0", "Response":"Successfully got %d units", \n "get_unit_details":' %len(get_units_rec)
          output += '['
          counter = 0
          for rec in get_units_rec:
          #{
             counter += 1
             if(counter == 1):
               
               output += '{"unit_desc":"%s","macid":"%s","long":"%s","lat":"%s","operation":"%s"}' %(rec[0] ,rec[1],rec[2],rec[3],rec[4])
             else:
              
               output += ',\n {"unit_desc":"%s","macid":"%s","long":"%s","lat":"%s","operation":"%s"}' %(rec[0] ,rec[1],rec[2],rec[3],rec[4])
          #}
          output += ']\n'
          output += '}'
          return mqttc.publish('jts/dtd/response',output)
       else:
           output = '{"function":"get_unit_details","error_code":"3", "error_desc": "Response=Failed to get the unit records, NO_DATA_FOUND"}'
           return mqttc.publish('jts/dtd/response',output)
     
    except MySQLdb.Error, e:
        try:
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
        except IndexError:
            #print "MySQL Error: %s" % str(e)
            return "MySQL Error: %s" % str(e)
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_unit_details","error_code":"3", "error_desc": "Response=Failed to get the unit details"}'
        return mqttc.publish('jts/dtd/response',output)
    finally:
        cursor.close()
        db.close()


############### delete unit ####################################
def delete_unit(mosq,obj,msg):
    req2 = requests.post(url = 'http://localhost:5903/DuskToDawnApp/delete_unit/', data = msg.payload,verify=False)
    res = req2.text
    res = json.loads(res)
    res["function"]="delete_unit" 
    res =json.dumps(res)
    #res = json.loads(res)
    #print "response fro cld: ",res
    return mqttc.publish('jts/dtd/response',str(res))
    '''
    db = MySQLdb.connect("localhost","dusktodawn","ptldusktodawn","dtdDB")
    cursor = db.cursor()
    output_str = "delete_unit- Unable to Authenticate/delete_unit... "
    #print "this is register string : ",str(msg.payload)
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/dtd/response','{"error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM DtD_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to delete unit"
          output = '{"error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/dtd/response',output)

       if((data1.get('unit_id') is None) or ((data1.get('unit_id') is not  None) and (len(data1['unit_id']) <= 0))):
           output_str += ", unit_id is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           name = data1['unit_id']

       sqlq2 = "DELETE FROM DtD_Units WHERE UnitId='"+name+"'"
       #sqlq2 = "SELECT UnitId,UnitDesc,Operation FROM DtD_Units"
       #print sqlq2
       get_del_rec=cursor.execute(sqlq2)
       db.commit()
       
       #print get_del_rec
       if get_del_rec > 0:
           output = '{"error_code":"0", "Response":"Successfully deleted the unit"}'
           return mqttc.publish('jts/dtd/response',output)
       else:
           output = '{"error_code":"3", "error_desc": "Response=Unable to  delete the unit "}'
           return mqttc.publish('jts/dtd/response',output)
     
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return None
        except IndexError:
            print "MySQL Error: %s" % str(e)
            return None
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"error_code":"3", "error_desc": "Response=Failed to delete the units"}'
        return mqttc.publish('jts/dtd/response',output)
    finally:
        cursor.close()
        db.close()
    '''
############### delete user ######################################
def delete_user(mosq,obj,msg):
    db = MySQLdb.connect("localhost","dusktodawn","ptldusktodawn","dtdDB")
    cursor = db.cursor()
    output_str = "delete_user- Unable to Authenticate/delete_user... "
    #print "this is register string : ",str(msg.payload)
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/dtd/response','{"function":"delete_user","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"delete_user","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"delete_user","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM DtD_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to delete unit"
          output = '{"function":"delete_user","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/dtd/response',output)

       if((data1.get('uname') is None) or ((data1.get('uname') is not  None) and (len(data1['uname']) <= 0))):
           output_str += ", uname is mandatory"
           output = '{"function":"delete_user","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           uname = data1['uname']

       sqlq2 = "DELETE FROM DtD_Users WHERE Username='"+uname+"'"
       #sqlq2 = "SELECT UnitId,UnitDesc,Operation FROM DtD_Units"
       #print sqlq2
       get_del_rec=cursor.execute(sqlq2)
       db.commit()
       
       #print get_del_rec
       if get_del_rec > 0:
           output = '{"function":"delete_user","error_code":"0", "Response":"Successfully deleted the user"}'
           return mqttc.publish('jts/dtd/response',output)
       else:
           output = '{"function":"delete_user","error_code":"3", "error_desc": "Response=Unable to  delete the user "}'
           return mqttc.publish('jts/dtd/response',output)
     
    except MySQLdb.Error, e:
        try:
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
        except IndexError:
            #print "MySQL Error: %s" % str(e)
            return "MySQL Error: %s" % str(e)
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"delete_user","error_code":"3", "error_desc": "Response=Failed to delete the user"}'
        return mqttc.publish('jts/dtd/response',output)
    finally:
        cursor.close()
        db.close()


########## add shedule ############################################
def add_shedule(mosq,obj,msg):
    db = MySQLdb.connect("localhost","dusktodawn","ptldusktodawn","dtdDB")
    cursor = db.cursor()
    output_str = "add_shedule - Unable to Authenticate/add_shedule... "
    #print "this is register string : ",str(msg.payload)
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/dtd/response','{"function":"add_shedule","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"add_shedule","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"add_shedule","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM DtD_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to add unit"
          output = '{"function":"add_shedule","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/dtd/response',output)      
        
       if((data1.get('type') is None) or ((data1.get('type') is not  None) and (len(data1['type']) <= 0))):
           output_str += ", type is mandatory"
           output = '{"function":"add_shedule","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           type1 = data1['type']

       if((data1.get('symbol') is None) or ((data1.get('symbol') is not  None) and (len(data1['symbol']) <= 0))):
           output_str += ", symbol is mandatory"
           output = '{"function":"add_shedule","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           symbol = data1['symbol']

       if((data1.get('hours') is None) or ((data1.get('hours') is not  None) and (len(data1['hours']) <= 0))):
           output_str += ", hours is mandatory"
           output = '{"function":"add_shedule","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           hours = data1['hours']

       if((data1.get('mns') is None) or ((data1.get('mns') is not  None) and (len(data1['mns']) <= 0))):
           output_str += ",mns is mandatory"
           output = '{"function":"add_shedule","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           mns = data1['mns'] 

       if((data1.get('unit_id') is None) or ((data1.get('unit_id') is not  None) and (len(data1['unit_id']) <= 0))):
           output_str += ",unit_id is mandatory"
           output = '{"function":"add_shedule","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           unitid = data1['unit_id']
       #print "came"
       add_rec=cursor.execute("""INSERT INTO DtD_Unit_Details(UnitId,Type,PosNeg,Hours,Mns,Status,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",(unitid,type1,symbol,hours,mns,'active',username,date))
       db.commit()
       #print "after commit"
       if add_rec > 0:
          #print "inserted"
          output = '{"function":"add_shedule","error_code":"0", "Response":"Successfully added the shedule"}'
          return mqttc.publish('jts/dtd/response',output)
             
       else:
          #print "insert filed"
          output = '{"function":"add_shedule","error_code":"-2", "error_desc": "Response= Unable to add unit"}'
          return mqttc.publish('jts/dtd/response',output)
    except MySQLdb.Error, e:
        try:
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
        except IndexError:
            #print "MySQL Error: %s" % str(e)
            return "MySQL Error: %s" % str(e)
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"add_shedule","error_code":"3", "error_desc": "Response=Failed to add the shedule"}'
        return mqttc.publish('jts/dtd/response',output)
    finally:
        cursor.close()
        db.close()

####################### get shedule ###############################
def get_shedule(mosq,obj,msg):
    db = MySQLdb.connect("localhost","dusktodawn","ptldusktodawn","dtdDB")
    cursor = db.cursor()
    output_str = "get_shedule - Unable to Authenticate/get_shedule... "
    #print "this is register string : ",str(msg.payload)
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/dtd/response','{"function":"get_shedule","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"get_shedule","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"get_shedule","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           password = data1['password']

       if((data1.get('unit_id') is None) or ((data1.get('unit_id') is not  None) and (len(data1['unit_id']) <= 0))):
           output_str += ", unit_id is mandatory"
           output = '{"function":"get_shedule","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           unitid = data1['unit_id']

       sqlq = "SELECT * FROM DtD_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to add shedule"
          output = '{"function":"get_shedule","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/dtd/response',output)
   
       #sqlq2 = "SELECT UnitId,UnitDesc,Longitude,Latitude,Operation FROM DtD_Units"
       sqlq2 = "SELECT UDId,Type,PosNeg,Hours,Mns,Status FROM DtD_Unit_Details WHERE UnitId='%s'" %(unitid)
       #print sqlq2
       cursor.execute(sqlq2)
       get_units_rec = cursor.fetchall()
       #print get_units_rec
       if(len(get_units_rec) > 0):
        #{
      
          output = '{"function":"get_shedule","error_code":"0", "Response":"Successfully got %d shedules", \n "get_shedule":' %len(get_units_rec)
          output += '['
          counter = 0
          sign=""
          for rec in get_units_rec:
	     if str(rec[2])=="null":
                sign=""
             else:
                sign=str(rec[2])
             counter += 1
             if(counter == 1):
               #print "in counter"
               output += '{"shedule_id":"%s","type":"%s","symbol":"%s","hours":"%s","mns":"%s","status":"%s"}' %(rec[0] ,rec[1],sign,rec[3],rec[4],rec[5])
             else:
               #print "in else counter"
               output += ',\n {"shedule_id":"%s","type":"%s","symbol":"%s","hours":"%s","mns":"%s","status":"%s"}' %(rec[0] ,rec[1],sign,rec[3],rec[4],rec[5])
          #}
          output += ']\n'
          output += '}'
          return mqttc.publish('jts/dtd/response',output)
       else:
           output = '{"function":"get_shedule","error_code":"3", "error_desc": "Response=Failed to get the shedule records, NO_DATA_FOUND"}'
           return mqttc.publish('jts/dtd/response',output)
     
    except MySQLdb.Error, e:
        try:
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
        except IndexError:
            #print "MySQL Error: %s" % str(e)
            return "MySQL Error: %s" % str(e)
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_shedule","error_code":"3", "error_desc": "Response=Failed to get the shedule"}'
        return mqttc.publish('jts/dtd/response',output)
    finally:
        cursor.close()
        db.close()
##################### update shedule #########################
def update_shedule(mosq,obj,msg):
    db = MySQLdb.connect("localhost","dusktodawn","ptldusktodawn","dtdDB")
    cursor = db.cursor()
    output_str = "update_shedule  - Unable to Authenticate/update_shedule... "
    #print "this is register string : ",str(msg.payload)
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/dtd/response','{"function":"update_shedule","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"update_shedule","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"update_shedule","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM DtD_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to add unit"
          output = '{"function":"update_shedule","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/dtd/response',output)      
        
       if((data1.get('shedule_id') is None) or ((data1.get('shedule_id') is not  None) and (len(data1['shedule_id']) <= 0))):
           output_str += ", shedule_id is mandatory"
           output = '{"function":"update_shedule","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           sheduleid = data1['shedule_id']
       if((data1.get('type') is None) or ((data1.get('type') is not  None) and (len(data1['type']) <= 0))):
           output_str += ", type is mandatory"
           output = '{"function":"update_shedule","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           type1 = data1['type']

       if((data1.get('symbol') is None) or ((data1.get('symbol') is not  None) and (len(data1['symbol']) <= 0))):
           output_str += ", symbol is mandatory"
           output = '{"function":"update_shedule","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           symbol = data1['symbol']
       if((data1.get('hours') is None) or ((data1.get('hours') is not  None) and (len(data1['hours']) <= 0))):
           output_str += ", hours is mandatory"
           output = '{"function":"update_shedule","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           hours = data1['hours']
       if((data1.get('mns') is None) or ((data1.get('mns') is not  None) and (len(data1['mns']) <= 0))):
           output_str += ", mns is mandatory"
           output = '{"function":"update_shedule","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           mns = data1['mns']
       #sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"' AND Longitude='"+long1+"' AND Latitude='"+lat+"'"
       sqlq1 = "SELECT * FROM DtD_Unit_Details WHERE UDId='"+sheduleid+"'"
       cursor.execute(sqlq1)
       results = cursor.fetchone()
       
       if results > 0:
          #print "unit existed"
          #updq="UPDATE DtD_Units SET Longitude=%s,Latitude=%s,ChangeDate=%s WHERE UnitId=%s"
          #val=(long1,lat,date,name)
          upd_rec=cursor.execute ("""UPDATE DtD_Unit_Details SET Type=%s,PosNeg=%s,Hours=%s,Mns=%s,ChangeDate=%s WHERE UDId=%s""", (type1,symbol,hours,mns,date,sheduleid))
          db.commit()         
          #print upd_rec
          if upd_rec > 0:
             #print "inserted"
             output = '{"function":"update_shedule","error_code":"0", "Response":"Successfully update the shedule"}'
             return mqttc.publish('jts/dtd/response',output)

          else:
             #print "insert filed"
             output = '{"function":"update_shedule","error_code":"-2", "error_desc": "Response= Unable to update the shedule"}'
             return mqttc.publish('jts/dtd/response',output)
                       
       else:
          #print "exitig........"
          output = '{"function":"update_shedule","error_code":"-2", "error_desc": "Response= Unable to update the shedule"}'
          return mqttc.publish('jts/dtd/response',output)
    except MySQLdb.Error, e:
        try:
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
        except IndexError:
            #print "MySQL Error: %s" % str(e)
            return "MySQL Error: %s" % str(e)
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"update_shedule","error_code":"3", "error_desc": "Response=Failed to update the shedule"}'
        return mqttc.publish('jts/dtd/response',output)
    finally:
        cursor.close()
        db.close()
#################### update shedule status #######################
def update_shedule_status(mosq,obj,msg):
    db = MySQLdb.connect("localhost","dusktodawn","ptldusktodawn","dtdDB")
    cursor = db.cursor()
    output_str = "update_shedule  - Unable to Authenticate/update_shedule... "
    #print "this is register string : ",str(msg.payload)
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/dtd/response','{"function":"update_shedule_status","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"update_shedule_status","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"update_shedule_status","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM DtD_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to add unit"
          output = '{"function":"update_shedule_status","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/dtd/response',output)      
        
       if((data1.get('shedule_id') is None) or ((data1.get('shedule_id') is not  None) and (len(data1['shedule_id']) <= 0))):
           output_str += ", shedule_id is mandatory"
           output = '{"function":"update_shedule_status","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           sheduleid = data1['shedule_id']
       if((data1.get('status') is None) or ((data1.get('status') is not  None) and (len(data1['status']) <= 0))):
           output_str += ", status is mandatory"
           output = '{"function":"update_shedule_status","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           status = data1['status']
          
       #sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"' AND Longitude='"+long1+"' AND Latitude='"+lat+"'"
       sqlq1 = "SELECT * FROM DtD_Unit_Details WHERE UDId='"+sheduleid+"'"
       cursor.execute(sqlq1)
       results = cursor.fetchone()
       
       if results > 0:
          #print "unit existed"
          #updq="UPDATE DtD_Units SET Longitude=%s,Latitude=%s,ChangeDate=%s WHERE UnitId=%s"
          #val=(long1,lat,date,name)
          upd_rec=cursor.execute ("""UPDATE DtD_Unit_Details SET Status=%s,ChangeDate=%s WHERE UDId=%s""", (status,date,sheduleid))
          db.commit()         
          #print upd_rec
          if upd_rec > 0:
             #print "inserted"
             output = '{"function":"update_shedule_status","error_code":"0", "Response":"Successfully update the shedule"}'
             return mqttc.publish('jts/dtd/response',output)

          else:
             #print "insert filed"
             output = '{"function":"update_shedule_status","error_code":"-2", "error_desc": "Response= Unable to update the shedule"}'
             return mqttc.publish('jts/dtd/response',output)
                       
       else:
          #print "exitig........"
          output = '{"function":"update_shedule_status","error_code":"-2", "error_desc": "Response= Unable to update the shedule"}'
          return mqttc.publish('jts/dtd/response',output)
    except MySQLdb.Error, e:
        try:
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
        except IndexError:
            #print "MySQL Error: %s" % str(e)
            return "MySQL Error: %s" % str(e)
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"update_shedule_status","error_code":"3", "error_desc": "Response=Failed to update the shedule"}'
        return mqttc.publish('jts/dtd/response',output)
    finally:
        cursor.close()
        db.close()



#################delete shedule ##########################
def delete_shedule(mosq,obj,msg):
    db = MySQLdb.connect("localhost","dusktodawn","ptldusktodawn","dtdDB")
    cursor = db.cursor()
    output_str = "delete_shedule- Unable to Authenticate/delete_shedule... "
    #print "this is register string : ",str(msg.payload)
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/dtd/response','{"function":"delete_shedule","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"delete_shedule","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"delete_shedule","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM DtD_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to delete unit"
          output = '{"function":"delete_shedule","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/dtd/response',output)

       if((data1.get('shedule_id') is None) or ((data1.get('shedule_id') is not  None) and (len(data1['shedule_id']) <= 0))):
           output_str += ", shedule_id is mandatory"
           output = '{"function":"delete_shedule","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/dtd/response',output)
       else:
           sid = data1['shedule_id']

       sqlq2 = "DELETE FROM DtD_Unit_Details WHERE UDId='"+sid+"'"
       #sqlq2 = "SELECT UnitId,UnitDesc,Operation FROM DtD_Units"
       #print sqlq2
       get_del_rec=cursor.execute(sqlq2)
       db.commit()
       
       #print get_del_rec
       if get_del_rec > 0:
           output = '{"function":"delete_shedule","error_code":"0", "Response":"Successfully deleted the shedule"}'
           return mqttc.publish('jts/dtd/response',output)
       else:
           output = '{"function":"delete_shedule","error_code":"3", "error_desc": "Response=Unable to  delete the shedule "}'
           return mqttc.publish('jts/dtd/response',output)
     
    except MySQLdb.Error, e:
        try:
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
        except IndexError:
            #print "MySQL Error: %s" % str(e)
            return "MySQL Error: %s" % str(e)
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"delete_shedule","error_code":"3", "error_desc": "Response=Failed to delete the shedule"}'
        return mqttc.publish('jts/dtd/response',output)
    finally:
        cursor.close()
        db.close()



############# mqtt publish Response ###############################
def on_publish(client, userdata, result):
        #print "data published \n"
        pass



def on_connect(client, userdata, flags, rc):
    #print("Connected with result code "+str(rc))
    mqttc.subscribe("jts/dtd/#")

def on_disconnect(client, userdata, rc):
    if rc != 0:
        print "Unexpected MQTT disconnection. Will auto-reconnect"
        #mqttc.subscribe("jts/dtd/#")
################### mqtt Connections ##############################
mqttc = mqtt.Client()
#Add message callbacks that will only trigger on a specific   subscription    match
mqttc.message_callback_add('jts/dtd/login', login)
mqttc.message_callback_add('jts/dtd/user_register', register)
#mqttc.message_callback_add('jts/dtd/Request', sunreq)
mqttc.message_callback_add('jts/dtd/get_units', get_units)
mqttc.message_callback_add('jts/dtd/add_units1', add_units1)
mqttc.message_callback_add('jts/dtd/get_unit_details', get_unit_details)
mqttc.message_callback_add('jts/dtd/update_unit_details',update_unit_details)
mqttc.message_callback_add('jts/dtd/delete_unit',delete_unit)
mqttc.message_callback_add('jts/dtd/delete_user',delete_user)

mqttc.message_callback_add('jts/dtd/add_shedule',add_shedule)
mqttc.message_callback_add('jts/dtd/get_shedule',get_shedule)
mqttc.message_callback_add('jts/dtd/update_shedule',update_shedule)
mqttc.message_callback_add('jts/dtd/delete_shedule',delete_shedule)
mqttc.message_callback_add('jts/dtd/update_shedule_status',update_shedule_status)

mqttc.on_publish = on_publish
mqttc.username_pw_set('esp', 'ptlesp01')
mqttc.connect("cld003.jts-prod.in", 1883, 60)
mqttc.on_disconnect = on_disconnect
mqttc.on_connect = on_connect
mqttc.subscribe("jts/dtd/#")
mqttc.loop_forever()
