import paho.mqtt.client as mqtt
import MySQLdb
import json
#import datetime
import sys
#import time
from datetime import datetime
import requests
####################################################

##################### Login #########################
def login(mosq,obj,msg):
    print "login......."
    db = MySQLdb.connect("localhost","oyoroom","ptljtsoyo","oyoDB")
    cursor = db.cursor()
    output_str = "login - Unable to Authenticate/login... " 
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"function":"validate_login","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
     
    try:
       if(data1.get('username') is None):
          output_str += ",username is mandatory"
          output = '{"function":"validate_login","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)
       
       if(data1.get('password') is None):
          output_str += ",password is mandatory"
          output = '{"function":"validate_login","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)

       username    = data1['username']
       password    = data1['password']

       sqlq = "SELECT * FROM Oyo_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       #sqlq = "SELECT * FROM Oyo_Users"
       #print "Checking Credentials : : ",sqlq
       cursor.execute(sqlq)
       results = cursor.fetchone()
       #print "Checking Data exists or not : ",results
       if results > 0:
          #print 'Login Data Existed'
          output = '{"function":"login","error_code":"0", "Response":"Successfully Authenticated for user: %s"}' %username
	  return mqttc.publish('jts/oyo/error',output)
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          cursor.close()
          db.close()
          output = '{"function":"validate_login","error_code":"2", "error_desc": "Response=%s"}' %output_str          
          return mqttc.publish('jts/oyo/error',output)

    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"validate_login","error_code":"3", "error_desc": "Response=Failed to login"}'
        return mqttc.publish('jts/oyo/error',output)
    
################ Register ############################
def add_unit(mosq,obj,msg):
    print "add_unit......."
    db = MySQLdb.connect("localhost","oyoroom","ptljtsoyo","oyoDB")
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
       return mqttc.publish('jts/oyo/error','{"function":"add_unit","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:
       if(data1.get('username') is None):
          output_str += ",username is mandatory"
          output = '{"function":"add_unit","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)
       
       if(data1.get('password') is None):
          output_str += ",password is mandatory"
          output = '{"function":"add_unit","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)

       username    = data1['username']
       password    = data1['password']
       sqlq = "SELECT * FROM Oyo_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
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
          cursor.close()
          db.close()
          output_str += ",Your not Autherize to Register a Device"
          output = '{"function":"add_unit","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)
   
       if((data1.get('ipmacid') is None) or ((data1.get('ipmacid') is not  None) and (len(data1['ipmacid']) <= 0))):
           output_str += ", ipmacid is mandatory"
           output = '{"function":"add_unit","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           ipmacid = data1['ipmacid'] 
           		 
       if((data1.get('opmacid') is None) or ((data1.get('opmacid') is not  None) and (len(data1['opmacid']) <= 0))):
           output_str += ", opmacid is mandatory"
           output = '{"function":"add_unit","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           opmacid = data1['opmacid'] 
           
       if((data1.get('floor_id') is None) or ((data1.get('floor_id') is not  None) and (len(data1['floor_id']) <= 0))):
           output_str += ", floor_id is mandatory"
           output = '{"function":"add_unit","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           floor = data1['floor_id']
           
       if((data1.get('room_id') is None) or ((data1.get('room_id') is not  None) and (len(data1['room_id']) <= 0))):
           output_str += ", room_id is mandatory"
           output = '{"function":"add_unit","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           room = data1['room_id']
          

       if((data1.get('premise_id') is None) or ((data1.get('premise_id') is not  None) and (len(data1['premise_id']) <= 0))):
           output_str += ", premise_id is mandatory"
           output = '{"function":"add_unit","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           premise = data1['premise_id']

       if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
           output_str += ", city_id is mandatory"
           output = '{"function":"add_unit","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           city = data1['city_id']       
       #DB
       ipmid=''
       opmid=''
       
       sqlq2="SELECT * FROM Oyo_Units WHERE IpmacId='%s' AND OpmacId='%s'" %(ipmacid,opmacid)
       #sqlq2 = "SELECT * FROM Oyo_Units WHERE IpmacId='"+ipmid+"' AND OpmacId='"+opmid+"'"
       #print "input and opearte unit macid mapping ....... :",sqlq2
       cursor.execute(sqlq2)
       results2 = cursor.fetchone()
       print "mapping data from db: ",results2
       if results2 > 0:
          print 'maping data exits......... so quiting ..Thanks'
          cursor.close()
          db.close()
          output = '{"function":"add_unit","error_code":"-2", "error_desc": "Response=Operate macid asociate with this only, no need to add"}'
          return mqttc.publish('jts/oyo/error',output)
       else:
          print 'start mapping .......'
          #print(floor,premise,ipmacid,opmacid,username,date)
          #print"""INSERT INTO Oyo_Units(FloorId,PremiseId,IpmacId,OpmacId,ChangeBy,ChangeDate) VALUES (%d,%d,%d,%d,%s,%s)""",(int(floor),int(premise),int(ipmacid),int(opmacid),username,date)
          #print("""INSERT INTO Oyo_Units(FloorId,RoomId,PremiseId,IpmacId,OpmacId,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s)""",(floor,room,premise,ipmid,opmid,username,date))
          add_rec1=cursor.execute("""INSERT INTO Oyo_Units(CityId,FloorId,RoomId,PremiseId,IpmacId,OpmacId,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",(city,floor,room,premise,ipmacid,opmacid,username,date))
          db.commit()
          if add_rec1 > 0:
             print 'input mac and operate  mac rec mapped in DB.'
             sqlq2="SELECT UnitId FROM Oyo_Units WHERE IpmacId='%s' AND OpmacId='%s'" %(ipmacid,opmacid)
             cursor.execute(sqlq2)
             results3 = cursor.fetchone()
             print "getting unit id", results3[0]
             if results3 > 0:
                add_rec1=cursor.execute("""INSERT INTO Oyo_Unit_Details(UnitId,Temp,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s)""",(results3[0],'0',username,date))
                db.commit()
                add_rec2=cursor.execute("""INSERT INTO Oyo_Unit_History(UnitId,CityId,PremiseId,FloorId,RoomId,Temp,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",(results3[0],city,premise,floor,room,'0',username,date))
                db.commit()
                if add_rec1 > 0 and add_rec2 > 0 :
                   print "both added"
                   cursor.close()
                   db.close()
                   output = '{"function":"add_units","error_code":"0", "Response":"Successfully Registered Input Unit and Operate Unit"}'
	           output1 = '{"function":"add_unit","error_code":"0", "Response":"Succes","opmacid":"%s"}'%(opmacid)
                   mqttc.publish('jts/oyo/'+ipmacid,output1)
                   return mqttc.publish('jts/oyo/error',output)
          else:
             print 'mapping issues .....check it.'
             cursor.close()
             db.close()
             output = '{"function":"add_unit","error_code":"2", "error_desc": "Response=Falied to add a Units"}'
             return mqttc.publish('jts/oyo/error',output)

    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]))
        except IndexError:
            print "MySQL Error: %s" % str(e)
            return mqttc.publish('jts/oyo/error',str(e)) 
       
    except Exception, e:  
        cursor.close()
        db.close()
	output = '{"function":"add_unit","error_code":"3", "error_desc": "Response=Failed to add the Register"}' 
        return mqttc.publish('jts/oyo/error',output)
####################### operations###################################
def operations(mosq,obj,msg):
    print "this is operation",str(msg.payload)
    #mqttc.publish('jts/oyo/res','{"user":"mahesh1","pass":"babu1"}')
###################### getTemparature from OP units ##################
def temp1(mosq,obj,msg):
    print "Temp........ "
    db = MySQLdb.connect("localhost","oyoroom","ptljtsoyo","oyoDB")
    cursor = db.cursor()
    output_str = "register - Unable to Authenticate/register... " 
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:
       imac=False
       omac=False   
       if((data1.get('macid') is None) or ((data1.get('macid') is not  None) and (len(data1['macid']) <= 0))):
           output_str += ", macid is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           macid = data1['macid']

           sqlq1 = "SELECT * FROM Oyo_Units WHERE OpmacId='"+macid+"'"
           #sqlq = "SELECT * FROM Oyo_Users"
           #print "Checking input mac id ....... :",sqlq1
           cursor.execute(sqlq1)
           results = cursor.fetchone()
           if results > 0 :
              omac=True
           sqlq2 = "SELECT * FROM Oyo_Units WHERE IpmacId='"+macid+"'"
           #sqlq = "SELECT * FROM Oyo_Users"
           #print "Checking input mac id ....... :",sqlq1
           cursor.execute(sqlq2)
           results1 = cursor.fetchone()
           if results1 > 0 :
              imac=True

       if imac==False and omac==False:
          output = '{"error_code":"2", "error_desc": "Response= macid not exists.please Configure it."}'
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

          sqlq1 = "SELECT UnitId,CityId,PremiseId,FloorId,RoomId FROM Oyo_Units WHERE OpmacId='"+macid+"'"
          #sqlq = "SELECT * FROM Oyo_Users"
          #print "Checking input mac id ....... :",sqlq1
          cursor.execute(sqlq1)
          results = cursor.fetchone()
          #print "checking in opreate unit ..... :",results
          if results > 0:
             unitid=results[0]
             cid=results[1]
             pid=results[2]
             fid=results[3]
             rid=results[4]
             #print "came op............"
             #upd_rec=cursor.execute ("""UPDATE Oyo_Units SET Temp=%s WHERE OpmacId=%s""", (temp,macid))
 	     #db.commit()
             sqlq3 = "SELECT * FROM Oyo_Unit_Details WHERE UnitId='%s'" %(unitid)
             #print sqlq3
	     cursor.execute(sqlq3)
             results2 = cursor.fetchone()
             #print "checking in units details ...",results2		
             if results2>0:
                #print "update"
                #print 'id is there. so updating .........Thanks.'
                upd_rec=cursor.execute ("""UPDATE Oyo_Unit_Details SET Fan=%s,Status=%s,SetTemp=%s,Voltage=%s,Power=%s,Current=%s,MotionE=%s,ChangeDate=%s,ChangeBy=%s WHERE UnitId=%s""", (fan,status,settemp,voltage,power,current,me,date,'jtsadmin',unitid))
                db.commit()
        
                add_rec2=cursor.execute("""INSERT INTO Oyo_Unit_History(UnitId,CityId,PremiseId,FloorId,RoomId,Fan,Status,SetTemp,Voltage,Power,Current,MotionE,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(unitid,cid,pid,fid,rid,fan,status,settemp,voltage,power,current,me,'jtsadmin',date))
                db.commit()
                if upd_rec > 0:
                   output = '{"error_code":"0", "Response":"Succesfully updated temp"}' 
                   cursor.close()
                   db.close()
                   return mqttc.publish('jts/oyo/error',output)
                else:
                   output = '{"error_code":"2", "error_desc": "Response=Unable to update temp"}'
                   cursor.close()
                   db.close()
                   return mqttc.publish('jts/oyo/error',output)
             else:
                #print "inserting ........."
                add_rec1=cursor.execute("""INSERT INTO Oyo_Unit_Details(UnitId,Fan,Status,SetTemp,Voltage,Power,Current,MotionE,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(unitid,fan,status,settemp,voltage,power,current,me,'jtsadmin',date))
                db.commit()
                add_rec2=cursor.execute("""INSERT INTO Oyo_Unit_History(UnitId,CityId,PremiseId,FloorId,RoomId,Fan,Status,SetTemp,Voltage,Power,Current,MotionE,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(unitid,cid,pid,fid,rid,fan,status,settemp,voltage,power,current,me,'jtsadmin',date))
                db.commit()
                if add_rec1>0:
                   output = '{"error_code":"0", "Response":"Succesfully added temp"}'
                   cursor.close()
                   db.close()
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
             temp = data1['temp']
 
          if((data1.get('motion') is None) or ((data1.get('motion') is not  None) and (len(data1['motion']) <= 0))):
              output_str += ", mption is mandatory"
              output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
              return mqttc.publish('jts/oyo/error',output)
          else:
             motion = data1['motion']
 
          sqlq1 = "SELECT UnitId,CityId,PremiseId,FloorId,RoomId FROM Oyo_Units WHERE IpmacId='"+macid+"'"
          #sqlq = "SELECT * FROM Oyo_Users"
          #print "Checking input mac id ....... :",sqlq1
          cursor.execute(sqlq1)
          results = cursor.fetchone()
          #print "checking in opreate unit ..... :",results
          if results > 0:
             unitid=results[0]
             cid=results[1]
             pid=results[2]
             fid=results[3]
             rid=results[4]
             #print "came ............"
             upd_rec=cursor.execute ("""UPDATE Oyo_Units SET Temp=%s WHERE IpmacId=%s""", (temp,macid))
 	     db.commit()
             #print "after update"
             sqlq3 = "SELECT * FROM Oyo_Unit_Details WHERE UnitId='%s'" %(unitid)
             #print sqlq3
	     cursor.execute(sqlq3)
             results2 = cursor.fetchone()
             #print "checking in units details ...",results2		
             if results2>0:
                #print "update"
                #print 'id is there. so updating .........Thanks.',temp
                upd_rec=cursor.execute ("""UPDATE Oyo_Unit_Details SET Temp=%s,Motion=%s,ChangeDate=%s,ChangeBy=%s WHERE UnitId=%s""", (temp,motion,date,'jtsadmin',unitid))
                db.commit()
                #print "inside update"
                add_rec2=cursor.execute("""INSERT INTO Oyo_Unit_History(UnitId,CityId,PremiseId,FloorId,RoomId,Temp,Motion,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(unitid,cid,pid,fid,rid,temp,motion,'jtsadmin',date))
                db.commit()
                if upd_rec > 0:
                   output = '{"error_code":"0", "Response":"Succesfully updated temp"}' 
                   cursor.close()
                   db.close()
                   return mqttc.publish('jts/oyo/error',output)
                else:
                   output = '{"error_code":"2", "error_desc": "Response=Unable to update temp"}'
                   cursor.close()
                   db.close()
                   return mqttc.publish('jts/oyo/error',output)
             else:
                #print "inserting ........."
                add_rec1=cursor.execute("""INSERT INTO Oyo_Unit_Details(UnitId,Temp,Motion,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s)""",(unitid,temp,motion,'jtsadmin',date))
                db.commit()
                add_rec2=cursor.execute("""INSERT INTO Oyo_Unit_History(UnitId,CityId,PremiseId,FloorId,RoomId,Temp,Motion,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(unitid,cid,pid,fid,rid,temp,motion,'jtsadmin',date))
                db.commit()
                if add_rec1>0:
                   output = '{"error_code":"0", "Response":"Succesfully added temp"}'
                   cursor.close()
                   db.close()
                   return mqttc.publish('jts/oyo/error',output) 
               
          else:
             #print 'mac id not avilable , so configure....Thanks.'
             output = '{"error_code":"2", "error_desc": "Response= macid not exists.please Configure it."}'
             return mqttc.publish('jts/oyo/error',output)
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]))
        except IndexError:
            print "MySQL Error: %s" % str(e)
            return mqttc.publish('jts/oyo/error',str(e)) 
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"error_code":"3", "error_desc": "Response=Failed to add the temp"}'
        return mqttc.publish('jts/oyo/error',output)

################################ get_roles #########################
def get_roles(mosq,obj,msg):
    print "get_roles......."
    db = MySQLdb.connect("localhost","oyoroom","ptljtsoyo","oyoDB")
    cursor = db.cursor()
    output_str = "get_role - Unable to Authenticate/get_role... "
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')

    if(not data1):
        output_str += ", details are mandatory"
        output = '{"function":"get_roles","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"function":"get_roles","error_code":"2", "error_desc": "Response=%s"}' %output_str
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('password') is None):
        output_str += ",passsword is mandatory"
        output = '{"function":"get_roles","error_code":"2", "error_desc": "Response=%s"}' %output_str
        return mqttc.publish('jts/oyo/error',output)

    username    = data1['username']
    password    = data1['password']
    sqlq = "SELECT * FROM Oyo_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
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
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to get roles"
       output = '{"function":"get_roles","error_code":"2", "error_desc": "Response=%s"}' %output_str
       return mqttc.publish('jts/oyo/error',output)
    try:
       sqlq2 = "SELECT AdminId,AdminDesc FROM Oyo_Admin "
       cursor.execute(sqlq2)
       get_roles_rec = cursor.fetchall()
       if(len(get_roles_rec) > 0):
        #{
          output = '{"funtion":"get_roles","error_code":"0", "Response":"Successfully got %d roles", \n "get_roles":' %len(get_roles_rec)
          output += '['
          counter = 0
          for rec in get_roles_rec:
          #{
             counter += 1
             if(counter == 1):
               output += '{"role_id":"%s","role_desc":"%s"}' %(rec[0] ,rec[1])
             else:
               output += ',\n {"role_id":"%s","role_desc":"%s"}' %(rec[0] ,rec[1])
          #}
          output += ']\n'
          output += '}'
          cursor.close()
          db.close()
          return mqttc.publish('jts/oyo/error',output)
       else:
           output = '{"function":"get_roles","error_code":"3", "error_desc": "Response=Failed to get the role records, NO_DATA_FOUND"}'
           return mqttc.publish('jts/oyo/error',output)
      
          
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_roles","error_code":"3", "error_desc": "Response=Failed to get the roles"}'
        return mqttc.publish('jts/oyo/error',output)

####################### add_user #################################
def add_user(mosq,obj,msg):
    print "add_user......."
    #print "this is add_users : ",str(msg.payload)
    db = MySQLdb.connect("localhost","oyoroom","ptljtsoyo","oyoDB")
    cursor = db.cursor()
    output_str = "add_user - Unable to Authenticate/add_user... " 
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"function":"add_user","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')

    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"function":"add_user","error_code":"2", "error_desc": "Response=%s"}' %output_str
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('password') is None):
       output_str += ",passsword is mandatory"
       output = '{"function":"add_user","error_code":"2", "error_desc": "Response=%s"}' %output_str
       return mqttc.publish('jts/oyo/error',output)

    username    = data1['username']
    password    = data1['password']
    sqlq = "SELECT * FROM Oyo_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
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
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to add the user"
       output = '{"function":"add_user","error_code":"2", "error_desc": "Response=%s"}' %output_str
       return mqttc.publish('jts/oyo/error',output)
    try:
          
       if((data1.get('Uname') is None) or ((data1.get('Uname') is not  None) and (len(data1['Uname']) <= 0))):
           output_str += ", Uname is mandatory"
           output = '{"function":"add_user","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           uname = data1['Uname'] 

       if((data1.get('Pass') is None) or ((data1.get('Pass') is not  None) and (len(data1['Pass']) <= 0))):
           output_str += ", Pass is mandatory"
           output = '{"function":"add_user","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           pass1 = data1['Pass'] 
 
       if((data1.get('Role') is None) or ((data1.get('Role') is not  None) and (len(data1['Role']) <= 0))):
           output_str += ", Role is mandatory"
           output = '{"function":"add_user","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           role = data1['Role'] 

       sqlq1 = "SELECT * FROM Oyo_Users WHERE Username='"+uname+"' AND Password='"+pass1+"'"
       #sqlq = "SELECT * FROM Oyo_Users"
       #print "Checking input mac id ....... :",sqlq1
       cursor.execute(sqlq1)
       results = cursor.fetchone()
       #print "checking in opreate unit ..... :",results
       if results > 0:
          output_str = "User Already Exits"
          output = '{"function":"add_user","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
          return mqttc.publish('jts/oyo/error',output)
       else:
          #print "inserting ........."
          add_rec1=cursor.execute("""INSERT INTO Oyo_Users(Username,Password,ChangeBy,Role,ChangeDate) VALUES (%s,%s,%s,%s,%s)""",(uname,pass1,username,role,date))
          db.commit()
          if add_rec1>0:
             output = '{"function":"add_user","error_code":"0", "Response":"Succesfully added user"}'
             cursor.close()
             db.close()
             return mqttc.publish('jts/oyo/error',output) 
    except MySQLdb.Error, e:
        try:
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1])) 
        except IndexError:
            #print "MySQL Error: %s" % str(e)
            return mqttc.publish('jts/oyo/error',str(e))
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"add_user","error_code":"3", "error_desc": "Response=Failed to add the User"}'
        return mqttc.publish('jts/oyo/error',output)

########################### get premices #####################################
def get_premise(mosq,obj,msg):
    print "get_premise......."
    db = MySQLdb.connect("localhost","oyoroom","ptljtsoyo","oyoDB")
    cursor = db.cursor()
    output_str = "get_premise - Unable to Authenticate/get_register... "
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"function":"get_premise","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')

    if(not data1):
        output_str += ", details are mandatory"
        output = '{"function":"get_premise","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"function":"get_premise","error_code":"2", "error_desc": "Response=%s"}' %output_str
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('password') is None):
       output_str += ",passsword is mandatory"
       output = '{"function":"get_premise","error_code":"2", "error_desc": "Response=%s"}' %output_str
       return mqttc.publish('jts/oyo/error',output)
    username    = data1['username']
    password    = data1['password']
    sqlq = "SELECT * FROM Oyo_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
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
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to get premise"
       output = '{"function":"get_premise","error_code":"2", "error_desc": "Response=%s"}' %output_str
       return mqttc.publish('jts/oyo/error',output)
    try:
       if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
           output_str += ", city_id is mandatory"
           output = '{"function":"get_premise","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           city_id = data1['city_id']

       sqlq2 = "SELECT PremiseId,PremiseDesc FROM Oyo_Premise where CityId='"+city_id+"'"
       #sqlq2="select Oyo_City_Premise.PremiseId, Oyo_Premise.PremiseDesc from Oyo_City_Premise inner join Oyo_Premise on Oyo_City_Premise.PremiseId=Oyo_Premise.PremiseId where CityId='"+city_id+"'"
       cursor.execute(sqlq2)
       get_premise_rec = cursor.fetchall()
       if(len(get_premise_rec) > 0):
        #{
           output = '{"function":"get_premise","error_code":"0", "Response":"Successfully got %d premise", \n "get_premise":' %len(get_premise_rec)
           output += '['
           counter = 0
           for rec in get_premise_rec:
           #{
              counter += 1
              if(counter == 1):
                output += '{"premise_id":"%s","premise_desc":"%s"}' %(rec[0] ,rec[1])
              else:
                output += ',\n {"premise_id":"%s","premise_desc":"%s"}' %(rec[0] ,rec[1])
           #}
           output += ']\n'
           output += '}'
           #cust_recs_json = serializers.serialize("json", curr_status_recs)
           return mqttc.publish('jts/oyo/error',output)
        #}
       else:
           output = '{"function":"get_premise","error_code":"3", "error_desc": "Response=Failed to get the premise records, NO_DATA_FOUND"}'
           return mqttc.publish('jts/oyo/error',output)

    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_premise","error_code":"3", "error_desc": "Response=Failed to get the premise"}'
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()
############################# get units##########################
def get_units(mosq,obj,msg):
    print "get_units......."
    db = MySQLdb.connect("localhost","oyoroom","ptljtsoyo","oyoDB")
    cursor = db.cursor()
    output_str = "get_units - Unable to Authenticate/get_units... "
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"function":"get_units","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')

    if(not data1):
        output_str += ", details are mandatory"
        output = '{"function":"get_units","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"function":"get_units","error_code":"2", "error_desc": "Response=%s"}' %output_str
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('password') is None):
       output_str += ",passsword is mandatory"
       output = '{"function":"get_units","error_code":"2", "error_desc": "Response=%s"}' %output_str
       return mqttc.publish('jts/oyo/error',output)

    username    = data1['username']
    password    = data1['password']
    sqlq = "SELECT * FROM Oyo_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
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
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to get units"
       output = '{"function":"get_units","error_code":"2", "error_desc": "Response=%s"}' %output_str
       return mqttc.publish('jts/oyo/error',output)
    try:
       history_all=False
       histoty_city=False
       history_premise=False
       get_units=False

       if((data1.get('history') is None) or ((data1.get('history') is not  None) and (len(data1['history']) <= 0))):
           output_str += ", history is mandatory"
           output = '{"function":"get_units","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           history = data1['history']
           #print history
           if history == "all_cities":
              #print "all cities"
              history_all=True
           elif history == "city_premise":
              #print "in premise"
              history_city=True
           elif history == "premise_units":
              #print "in units of premise"
              #history_premise=True
              if((data1.get('premise_id') is None) or ((data1.get('premise_id') is not  None) and (len(data1['premise_id']) <= 0))):
                 output_str += ", premise_id is mandatory"
                 output = '{"function":"get_units","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
                 return mqttc.publish('jts/oyo/error',output)
              else:
                 pid = data1['premise_id']

              sqlq2 = "select Oyo_Units.UnitId,Oyo_Units.IpmacId,Oyo_Units.OpmacId,Oyo_Unit_Details.Fan,Oyo_Unit_Details.Status,Oyo_Unit_Details.Temp,Oyo_Unit_Details.SetTemp,Oyo_Unit_Details.Voltage,Oyo_Unit_Details.Power,Oyo_Unit_Details.Current from Oyo_Unit_Details inner join Oyo_Units on Oyo_Unit_Details.UnitId=Oyo_Units.UnitId where PremiseId='%s'" %(pid)
              cursor.execute(sqlq2)
              get_units_rec = cursor.fetchall()
              if(len(get_units_rec) > 0):
                 output = '{"function":"premise_units","error_code":"0", "Response":"Successfully got %d units", \n "get_units":' %len(get_units_rec)
                 output += '['
                 counter = 0
                 for rec in get_units_rec:
                     counter += 1
                     if(counter == 1):
                        output += '{"Unit_id":"%s","premise_id":"%s","ipmacid":"%s","opmacid":"%s","fan":"%s","status":"%s","temp":"%s","settemp":"%s","voltage":"%s","power":"%s","current":"%s"}' %(rec[0] ,pid,rec[1],rec[2],rec[3],rec[4],rec[5],rec[6],rec[7],rec[8],rec[9])
                     else:
                        output += ',\n {"Unit_id":"%s","premise_id":"%s","ipmacid":"%s","opmacid":"%s","fan":"%s","status":"%s","temp":"%s","settemp":"%s","voltage":"%s","power":"%s","current":"%s"}' %(rec[0] ,pid,rec[1],rec[2],rec[3],rec[4],rec[5],rec[6],rec[7],rec[8],rec[9])
                 output += ']\n'
                 output += '}'
                 return mqttc.publish('jts/oyo/error',output)

              else:
                 output = '{"function":"get_units","error_code":"3", "error_desc": "Response=Failed to get the unit records, NO_DATA_FOUND"}'
                 return mqttc.publish('jts/oyo/error',output)

           elif history == "get_units":
              print "in units"
              sqlq2 = "select Oyo_Units.UnitId,Oyo_Units.IpmacId,Oyo_Units.OpmacId,Oyo_Rooms.RoomDesc,Oyo_Floors.FloorDesc,Oyo_Premise.PremiseDesc,Oyo_City.CityDesc,Oyo_Units.ChangeDate from Oyo_Units,Oyo_City,Oyo_Premise,Oyo_Floors,Oyo_Rooms where Oyo_City.CityId=Oyo_Units.CityId and Oyo_Premise.PremiseId=Oyo_Units.PremiseId and Oyo_Floors.FloorId=Oyo_Units.FloorId and Oyo_Rooms.RoomId=Oyo_Units.RoomId"
              cursor.execute(sqlq2)
              get_units_rec = cursor.fetchall()
              if(len(get_units_rec) > 0):
                 output = '{"function":"get_units","error_code":"0", "Response":"Successfully got %d units", \n "get_units":' %len(get_units_rec)
                 output += '['
                 counter = 0
                 for rec in get_units_rec:
                     counter += 1
                     if(counter == 1):
                        output += '{"Unit_id":"%s","IpmacId":"%s","OpmacId":"%s","RoomDesc":"%s","FloorDesc":"%s","PremiseDesc":"%s","CityDesc":"%s","ChangeDate":"%s"}' %(rec[0] ,rec[1],rec[2],rec[3],rec[4],rec[5],rec[6],rec[7])
                     else:
                        output += ',\n {"Unit_id":"%s","IpmacId":"%s","OpmacId":"%s","RoomDesc":"%s","FloorDesc":"%s","PremiseDesc":"%s","CityDesc":"%s","ChangeDate":"%s"}' %(rec[0] ,rec[1],rec[2],rec[3],rec[4],rec[5],rec[6],rec[7])

                 output += ']\n'
                 output += '}'
                 return mqttc.publish('jts/oyo/error',output)

              else:
                 output = '{"function":"get_units","error_code":"3", "error_desc": "Response=Failed to get the unit records, NO_DATA_FOUND"}'
                 return mqttc.publish('jts/oyo/error',output)
           
           else:
              print "nothing satisfied"
           #print "end...."
       
       if history_all == True:
          sqlq = "select count(Temp) from Oyo_Unit_History where Temp between 23 and 25 GROUP BY CityId"
          cursor.execute(sqlq)
          good = cursor.fetchall()
          print "response fro cld good : ",good,len(good)
          sqlq1 = "select count(Temp) from Oyo_Unit_History where Temp between 25 and 27 GROUP BY CityId"
          cursor.execute(sqlq1)
          warning = cursor.fetchall()
          print "response fro cld warn : ",warning,len(warning)
          sqlq2 = "select count(Temp) from Oyo_Unit_History where Temp between 20 and 22 GROUP BY CityId"
          cursor.execute(sqlq2)
          problem = cursor.fetchall()
          print "response fro cld problem : ",problem,len(problem)
          sqlq = "select count(Temp) from Oyo_Unit_History where Temp=50 GROUP BY CityId"
          cursor.execute(sqlq)
          main = cursor.fetchall()
          print "response fro cld maintenance : ",main,len(main)
          sqlq3 = "select CityDesc from Oyo_City"
          cursor.execute(sqlq3)
          getcity = cursor.fetchall()
          print "response fro cld maintenance : ",getcity,len(getcity)
      
          sqlq2= "select CityId,round(avg(Temp)) as avgtemp from Oyo_Unit_History GROUP BY CityId"
          #sqlq2 = "select Oyo_City.CityId,Oyo_City.CityDesc,round(avg(Temp)) as avgtemp from Oyo_Unit_History,Oyo_City GROUP BY CityId"
          #sqlq2 = "select Oyo_City.CityId,Oyo_City.CityDesc,round(avg(Temp)) as avgtemp,(select count(Temp) from Oyo_Unit_History where Temp between 23 and 25) as good,(select count(Temp) from Oyo_Unit_History where Temp between 25 and 27) as warning,(select count(Temp) from Oyo_Unit_History where Temp between 27 and 21) as problem,(select count(Temp) from Oyo_Unit_History where Temp=99) as maintenance from Oyo_Unit_History,Oyo_City where Oyo_Unit_History.CityId=Oyo_City.CityId  GROUP BY CityId"
          #print sqlq2
          cursor.execute(sqlq2)
          get_units_rec = cursor.fetchall()
            
          print get_units_rec,len(get_units_rec)
          if(len(get_units_rec) > 0):
             #desc = cursor.description
	     output = '{"function":"all_cities","error_code":"0", "Response":"Successfully got %d units", \n "get_units":' %len(get_units_rec)
             output += '['
             counter = 0
             goodval = "0"
             warningval = "0"
             problemval = "0"
             mainval = "0"
             cityDesc=""
             for i,rec in enumerate(get_units_rec):
                 print "i : ",i
                
                 if i < len(good):
                    print "ggod if "
                    goodval=good[i][0]
                    print "ggod if ",goodval
                 else:
                    print "ggod else"
                    goodval="0"
                    print "ggod else ",goodval
		 	                 
                 if i < len(warning):
                    print "warn if"
                    warningval=warning[i][0]
                    print "warn if ",warningval
                 else:
                    print "wanr else"
                    warningval="0"
                    print "warn else ",warningval
                 if i < len(problem):
                    print "prob if"
                    problemval=problem[i][0]
                    print "prob if",problemval
                 else:
                    print "prob else"
                    problemval="0"
                    print "prob else",problemval
     		 if i < len(main):
                    print "main if"
                    mainval=main[i][0]
                    print "main if",mainval
                 else:
                    print "main else"
                    mainval="0"
                    print "main else",mainval
                 #print goodval,warningval,problemval,mainval,getcity[i][0]        
                 print warningval
                 print problemval
                 print mainval
                 print getcity[i][0] 
                 cityDesc=getcity[i][0]
                 counter += 1
                 if(counter == 1):
                    
                    output += '{"city_id":"%s","CityDesc":"%s","avgtemp":"%s","good":"%s","warning":"%s","problem":"%s","maintenance":"%s"}' %(rec[0] ,cityDesc,rec[1],goodval,warningval,problemval,mainval)
                 else:
                    
                    output += ',\n {"city_id":"%s","CityDesc":"%s","avgtemp":"%s","good":"%s","warning":"%s","problem":"%s","maintenance":"%s"}' %(rec[0] ,cityDesc,rec[1],goodval,warningval,problemval,mainval)
                 print output
             output += ']\n'
             output += '}'
             print output
             return mqttc.publish('jts/oyo/error',output) 
          else:
             output = '{"function":"get_units","error_code":"3", "error_desc": "Response=Failed to get the unit records, NO_DATA_FOUND"}'
             return mqttc.publish('jts/oyo/error',output)


       if history_city == True:
          #print "in city func"
          if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
             output_str += ", city_id is mandatory"
             output = '{"function":"get_units","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
             return mqttc.publish('jts/oyo/error',output)
          else:
             cid = data1['city_id']

             sqlq = "select count(Temp) from Oyo_Unit_History where Temp between 23 and 25 and CityId='%s' GROUP BY PremiseId" %(cid)
             cursor.execute(sqlq)
             good = cursor.fetchall()
             print "response fro cld good : ",good,len(good)
             sqlq1 = "select count(Temp) from Oyo_Unit_History where Temp between 25 and 27 and CityId='%s' GROUP BY PremiseId" %(cid)
             cursor.execute(sqlq1)
             warning = cursor.fetchall()
             print "response fro cld warn : ",warning,len(warning)
             sqlq2 = "select count(Temp) from Oyo_Unit_History where Temp between 20 and 22 and CityId='%s' GROUP BY PremiseId" %(cid)
             cursor.execute(sqlq2)
             problem = cursor.fetchall()
             print "response fro cld problem : ",problem,len(problem)
             sqlq = "select count(Temp) from Oyo_Unit_History where Temp=50 and CityId='%s' GROUP BY PremiseId" %(cid)
             cursor.execute(sqlq)
             main = cursor.fetchall()
             print "response fro cld maintenance : ",main,len(main)
             sqlq3 = "select PremiseDesc from Oyo_Premise"
             cursor.execute(sqlq3)
             getpremise = cursor.fetchall()
             print "response fro cld maintenance : ",getpremise,len(getpremise)
             sqlq2= "select PremiseId,round(avg(Temp)) as avgtemp from Oyo_Unit_History WHERE CityId='%s' GROUP BY PremiseId" %(cid)
             #sqlq2 = "select Oyo_Premise.PremiseId,Oyo_Premise.PremiseDesc,round(avg(Temp)) as avgtemp,(select count(Temp) from Oyo_Unit_History where Temp between 23 and 25) as good,(select count(Temp) from Oyo_Unit_History where Temp between 25 and 27) as warning,(select count(Temp) from Oyo_Unit_History where Temp between 27 and 21) as problem,(select count(Temp) from Oyo_Unit_History where Temp=99) as maintenance from Oyo_Unit_History,Oyo_Premise where Oyo_Unit_History.CityId='%s' and Oyo_Premise.PremiseId=Oyo_Unit_History.PremiseId GROUP BY PremiseId" %(cid)
             #print sqlq2
             cursor.execute(sqlq2)
             get_units_rec = cursor.fetchall()
             #print get_units_rec
             if(len(get_units_rec) > 0):
                output = '{"function":"city_premise","error_code":"0", "Response":"Successfully got %d units", \n "get_units":' %len(get_units_rec)
                output += '['
                counter = 0
                goodval = "0"
                warningval = "0"
                problemval = "0"
                mainval = "0"
                preDesc=""
                for i,rec in enumerate(get_units_rec):
 	            if i < len(good):
                       print "ggod if "
                       goodval=good[i][0]
                       print "ggod if ",goodval
                    else:
                       print "ggod else"
                       goodval="0"
                       print "ggod else ",goodval

                    if i < len(warning):
                       print "warn if"
                       warningval=warning[i][0]
                       print "warn if ",warningval
                    else:
                       print "wanr else"
                       warningval="0"
                       print "warn else ",warningval
                    if i < len(problem):
                       print "prob if"
                       problemval=problem[i][0]
                       print "prob if",problemval
                    else:
                       print "prob else"
                       problemval="0"
                       print "prob else",problemval
                    if i < len(main):
                       print "main if"
                       mainval=main[i][0]
                       print "main if",mainval
                    else:
                       print "main else"
                       mainval="0"
   		    print warningval
                    print problemval
                    print mainval
                    print getpremise[i][0]
                    preDesc=getpremise[i][0]
                    counter += 1
                    if(counter == 1):
                       output += '{"city_id":"%s","premise_id":"%s","premise_desc":"%s","avgtemp":"%s","good":"%s","warning":"%s","problem":"%s","maintenance":"%s"}' %(cid,rec[0] ,preDesc,rec[1],goodval,warningval,problemval,mainval)
                    else:
                       output += ',\n {"city_id":"%s","premise_id":"%s","premise_desc":"%s","avgtemp":"%s","good":"%s","warning":"%s","problem":"%s","maintenance":"%s"}' %(cid,rec[0] ,preDesc,rec[1],goodval,warningval,problemval,mainval)
                output += ']\n'
                output += '}'
                print output
                return mqttc.publish('jts/oyo/error',output)
             else:
                output = '{"function":"get_units","error_code":"3", "error_desc": "Response=Failed to get the unit records, NO_DATA_FOUND"}'
                return mqttc.publish('jts/oyo/error',output)
   
              
       '''
       sqlq2 = "SELECT UnitId,CityId,FloorId,RoomId,IpmacId,OpmacId,Temp,ChangeDate FROM Oyo_Units where PremiseId='"+premise+"'"
       cursor.execute(sqlq2)
       get_units_rec = cursor.fetchall()
       if(len(get_units_rec) > 0):
        #{
           output = '{"function":"get_units","error_code":"0", "Response":"Successfully got %d units", \n "get_units":' %len(get_units_rec)
           output += '['
           counter = 0
           for rec in get_units_rec:
           #{
              counter += 1
              if(counter == 1):
                output += '{"Unit_id":"%s","City_id":"%s","Floor_id":"%s","Room_id":"%s","Ipunit_id":"%s","Opunit_id":"%s","Temp":"%s","CreateDate":"%s"}' %(rec[0] ,rec[1],rec[2],rec[3],rec[4],rec[5],rec[6],rec[7])
              else:
                output += ',\n {"Unit_id":"%s","City_id":"%s","Floor_id":"%s","Room_id":"%s","Ipunit_id":"%s","Opunit_id":"%s","Temp":"%s","CreateDate":"%s"}' %(rec[0] ,rec[1],rec[2],rec[3],rec[4],rec[5],rec[6],rec[7])
           #}
           output += ']\n'
           output += '}'
           #cust_recs_json = serializers.serialize("json", curr_status_recs)
           return mqttc.publish('jts/oyo/error',output) 
        #}
       else:
           output = '{"error_code":"3", "error_desc": "Response=Failed to get the unit records, NO_DATA_FOUND"}'
           return mqttc.publish('jts/oyo/error',output)   
       '''
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_units","error_code":"3", "error_desc": "Response=Failed to get the units"}'
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()
###################### get rooms #################################
def get_rooms(mosq,obj,msg):
    print "get_rooms......."
    db = MySQLdb.connect("localhost","oyoroom","ptljtsoyo","oyoDB")
    cursor = db.cursor()
    output_str = "get_rooms - Unable to Authenticate/get_rooms... "
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')

    if(not data1):
        output_str += ", details are mandatory"
        output = '{"function":"get_rooms","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"function":"get_rooms","error_code":"2", "error_desc": "Response=%s"}' %output_str
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('password') is None):
       output_str += ",passsword is mandatory"
       output = '{"function":"get_rooms","error_code":"2", "error_desc": "Response=%s"}' %output_str
       return mqttc.publish('jts/oyo/error',output)
    username    = data1['username']
    password    = data1['password']
    sqlq = "SELECT * FROM Oyo_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
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
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to get rooms"
       output = '{"function":"get_rooms","error_code":"2", "error_desc": "Response=%s"}' %output_str
       return mqttc.publish('jts/oyo/error',output)
    try:
       if((data1.get('floor_id') is None) or ((data1.get('floor_id') is not  None) and (len(data1['floor_id']) <= 0))):
           output_str += ", floor_id is mandatory"
           output = '{"function":"get_rooms","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           fid = data1['floor_id']

       sqlq2 = "SELECT RoomId,RoomDesc from Oyo_Rooms WHERE FloorId='%s'" %(fid)
       cursor.execute(sqlq2)
       get_rooms_rec = cursor.fetchall()
       if(len(get_rooms_rec) > 0):
        #{
           output = '{"function":"get_rooms","error_code":"0", "Response":"Successfully got %d rooms", \n "get_rooms":' %len(get_rooms_rec)
           output += '['
           counter = 0
           for rec in get_rooms_rec:
           #{
              counter += 1
              if(counter == 1):
                output += '{"Room_id":"%s","Room_desc":"%s"}' %(rec[0] ,rec[1])
              else:
                output += ',\n {"Room_id":"%s","Room_desc":"%s"}' %(rec[0] ,rec[1])
           #}
           output += ']\n'
           output += '}'
           return mqttc.publish('jts/oyo/error',output)
        #}
       else:
           output = '{"function":"get_rooms","error_code":"3", "error_desc": "Response=Failed to get the rooms records, NO_DATA_FOUND"}'
           return mqttc.publish('jts/oyo/error',output) 
          
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_rooms","error_code":"3", "error_desc": "Response=Failed to get the rooms"}'
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()
####################### get floors ##########################
def get_floors(mosq,obj,msg):
    print "get_floors......."
    db = MySQLdb.connect("localhost","oyoroom","ptljtsoyo","oyoDB")
    cursor = db.cursor()
    output_str = "get_floors - Unable to Authenticate/get_floors... "
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"function":"get_floors","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')

    if(not data1):
        output_str += ", details are mandatory"
        output = '{"function":"get_floors","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"function":"get_floors","error_code":"2", "error_desc": "Response=%s"}' %output_str
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('password') is None):
        output_str += ",passsword is mandatory"
        output = '{"function":"get_floors","error_code":"2", "error_desc": "Response=%s"}' %output_str
        return mqttc.publish('jts/oyo/error',output)
    username    = data1['username']
    password    = data1['password']
    sqlq = "SELECT * FROM Oyo_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
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
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to get floors"
       output = '{"function":"get_floors","error_code":"2", "error_desc": "Response=%s"}' %output_str
       return mqttc.publish('jts/oyo/error',output)
    try:
       if((data1.get('premise_id') is None) or ((data1.get('premise_id') is not  None) and (len(data1['premise_id']) <= 0))):
           output_str += ", premise_id is mandatory"
           output = '{"function":"get_floors","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           pid = data1['premise_id']      
       sqlq2 = "SELECT FloorId,FloorDesc FROM Oyo_Floors WHERE PremiseId='%s'" %(pid)
       cursor.execute(sqlq2)
       get_floors_rec = cursor.fetchall()
       if(len(get_floors_rec) > 0): 
        #{
           output = '{"function":"get_floors","error_code":"0", "Response":"Successfully got %d floors", \n "get_floors":' %len(get_floors_rec)
           output += '['
           counter = 0
           for rec in get_floors_rec:
           #{
              counter += 1
              if(counter == 1):
                output += '{"floor_id":"%s","floor_desc":"%s"}' %(rec[0] ,rec[1])
              else: 
                output += ',\n {"floor_id":"%s","floor_desc":"%s"}' %(rec[0] ,rec[1])
           #}
           output += ']\n'
           output += '}'
           return mqttc.publish('jts/oyo/error',output)
        #} 
       else:
           output = '{"function":"get_floors","error_code":"3", "error_desc": "Response=Failed to get the floors records, NO_DATA_FOUND"}' 
           return mqttc.publish('jts/oyo/error',output)
            
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_floors","error_code":"3", "error_desc": "Response=Failed to get the floors"}'
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close() 
############## get units details ################################
def get_units_details(mosq,obj,msg):
    print "get_unit_details......."
    db = MySQLdb.connect("localhost","oyoroom","ptljtsoyo","oyoDB")
    cursor = db.cursor()
    output_str = "get_units_details - Unable to Authenticate/get_units_details... "
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"function":"get_units_details","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')

    if(not data1):
        output_str += ", details are mandatory"
        output = '{"function":"get_units_details","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"function":"get_units_details","error_code":"2", "error_desc": "Response=%s"}' %output_str
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('password') is None):
        output_str += ",passsword is mandatory"
        output = '{"function":"get_units_details","error_code":"2", "error_desc": "Response=%s"}' %output_str
        return mqttc.publish('jts/oyo/error',output)

    username    = data1['username']
    password    = data1['password']
    sqlq = "SELECT * FROM Oyo_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
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
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to get unit details"
       output = '{"function":"get_units_details","error_code":"2", "error_desc": "Response=%s"}' %output_str
       return mqttc.publish('jts/oyo/error',output)
    try:
       if((data1.get('UnitId') is None) or ((data1.get('UnitId') is not  None) and (len(data1['UnitId']) <= 0))):
           output_str += ", UnitId is mandatory"
           output = '{"function":"get_units_details","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           unitid = data1['UnitId'] 
       sqlq2 = "SELECT Fan,Status,Temp,SetTemp,Runtime,Strength FROM Oyo_Unit_Details WHERE UnitId='"+unitid+"'"
       cursor.execute(sqlq2)
       get_units_rec = cursor.fetchall()
       if(len(get_units_rec) > 0):
        #{
           output = '{"function":"get_unit_details","error_code":"0", "Response":"Successfully got %d units", \n "get_units_details":' %len(get_units_rec)
           output += '['
           counter = 0
           for rec in get_units_rec:
           #{
              counter += 1
              if(counter == 1):
                output += '{"Fan":"%s","Status":"%s","Temp":"%s","SetTemp":"%s","Runtime":"%s","Strength":"%s"}' %(rec[0] ,rec[1],rec[2],rec[3],rec[4],rec[5])
              else:
                output += ',\n {"Fan":"%s","Status":"%s","Temp":"%s","SetTemp":"%s","Runtime":"%s","Strength":"%s"}' %(rec[0] ,rec[1],rec[2],rec[3],rec[4],rec[5])
           #}
           output += ']\n'
           output += '}'
           return mqttc.publish('jts/oyo/error',output)
        #}
       else:
           output = '{"function":"get_units_details","error_code":"3", "error_desc": "Response=Failed to get the unit records, NO_DATA_FOUND"}'
           return mqttc.publish('jts/oyo/error',output)      

    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_units_details","error_code":"3", "error_desc": "Response=Failed to get the units"}'
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()
################### get cities #############################################
def get_city(mosq,obj,msg):
    print "get_cities......."
    db = MySQLdb.connect("localhost","oyoroom","ptljtsoyo","oyoDB")
    cursor = db.cursor()
    output_str = "get_cities - Unable to Authenticate/get_cities... "
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"function":"get_city","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')

    if(not data1):
        output_str += ", details are mandatory"
        output = '{"function":"get_city","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"function":"get_city","error_code":"2", "error_desc": "Response=%s"}' %output_str
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('password') is None):
        output_str += ",passsword is mandatory"
        output = '{"function":"get_city","error_code":"2", "error_desc": "Response=%s"}' %output_str
        return mqttc.publish('jts/oyo/error',output)
    username    = data1['username']
    password    = data1['password']
    sqlq = "SELECT * FROM Oyo_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
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
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to get cities"
       output = '{"function":"get_city","error_code":"2", "error_desc": "Response=%s"}' %output_str
       return mqttc.publish('jts/oyo/error',output)
    try:
       sqlq2 = "SELECT CityId,CityDesc FROM Oyo_City "
       cursor.execute(sqlq2)
       get_cities_rec = cursor.fetchall()
       if(len(get_cities_rec) > 0): 
        #{
           output = '{"function":"get_cities","error_code":"0", "Response":"Successfully got %d cities", \n "get_cities":' %len(get_cities_rec)
           output += '['
           counter = 0
           for rec in get_cities_rec:
           #{
              counter += 1
              if(counter == 1):
                output += '{"city_id":"%s","city_desc":"%s"}' %(rec[0] ,rec[1])
              else: 
                output += ',\n {"city_id":"%s","city_desc":"%s"}' %(rec[0] ,rec[1])
           #}
           output += ']\n'
           output += '}'
           return mqttc.publish('jts/oyo/error',output)
        #} 
       else:
           output = '{"function":"get_city","error_code":"3", "error_desc": "Response=Failed to get the city records, NO_DATA_FOUND"}' 
           return mqttc.publish('jts/oyo/error',output)
            
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_city","error_code":"3", "error_desc": "Response=Failed to get the cities"}'
        return mqttc.publish('jts/oyo/error',output)
    finally:
        username=''
        password=''
        cursor.close()
        db.close() 
######################## add cities ###############################################
def add_city(mosq,obj,msg):
    print "add_city......."
    db = MySQLdb.connect("localhost","oyoroom","ptljtsoyo","oyoDB")
    cursor = db.cursor()
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
       return mqttc.publish('jts/oyo/error','{"function":"add_city","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"add_city","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"add_city","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM Oyo_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to add unit"
          output = '{"function":"add_city","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('city_name') is None) or ((data1.get('city_name') is not  None) and (len(data1['city_name']) <= 0))):
           output_str += ", city_name is mandatory"
           output = '{"function":"add_city","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           cname = data1['city_name']

       
       #sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"' AND Longitude='"+long1+"' AND Latitude='"+lat+"'"
       sqlq1 = "SELECT * FROM Oyo_City WHERE CityDesc='"+cname+"'"
       cursor.execute(sqlq1)
       results = cursor.fetchone()
       
       if results > 0:
          #print "unit existed"
          output_str = "The city already Registered"
          output = '{"function":"add_city","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)
          
       else:
          #print "exitig........"
          #add_rec=cursor.execute("""INSERT INTO DtD_Units(UnitDesc,Longitude,Latitude,Operation,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s)""",(name,float(long1),float(lat),'OFF',username,date))
          add_rec=cursor.execute("""INSERT INTO Oyo_City(CityDesc,ChangeBy,ChangeDate) VALUES (%s,%s,%s)""",(cname,username,date))
          db.commit()
          if add_rec > 0:
             #print "inserted"
             output = '{"function":"add_city","error_code":"0", "Response":"Successfully added the city : %s"}'%cname
             return mqttc.publish('jts/oyo/error',output)
             
          else:
             #print "insert filed"
             output = '{"function":"add_city","error_code":"-2", "error_desc": "Response= Unable to add city"}'
             return mqttc.publish('jts/oyo/error',output)
    except MySQLdb.Error, e:
        try:
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1])) 
        except IndexError:
            #print "MySQL Error: %s" % str(e)
            return mqttc.publish('jts/oyo/error',str(e))
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"add_city","error_code":"3", "error_desc": "Response=Failed to add the city"}'
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()
'''
#################### delete city ##############################################
def delete_city(mosq,obj,msg):
    db = MySQLdb.connect("localhost","oyoroom","ptljtsoyo","oyoDB")
    cursor = db.cursor()
    output_str = "delete_city - Unable to Authenticate/delete_city... "
    print "this is register string : ",str(msg.payload)
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM Oyo_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          print 'Login Data Existed'
          pass
       else:
          print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to add unit"
          output = '{"error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
           output_str += ", city_id is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           cid = data1['city_id']

       sqlq12= "DELETE FROM Oyo_City WHERE CityId="+cid
       print sqlq12
       del_rec=cursor.execute(sqlq12)
       db.commit()
       print del_rec
       if del_rec > 0:
          output = '{"error_code":"0", "Response":"Successfully deleted the city : %s"}'%cid
          return mqttc.publish('jts/oyo/error',output)
       else:
          output = '{"error_code":"-2", "error_desc": "Response= Unable to delete the city"}'
          return mqttc.publish('jts/oyo/error',output)
             
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1])) 
        except IndexError:
            print "MySQL Error: %s" % str(e)
            return mqttc.publish('jts/oyo/error',str(e))
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"error_code":"3", "error_desc": "Response=Failed to delete the city"}'
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()
'''
############### update city  #######################################
def update_city(mosq,obj,msg):
    print "update......."
    db = MySQLdb.connect("localhost","oyoroom","ptljtsoyo","oyoDB")
    cursor = db.cursor()
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
       return mqttc.publish('jts/oyo/error','{"function":"update_city","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"update_city","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"update_city","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM Oyo_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to update_city"
          output = '{"function":"update_city","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
           output_str += ", city_id is mandatory"
           output = '{"function":"update_city","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           cid = data1['city_id']

       if((data1.get('city_name') is None) or ((data1.get('city_name') is not  None) and (len(data1['city_name']) <= 0))):
           output_str += ", city_name is mandatory"
           output = '{"function":"update_city","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           cname = data1['city_name']
       
       #sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"' AND Longitude='"+long1+"' AND Latitude='"+lat+"'"
       upd_rec=cursor.execute ("""UPDATE Oyo_City SET CityDesc=%s,ChangeBy=%s,ChangeDate=%s WHERE CityId=%s""", (cname,username,date,cid))
       db.commit() 
       
       if upd_rec > 0:
          output = '{"function":"update_city","error_code":"0", "Response":"Successfully updated the city : %s"}'%cid
          return mqttc.publish('jts/oyo/error',output)
       else:
          output_str = "City not existed"
          output = '{"function":"update_city","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output) 

          
          
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish('jts/oyo/error',e.args[0], e.args[1])
        except IndexError:
            print "MySQL Error: %s" % str(e)
            return mqttc.publish('jts/oyo/error',str(e))
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"update_city","error_code":"3", "error_desc": "Response=Failed to update the city"}'
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()

######################## add premise ###############################################
def add_premise(mosq,obj,msg):
    print "add premise......."
    db = MySQLdb.connect("localhost","oyoroom","ptljtsoyo","oyoDB")
    cursor = db.cursor()
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
       return mqttc.publish('jts/oyo/error','{"function":"add_premise","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"add_premise","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"add_premise","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM Oyo_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to add premise"
          output = '{"function":"add_premise","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('premise_name') is None) or ((data1.get('premise_name') is not  None) and (len(data1['premise_name']) <= 0))):
           output_str += ", premise_name is mandatory"
           output = '{"function":"add_premise","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           pname = data1['premise_name']

       if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
           output_str += ", city_id is mandatory"
           output = '{"function":"add_premise","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           cid = data1['city_id']

       
       #sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"' AND Longitude='"+long1+"' AND Latitude='"+lat+"'"
       sqlq1 = "SELECT * FROM Oyo_Premise WHERE PremiseDesc='"+pname+"'"
       cursor.execute(sqlq1)
       results = cursor.fetchone()
       pid=''
       if results > 0:
          #print "unit existed",results[0]
          output = '{"function":"add_premise","error_code":"2", "error_desc": "Response=Premise already exists"}' 
          return mqttc.publish('jts/oyo/error',output)  
       else:
          #print "exitig........"
          add_rec=cursor.execute("""INSERT INTO Oyo_Premise(CityId,PremiseDesc,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s)""",(cid,pname,username,date))
          db.commit()
          if add_rec > 0:
             #print "inserted"
             output = '{"function":"add_premise","error_code":"0", "Response":"Successfully added the premise : %s"}'%pname
             return mqttc.publish('jts/oyo/error',output)
          else:
             output_str = "The premise not mapping to city"
             output = '{"function":"add_premise","error_code":"2", "error_desc": "Response=%s"}' %output_str
             return mqttc.publish('jts/oyo/error',output)
             
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]))
        except IndexError:
            print "MySQL Error: %s" % str(e)
            return mqttc.publish('jts/oyo/error',str(e))
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"add_premise","error_code":"3", "error_desc": "Response=Failed to add the premise"}'
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()
'''
#################### delete premise ##############################################
def delete_premise(mosq,obj,msg):
    print "delete premise......."
    db = MySQLdb.connect("localhost","oyoroom","ptljtsoyo","oyoDB")
    cursor = db.cursor()
    output_str = "delete_premise - Unable to Authenticate/delete_premise... "
    #print "this is register string : ",str(msg.payload)
    #data1 = json.loads(str(msg.payload))
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    #print "date ", date
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"function":"add_premise","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"add_premise","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"add_premise","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM Oyo_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to add unit"
          output = '{"function":"add_premise","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('premise_id') is None) or ((data1.get('premise_id') is not  None) and (len(data1['premise_id']) <= 0))):
           output_str += ", premise_id is mandatory"
           output = '{"function":"add_premise","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           pid = data1['premise_id']
             
       sqlq12= "DELETE FROM Oyo_Premise WHERE PremiseId='%s'" %(pid)
       print sqlq12
       errorcode=cursor.execute(sqlq12)
       db.commit()
       
       req2 = requests.post(url = 'http://cld003.jts-prod.in:5904/OyoApp/delete_premise/', data = msg.payload,verify=False)
       res = req2.text
       #res = json.loads(res)
       #print "response fro cld: ",res
       return mqttc.publish('jts/oyo/error',str(res))
       #errorcode=res['error_code']
       
       print errorcode
       if errorcode > 0:
          output = '{"error_code":"0", "Response":"Successfully deleted the premise"}'
          return mqttc.publish('jts/oyo/error',output)
       else:
          output = '{"error_code":"-2", "error_desc": "Response= Unable to delete the premise"}'
          return mqttc.publish('jts/oyo/error',output)
            
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (str(e.args[0])+str(e.args[1]))
            return None
        except IndexError:
            print "MySQL Error: %s" % str(e)
            return None
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"error_code":"3", "error_desc": "Response=Failed to delete the premise"}'
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()
'''
############### update premise  #######################################
def update_premise(mosq,obj,msg):
    print "update premise......."
    db = MySQLdb.connect("localhost","oyoroom","ptljtsoyo","oyoDB")
    cursor = db.cursor()
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
       return mqttc.publish('jts/oyo/error','{"function":"update_premise","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"update_premise","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"update_premise","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM Oyo_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to update_city"
          output = '{"function":"update_premise","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('premise_id') is None) or ((data1.get('premise_id') is not  None) and (len(data1['premise_id']) <= 0))):
           output_str += ", premise_id is mandatory"
           output = '{"function":"update_premise","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           pid = data1['premise_id']

       if((data1.get('premise_name') is None) or ((data1.get('premise_name') is not  None) and (len(data1['premise_name']) <= 0))):
           output_str += ", premise_name is mandatory"
           output = '{"function":"update_premise","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           pname = data1['premise_name']
      
       #sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"' AND Longitude='"+long1+"' AND Latitude='"+lat+"'"
       upd_rec=cursor.execute ("""UPDATE Oyo_Premise SET PremiseDesc=%s,ChangeBy=%s,ChangeDate=%s WHERE PremiseId=%s""", (pname,username,date,pid))
       db.commit() 
       
       if upd_rec > 0:
          output = '{"function":"update_premise","error_code":"0", "Response":"Successfully updated the premise : %s"}'%pid
          return mqttc.publish('jts/oyo/error',output)
       else:
          output_str = "premise not existed"
          output = '{"function":"update_premise","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output) 

          
          
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]))
        except IndexError:
            print "MySQL Error: %s" % str(e)
            return mqttc.publish('jts/oyo/error',str(e))
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"update_premise","error_code":"3", "error_desc": "Response=Failed to update the premise"}'
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()

###################### optional testing ################################
def test01(mosq,obj,msg):
    print "test01......."
    db = MySQLdb.connect("localhost","oyoroom","ptljtsoyo","oyoDB")
    cursor = db.cursor()
    output_str = "get_units_details - Unable to Authenticate/get_units_details... "
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')

    if(not data1):
        output_str += ", details are mandatory"
        output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"error_code":"2", "error_desc": "Response=%s"}' %output_str
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('password') is None):
        output_str += ",passsword is mandatory"
        output = '{"error_code":"2", "error_desc": "Response=%s"}' %output_str
        return mqttc.publish('jts/oyo/error',output)

    username    = data1['username']
    password    = data1['password']
    sqlq = "SELECT * FROM Oyo_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
    #sqlq = "SELECT * FROM Oyo_Users"
    #print "Checking Credentials : : ",sqlq
    cursor.execute(sqlq)
    results = cursor.fetchone()
    print "Checking Data exists or not : ",results
    if results > 0:
       pass
       #print 'Login Data Existed'
    else:
       #print 'Login Data not authorized so quiting ........Thanks'
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to get unit details"
       output = '{"error_code":"2", "error_desc": "Response=%s"}' %output_str
       return mqttc.publish('jts/oyo/error',output)
    try:
       sql1="SELECT * FROM Oyo_Unit_Details"
       if((data1.get('unitid') is not  None) and (len(data1['unitid']) > 0)):
         print "id existed"
         sql1+="WHERE "
       
       '''
       if((data1.get('UnitId') is None) or ((data1.get('UnitId') is not  None) and (len(data1['UnitId']) <= 0))):
           output_str += ", UnitId is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           unitid = data1['UnitId'] 
       sqlq2 = "SELECT Fan,Status,Temp,SetTemp,Runtime,Strength FROM Oyo_Unit_Details WHERE UnitId='"+unitid+"'"
       cursor.execute(sqlq2)
       get_units_rec = cursor.fetchall()
       if(len(get_units_rec) > 0):
        #{
           output = '{"error_code":"0", "Response":"Successfully got %d units", \n "get_units_details":' %len(get_units_rec)
           output += '['
           counter = 0
           for rec in get_units_rec:
           #{
              counter += 1
              if(counter == 1):
                output += '{"Fan":"%s","Status":"%s","Temp":"%s","SetTemp":"%s","Runtime":"%s","Strength":"%s"}' %(rec[0] ,rec[1],rec[2],rec[3],rec[4],rec[5])
              else:
                output += ',\n {"Fan":"%s","Status":"%s","Temp":"%s","SetTemp":"%s","Runtime":"%s","Strength":"%s"}' %(rec[0] ,rec[1],rec[2],rec[3],rec[4],rec[5])
           #}
           output += ']\n'
           output += '}'
           return mqttc.publish('jts/oyo/error',output)
        #}
       else:
           output = '{"error_code":"3", "error_desc": "Response=Failed to get the unit records, NO_DATA_FOUND"}'
           return mqttc.publish('jts/oyo/error',output)      
       '''
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"error_code":"3", "error_desc": "Response=Failed to get the units"}'
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()

################## add floor #####################################
def add_floor(mosq,obj,msg):
    print "add_floor......."
    db = MySQLdb.connect("localhost","oyoroom","ptljtsoyo","oyoDB")
    cursor = db.cursor()
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
       return mqttc.publish('jts/oyo/error','{"function":"add_floor","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"add_floor","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"add_floor","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM Oyo_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to add premise"
          output = '{"function":"add_floor","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('floor_name') is None) or ((data1.get('floor_name') is not  None) and (len(data1['floor_name']) <= 0))):
           output_str += ", floor_name is mandatory"
           output = '{"function":"add_floor","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           fname = data1['floor_name']

       if((data1.get('premise_id') is None) or ((data1.get('premise_id') is not  None) and (len(data1['premise_id']) <= 0))):
           output_str += ", premise_id is mandatory"
           output = '{"function":"add_floor","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           pid = data1['premise_id']

       if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
           output_str += ", city_id is mandatory"
           output = '{"function":"add_floor","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           cid = data1['city_id'] 
       #sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"' AND Longitude='"+long1+"' AND Latitude='"+lat+"'"
       sqlq1 = "SELECT * FROM Oyo_Floors WHERE FloorDesc='"+fname+"'"
       cursor.execute(sqlq1)
       results = cursor.fetchone()
       fid=''
       if results > 0:
          #print "unit existed"
          output_str = "The floor is already exists"
          output = '{"function":"add_floor","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)

          
       else:
          #print "inserted"
          add_rec1=cursor.execute("""INSERT INTO Oyo_Floors(CityId,PremiseId,FloorDesc,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s)""",(cid,pid,fname,username,date))
          db.commit()
          if add_rec1 > 0:
             output = '{"function":"add_floor","error_code":"0", "Response":"Successfully added the floor : %s"}'%fname
             return mqttc.publish('jts/oyo/error',output)
          else:
             output_str = "The floor not mapping to premise"
             output = '{"function":"add_floor","error_code":"2", "error_desc": "Response=%s"}' %output_str
             return mqttc.publish('jts/oyo/error',output)
            
            
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]))
        except IndexError:
            print "MySQL Error: %s" % str(e)
            return mqttc.publish('jts/oyo/error',str(e))
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"add_floor","error_code":"3", "error_desc": "Response=Failed to add the floor"}'
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()

######################### update floor ####################################
def update_floor(mosq,obj,msg):
    print "update_floor......."
    db = MySQLdb.connect("localhost","oyoroom","ptljtsoyo","oyoDB")
    cursor = db.cursor()
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
       return mqttc.publish('jts/oyo/error','{"function":"update_floor","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"update_floor","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"update_floor","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM Oyo_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to update_city"
          output = '{"function":"update_floor","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('floor_id') is None) or ((data1.get('floor_id') is not  None) and (len(data1['floor_id']) <= 0))):
           output_str += ", floor_id is mandatory"
           output = '{"function":"update_floor","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           fid = data1['floor_id']

       if((data1.get('floor_name') is None) or ((data1.get('floor_name') is not  None) and (len(data1['floor_name']) <= 0))):
           output_str += ", floor_name is mandatory"
           output = '{"function":"update_floor","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           fname = data1['floor_name']
      
       #sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"' AND Longitude='"+long1+"' AND Latitude='"+lat+"'"
       upd_rec=cursor.execute ("""UPDATE Oyo_Floors SET FloorDesc=%s,ChangeBy=%s,ChangeDate=%s WHERE FloorId=%s""", (fname,username,date,fid))
       db.commit() 
       
       if upd_rec > 0:
          output = '{"function":"update_floor","error_code":"0", "Response":"Successfully updated the floor "}'
          return mqttc.publish('jts/oyo/error',output)
       else:
          output_str = "premise not existed"
          output = '{"function":"update_floor","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output) 

          
          
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]))
        except IndexError:
            print "MySQL Error: %s" % str(e)
            return mqttc.publish('jts/oyo/error',str(e))
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"update_floor","error_code":"3", "error_desc": "Response=Failed to update the floor"}'
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()
################### add room ####################################################
def add_room(mosq,obj,msg):
    print "add_rooom......."
    db = MySQLdb.connect("localhost","oyoroom","ptljtsoyo","oyoDB")
    cursor = db.cursor()
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
       return mqttc.publish('jts/oyo/error','{"function":"add_room","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"add_room","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"add_room","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM Oyo_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to add premise"
          output = '{"function":"add_room","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('room_name') is None) or ((data1.get('room_name') is not  None) and (len(data1['room_name']) <= 0))):
           output_str += ", room_name is mandatory"
           output = '{"function":"add_room","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           rname = data1['room_name']

       if((data1.get('floor_id') is None) or ((data1.get('floor_id') is not  None) and (len(data1['floor_id']) <= 0))):
           output_str += ", floor_id is mandatory"
           output = '{"function":"add_room","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           fid = data1['floor_id']

       if((data1.get('premise_id') is None) or ((data1.get('premise_id') is not  None) and (len(data1['premise_id']) <= 0))):
           output_str += ", premise_id is mandatory"
           output = '{"function":"add_room","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           pid = data1['premise_id']

       if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
           output_str += ", city_id is mandatory"
           output = '{"function":"add_room","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           cid = data1['city_id']
       
       #sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"' AND Longitude='"+long1+"' AND Latitude='"+lat+"'"
       sqlq1 = "SELECT * FROM Oyo_Rooms WHERE RoomDesc='"+rname+"'"
       cursor.execute(sqlq1)
       results = cursor.fetchone()
       
       if results > 0:
          #print "unit existed"
          output_str = "The room already exists"
          output = '{"function":"add_room","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)
          
       else:
          #print "exitig........"
          add_rec=cursor.execute("""INSERT INTO Oyo_Rooms(CityId,PremiseId,RoomDesc,FloorId,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s)""",(cid,pid,rname,fid,username,date))
          db.commit()
          if add_rec > 0:
             #print "inserted"
             output = '{"function":"add_room","error_code":"0", "Response":"Successfully added the room: %s"}'%rname
             return mqttc.publish('jts/oyo/error',output)
          else:
             output_str = "The room not mapping to floor"
             output = '{"error_code":"2", "error_desc": "Response=%s"}' %output_str
             return mqttc.publish('jts/oyo/error',output)
             
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]))
        except IndexError:
            print "MySQL Error: %s" % str(e)
            return mqttc.publish('jts/oyo/error',str(e))
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"add_room","error_code":"3", "error_desc": "Response=Failed to add the room"}'
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()

######################### update room ####################################
def update_room(mosq,obj,msg):
    print "update_room......."
    db = MySQLdb.connect("localhost","oyoroom","ptljtsoyo","oyoDB")
    cursor = db.cursor()
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
       return mqttc.publish('jts/oyo/error','{"function":"update_room","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"update_room","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"update_room","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM Oyo_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          #print 'Login Data Existed'
          pass
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to update_city"
          output = '{"function":"update_room","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('room_id') is None) or ((data1.get('room_id') is not  None) and (len(data1['room_id']) <= 0))):
           output_str += ", room_id is mandatory"
           output = '{"function":"update_room","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           rid = data1['room_id']

       if((data1.get('room_name') is None) or ((data1.get('room_name') is not  None) and (len(data1['room_name']) <= 0))):
           output_str += ", room_name is mandatory"
           output = '{"function":"update_room","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           rname = data1['room_name']
      
       #sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"' AND Longitude='"+long1+"' AND Latitude='"+lat+"'"
       upd_rec=cursor.execute ("""UPDATE Oyo_Rooms SET RoomDesc=%s,ChangeBy=%s,ChangeDate=%s WHERE RoomId=%s""", (rname,username,date,rid))
       db.commit() 
       
       if upd_rec > 0:
          output = '{"function":"update_room","error_code":"0", "Response":"Successfully updated the room "}'
          return mqttc.publish('jts/oyo/error',output)
       else:
          output_str = "room not existed"
          output = '{"function":"update_room","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output) 

          
          
    except MySQLdb.Error, e:
        try:
            print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]))
        except IndexError:
            print "MySQL Error: %s" % str(e)
            return mqttc.publish('jts/oyo/error',str(e))
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"update_room","error_code":"3", "error_desc": "Response=Failed to update the room"}'
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()

###################### delete unit ##########################################
def delete_unit(mosq,obj,msg):
       print "delete unit......."
       req2 = requests.post(url = 'http://localhost:5904/OyoApp/delete_unit/', data = msg.payload,verify=False)
       res = req2.text
       #res = json.loads(res)
       #print "response fro cld: ",res
       return mqttc.publish('jts/oyo/error',str(res))
       

###################### delete room ##########################################
def delete_room(mosq,obj,msg):
       print "delete room......."
       req2 = requests.post(url = 'http://localhost:5904/OyoApp/delete_room/', data = msg.payload,verify=False)
       res = req2.text
       #res = json.loads(res)
       #print "response fro cld: ",res
       return mqttc.publish('jts/oyo/error',str(res))
       
###################### delete floor ##########################################
def delete_floor(mosq,obj,msg):
       print "delete floor......."
       req2 = requests.post(url = 'http://localhost:5904/OyoApp/delete_floor/', data = msg.payload,verify=False)
       res = req2.text
       #res = json.loads(res)
       #print "response fro cld: ",res
       return mqttc.publish('jts/oyo/error',str(res))
       

###################### delete city ##########################################
def delete_city(mosq,obj,msg):
       print "delete city......."
       req2 = requests.post(url = 'http://localhost:5904/OyoApp/delete_city/', data = msg.payload,verify=False)
       res = req2.text
       #res = json.loads(res)
       #print "response fro cld: ",res
       return mqttc.publish('jts/oyo/error',str(res))
       
################# delete premise ##############################################
def delete_premise(mosq,obj,msg):
       print "delete premise......."
       req2 = requests.post(url = 'http://localhost:5904/OyoApp/delete_premise/', data = msg.payload,verify=False)
       res = req2.text
       #res = json.loads(res)
       #print "response fro cld: ",res
       return mqttc.publish('jts/oyo/error',str(res))

###################### delete city ##########################################
def get_units_test(mosq,obj,msg):
    print "get units test ......."
    db = MySQLdb.connect("localhost","oyoroom","ptljtsoyo","oyoDB")
    cursor = db.cursor()
    sqlq = "select count(Temp) from Oyo_Unit_History where Temp between 23 and 25 GROUP BY CityId"
    cursor.execute(sqlq)
    good = cursor.fetchall()
    print "response fro cld good : ",good
    
    sqlq1 = "select count(Temp) from Oyo_Unit_History where Temp between 25 and 27 GROUP BY CityId"
    cursor.execute(sqlq1)
    warning = cursor.fetchall()
    print "response fro cld warn : ",warning
    
    sqlq2 = "select count(Temp) from Oyo_Unit_History where Temp between 20 and 22 GROUP BY CityId"
    cursor.execute(sqlq2)
    problem = cursor.fetchall()
    print "response fro cld problem : ",problem

    sqlq = "select count(Temp) from Oyo_Unit_History where Temp=50 GROUP BY CityId"
    cursor.execute(sqlq)
    main = cursor.fetchall()
    print "response fro cld maintenance : ",main

    sqlq = "select Oyo_City.CityId,Oyo_City.CityDesc,round(avg(Temp)) as avgtemp from Oyo_Unit_History,Oyo_City GROUP BY CityId;"
    cursor.execute(sqlq)
    test = cursor.fetchall()
    print "response fro cld test : ",test
    desc = cursor.description
    print len(desc)
    for row in test:
        for col in range(len(desc)):
            #print "%s=%s" % (desc[col][0], row[col] )
            print "%s" % (row[col] )

    for i,rec in enumerate(test): 
    #for rec in range(0,len(test)):
        print rec,i
        #print good[rec],warning[rec],

################## publish response #################################################
def on_publish(client, userdata, result):
        print "data published \n"
######################### mqtt methods ####################################
mqttc = mqtt.Client()
#################### esp calls ####################################
mqttc.message_callback_add('jts/oyo/add_unit',add_unit)
mqttc.message_callback_add('jts/oyo/temp',temp1)
mqttc.message_callback_add('jts/oyo/op',operations)
######## web calls###############################
mqttc.message_callback_add('jts/oyo/validate_login',login)
mqttc.message_callback_add('jts/oyo/get_roles',get_roles)
mqttc.message_callback_add('jts/oyo/add_user',add_user)

mqttc.message_callback_add('jts/oyo/get_city',get_city)
mqttc.message_callback_add('jts/oyo/add_city',add_city)
mqttc.message_callback_add('jts/oyo/delete_city',delete_city)
mqttc.message_callback_add('jts/oyo/update_city',update_city)

mqttc.message_callback_add('jts/oyo/get_city_premise',get_premise)
mqttc.message_callback_add('jts/oyo/add_premise',add_premise)
mqttc.message_callback_add('jts/oyo/delete_premise',delete_premise)
mqttc.message_callback_add('jts/oyo/update_premise',update_premise)

mqttc.message_callback_add('jts/oyo/add_floor',add_floor)
mqttc.message_callback_add('jts/oyo/update_floor',update_floor)
mqttc.message_callback_add('jts/oyo/get_floors',get_floors)
mqttc.message_callback_add('jts/oyo/delete_floor',delete_floor)

mqttc.message_callback_add('jts/oyo/get_rooms',get_rooms)
mqttc.message_callback_add('jts/oyo/add_room',add_room)
mqttc.message_callback_add('jts/oyo/update_room',update_room)
mqttc.message_callback_add('jts/oyo/delete_room',delete_room)

mqttc.message_callback_add('jts/oyo/get_units_all',get_units)
mqttc.message_callback_add('jts/oyo/delete_unit',delete_unit)
mqttc.message_callback_add('jts/oyo/get_units_test',get_units_test)

#mqttc.message_callback_add('jts/oyo/get_history',get_history)
#mqttc.message_callback_add('jts/oyo/add_roles',add_roles)
#mqttc.message_callback_add('jts/oyo/mod_roles',mod_roles)
#mqttc.message_callback_add('jts/oyo/del_roles',del_roles)
mqttc.message_callback_add('jts/oyo/test01',test01)


mqttc.on_publish = on_publish
mqttc.username_pw_set('esp', 'ptlesp01')
mqttc.connect("cld003.jts-prod.in", 1883, 60)
mqttc.subscribe("jts/oyo/#")
mqttc.loop_forever()
#mqttc.username_pw_set('esp', 'ptlesp01')




