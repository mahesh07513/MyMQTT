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
####################################################
def id_generator(size=9, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

##################### Login #########################
def login(mosq,obj,msg):
    #print "login......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
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

       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"validate_login","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']

       if(data1.get('username') is None):
          output_str += ",username is mandatory"
          output = '{"function":"validate_login","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          return mqttc.publish('jts/oyo/error',output)


       
       if(data1.get('password') is None):
          output_str += ",password is mandatory"
          output = '{"function":"validate_login","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          return mqttc.publish('jts/oyo/error',output)


       

       username    = data1['username']
       password    = data1['password']
       

       sqlq = "SELECT ImgId FROM AC_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       #sqlq = "SELECT * FROM Oyo_Users"
       #print "Checking Credentials : : ",sqlq
       cursor.execute(sqlq)
       results = cursor.fetchone()
       #print "Checking Data exists or not : ",results
       if results > 0:
          imgid=results[0]
          sqlq1 = "SELECT Image FROM AC_Images WHERE ImgId='%s'" %(imgid)
          cursor.execute(sqlq1)
          results1 = cursor.fetchone()
          imgpath='http://cld003.jts-prod.in:5904/GenericACApp/media/'
          #print results1
          if results1 > 0:
             imagname=results1[0]
             imgpath=imgpath+imagname
          else:
             imagname='Jochebed.png'
             imgpath=imgpath+imagname
          #print 'Login Data Existed'
          output = '{"function":"login","session_id":"%s","error_code":"0", "Response":"Successfully Authenticated for user: %s","img_path":"%s"}' %(sid,username,imgpath)
          #print output
	  return mqttc.publish('jts/oyo/error',output)
       else:
          #print 'Login Data not authorized so quiting ........Thanks'
          cursor.close()
          db.close()
          output = '{"function":"validate_login","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)          
          return mqttc.publish('jts/oyo/error',output)

    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"validate_login","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to login"}' %sid
        return mqttc.publish('jts/oyo/error',output)
    
################ add_unit ############################
'''
def add_unit(mosq,obj,msg):
    print "add_unit......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
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

       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"add_unit","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']

       if(data1.get('username') is None):
          output_str += ",username is mandatory"
          output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          return mqttc.publish('jts/oyo/error',output)
       
       if(data1.get('password') is None):
          output_str += ",password is mandatory"
          output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
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
          output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          return mqttc.publish('jts/oyo/error',output)
   
       if((data1.get('ipmacid') is None) or ((data1.get('ipmacid') is not  None) and (len(data1['ipmacid']) <= 0))):
           output_str += ", ipmacid is mandatory"
           output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           return mqttc.publish('jts/oyo/error',output)
       else:
           ipmacid = data1['ipmacid'] 
           		 
       if((data1.get('opmacid') is None) or ((data1.get('opmacid') is not  None) and (len(data1['opmacid']) <= 0))):
           output_str += ", opmacid is mandatory"
           output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           return mqttc.publish('jts/oyo/error',output)
       else:
           opmacid = data1['opmacid'] 
           
       if((data1.get('floor_id') is None) or ((data1.get('floor_id') is not  None) and (len(data1['floor_id']) <= 0))):
           output_str += ", floor_id is mandatory"
           output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           return mqttc.publish('jts/oyo/error',output)
       else:
           floor = data1['floor_id']
           
       if((data1.get('room_id') is None) or ((data1.get('room_id') is not  None) and (len(data1['room_id']) <= 0))):
           output_str += ", room_id is mandatory"
           output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           return mqttc.publish('jts/oyo/error',output)
       else:
           room = data1['room_id']
          

       if((data1.get('premise_id') is None) or ((data1.get('premise_id') is not  None) and (len(data1['premise_id']) <= 0))):
           output_str += ", premise_id is mandatory"
           output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           return mqttc.publish('jts/oyo/error',output)
       else:
           premise = data1['premise_id']

       if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
           output_str += ", city_id is mandatory"
           output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
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
       #print "mapping data from db: ",results2
       if results2 > 0:
          #print 'maping data exits......... so quiting ..Thanks'
          cursor.close()
          db.close()
          output = '{"function":"add_unit","session_id":"%s","error_code":"-2", "error_desc": "Response=Operate macid asociate with this only, no need to add"}' %(sid)
          return mqttc.publish('jts/oyo/error',output)
       else:
          #print 'start mapping .......'
          #print(floor,premise,ipmacid,opmacid,username,date)
          #print"""INSERT INTO Oyo_Units(FloorId,PremiseId,IpmacId,OpmacId,ChangeBy,ChangeDate) VALUES (%d,%d,%d,%d,%s,%s)""",(int(floor),int(premise),int(ipmacid),int(opmacid),username,date)
          #print("""INSERT INTO Oyo_Units(FloorId,RoomId,PremiseId,IpmacId,OpmacId,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s)""",(floor,room,premise,ipmid,opmid,username,date))
          add_rec1=cursor.execute("""INSERT INTO Oyo_Units(CityId,FloorId,RoomId,PremiseId,IpmacId,OpmacId,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",(city,floor,room,premise,ipmacid,opmacid,username,date))
          db.commit()
          if add_rec1 > 0:
             #print 'input mac and operate  mac rec mapped in DB.'
             sqlq2="SELECT UnitId FROM Oyo_Units WHERE IpmacId='%s' AND OpmacId='%s'" %(ipmacid,opmacid)
             cursor.execute(sqlq2)
             results3 = cursor.fetchone()
             #print "getting unit id", results3[0]
             if results3 > 0:
                add_rec1=cursor.execute("""INSERT INTO Oyo_Unit_Details(UnitId,Temp,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s)""",(results3[0],'0',username,date))
                db.commit()
                add_rec2=cursor.execute("""INSERT INTO Oyo_Unit_History(UnitId,CityId,PremiseId,FloorId,RoomId,Temp,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",(results3[0],city,premise,floor,room,'0',username,date))
                db.commit()
                if add_rec1 > 0 and add_rec2 > 0 :
                   #print "both added"
                   cursor.close()
                   db.close()
                   output = '{"function":"add_unit","session_id":"%s","error_code":"0", "Response":"Successfully Registered Input Unit and Operate Unit"}' %(sid)
	           output1 = '{"function":"add_unit","error_code":"0", "Response":"Succes","opmacid":"%s"}'%(opmacid)
                   mqttc.publish('jts/oyo/'+ipmacid,output1)
                   return mqttc.publish('jts/oyo/error',output)
          else:
             #print 'mapping issues .....check it.'
             cursor.close()
             db.close()
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=Falied to add a Units"}' %(sid)
             return mqttc.publish('jts/oyo/error',output)

    except MySQLdb.Error, e:
        try:
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]) )
        except IndexError:
            #print "MySQL Error: %s" % str(e)
            return mqttc.publish('jts/oyo/error',str(e)) 
       
    except Exception, e:  
        cursor.close()
        db.close()
	output = '{"function":"add_unit","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to add the Register"}' %(sid) 
        return mqttc.publish('jts/oyo/error',output)
'''
####################### operations###################################
def operations(mosq,obj,msg):
    pass
    #print "this is operation",str(msg.payload)
    #mqttc.publish('jts/oyo/res','{"user":"mahesh1","pass":"babu1"}')
###################### getTemparature from OP units ##################
def temp1(mosq,obj,msg):
    #print "Temp........ "
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
    cursor = db.cursor()
    output_str = "register - Unable to Authenticate/register... " 
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}' )
    try:
       imac=False
       omac=False

       

       if((data1.get('macid') is None) or ((data1.get('macid') is not  None) and (len(data1['macid']) <= 0))):
           output_str += ", macid is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           macid = data1['macid']

           sqlq1 = "SELECT * FROM AC_Units WHERE OpmacId='"+macid+"'"
           #sqlq = "SELECT * FROM Oyo_Users"
           #print "Checking input mac id ....... :",sqlq1
           cursor.execute(sqlq1)
           results = cursor.fetchone()
           if results > 0 :
              omac=True
           sqlq2 = "SELECT * FROM AC_Units WHERE IpmacId='"+macid+"'"
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

          sqlq1 = "SELECT UnitId,CityId,PremiseId,FloorId,RoomId FROM AC_Units WHERE OpmacId='"+macid+"'"
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
             sqlq3 = "SELECT * FROM AC_Unit_Details WHERE UnitId='%s'" %(unitid)
             #print sqlq3
	     cursor.execute(sqlq3)
             results2 = cursor.fetchone()
             #print "checking in units details ...",results2		
             if results2>0:
                #print "update"
                #print 'id is there. so updating .........Thanks.'
                upd_rec=cursor.execute ("""UPDATE AC_Unit_Details SET Fan=%s,Status=%s,SetTemp=%s,Voltage=%s,Power=%s,Current=%s,MotionE=%s,ChangeDate=%s,ChangeBy=%s WHERE UnitId=%s""", (fan,status,settemp,voltage,power,current,me,date,'jtsadmin',unitid))
                db.commit()
        
                add_rec2=cursor.execute("""INSERT INTO AC_Unit_History(UnitId,CityId,PremiseId,FloorId,RoomId,Fan,Status,SetTemp,Voltage,Power,Current,MotionE,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(unitid,cid,pid,fid,rid,fan,status,settemp,voltage,power,current,me,'jtsadmin',date))
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
                add_rec1=cursor.execute("""INSERT INTO AC_Unit_Details(UnitId,Fan,Status,SetTemp,Voltage,Power,Current,MotionE,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(unitid,fan,status,settemp,voltage,power,current,me,'jtsadmin',date))
                db.commit()
                add_rec2=cursor.execute("""INSERT INTO AC_Unit_History(UnitId,CityId,PremiseId,FloorId,RoomId,Fan,Status,SetTemp,Voltage,Power,Current,MotionE,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(unitid,cid,pid,fid,rid,fan,status,settemp,voltage,power,current,me,'jtsadmin',date))
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
 
          sqlq1 = "SELECT UnitId,CityId,PremiseId,FloorId,RoomId FROM AC_Units WHERE IpmacId='"+macid+"'"
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
             upd_rec=cursor.execute ("""UPDATE AC_Units SET Temp=%s WHERE IpmacId=%s""", (temp,macid))
 	     db.commit()
             #print "after update"
             sqlq3 = "SELECT * FROM AC_Unit_Details WHERE UnitId='%s'" %(unitid)
             #print sqlq3
	     cursor.execute(sqlq3)
             results2 = cursor.fetchone()
             #print "checking in units details ...",results2		
             if results2>0:
                #print "update"
                #print 'id is there. so updating .........Thanks.',temp
                upd_rec=cursor.execute ("""UPDATE AC_Unit_Details SET Temp=%s,Motion=%s,ChangeDate=%s,ChangeBy=%s WHERE UnitId=%s""", (temp,motion,date,'jtsadmin',unitid))
                db.commit()
                #print "inside update"
                add_rec2=cursor.execute("""INSERT INTO AC_Unit_History(UnitId,CityId,PremiseId,FloorId,RoomId,Temp,Motion,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(unitid,cid,pid,fid,rid,temp,motion,'jtsadmin',date))
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
                add_rec1=cursor.execute("""INSERT INTO AC_Unit_Details(UnitId,Temp,Motion,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s)""",(unitid,temp,motion,'jtsadmin',date))
                db.commit()
                add_rec2=cursor.execute("""INSERT INTO AC_Unit_History(UnitId,CityId,PremiseId,FloorId,RoomId,Temp,Motion,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(unitid,cid,pid,fid,rid,temp,motion,'jtsadmin',date))
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
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]))
        except IndexError:
            #print "MySQL Error: %s" % str(e)
            return mqttc.publish('jts/oyo/error',str(e)) 
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"error_code":"3", "error_desc": "Response=Failed to add the temp"}'
        return mqttc.publish('jts/oyo/error',output)

################################ get_roles #########################
def get_roles(mosq,obj,msg):
    #print "get_roles......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
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


    if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"get_roles","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

    sid = data1['session_id']

    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"function":"get_roles","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('password') is None):
        output_str += ",passsword is mandatory"
        output = '{"function":"get_roles","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/oyo/error',output)

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
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to get roles"
       output = '{"function":"get_roles","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
       return mqttc.publish('jts/oyo/error',output)
    try:
       sqlq2 = "SELECT AdminId,AdminDesc FROM AC_Admin "
       cursor.execute(sqlq2)
       get_roles_rec = cursor.fetchall()
       if(len(get_roles_rec) > 0):
        #{
          output = '{"function":"get_roles","session_id":"%s","error_code":"0", "Response":"Successfully got %d roles", \n "get_roles":' %(sid,len(get_roles_rec))
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
           output = '{"function":"get_roles","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the role records, NO_DATA_FOUND"}' %(sid)
           return mqttc.publish('jts/oyo/error',output)
      
          
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_roles","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the roles"}' %(sid)
        return mqttc.publish('jts/oyo/error',output)

####################### add_user #################################
def add_user(mosq,obj,msg):
    #print "add_user......."
    #print "this is add_users : ",str(msg.payload)
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
    cursor = db.cursor()
    output_str = "add_user - Unable to Authenticate/add_user... " 
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"function":"add_user","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')

    if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"add_user","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

    sid = data1['session_id']


    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"function":"add_user","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('password') is None):
       output_str += ",passsword is mandatory"
       output = '{"function":"add_user","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
       return mqttc.publish('jts/oyo/error',output)

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
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to add the user"
       output = '{"function":"add_user","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
       return mqttc.publish('jts/oyo/error',output)
    try:

           
       if((data1.get('Uname') is None) or ((data1.get('Uname') is not  None) and (len(data1['Uname']) <= 0))):
           output_str += ", Uname is mandatory"
           output = '{"function":"add_user","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           return mqttc.publish('jts/oyo/error',output)
       else:
           uname = data1['Uname'] 

       if((data1.get('Pass') is None) or ((data1.get('Pass') is not  None) and (len(data1['Pass']) <= 0))):
           output_str += ", Pass is mandatory"
           output = '{"function":"add_user","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           return mqttc.publish('jts/oyo/error',output)
       else:
           pass1 = data1['Pass'] 
 
       if((data1.get('Role') is None) or ((data1.get('Role') is not  None) and (len(data1['Role']) <= 0))):
           output_str += ", Role is mandatory"
           output = '{"function":"add_user","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           return mqttc.publish('jts/oyo/error',output)
       else:
           role = data1['Role']
 
       if((data1.get('client_id') is None) or ((data1.get('client_id') is not  None) and (len(data1['client_id']) <= 0))):
           output_str += ",client_id is mandatory"
           output = '{"function":"add_user","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           return mqttc.publish('jts/oyo/error',output)
       else:
           cid = data1['client_id']

       if((data1.get('image_id') is None) or ((data1.get('image_id') is not  None) and (len(data1['image_id']) <= 0))):
           output_str += ",image_id is mandatory"
           output = '{"function":"add_user","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           return mqttc.publish('jts/oyo/error',output)
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
          return mqttc.publish('jts/oyo/error',output)
       else:
          #print "inserting ........."
          add_rec1=cursor.execute("""INSERT INTO AC_Users(Username,Password,ChangeBy,Role,ClientId,ImgId,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s)""",(uname,pass1,username,role,cid,imgid,date))
          db.commit()
          if add_rec1>0:
             output = '{"function":"add_user","session_id":"%s","error_code":"0", "Response":"Succesfully added user"}' %(sid)
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
        output = '{"function":"add_user","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to add the User"}' %(sid)
        return mqttc.publish('jts/oyo/error',output)

########################### get premices #####################################
def get_premise(mosq,obj,msg):
    #print "get_premise......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
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

    if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"get_premise","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

    sid = data1['session_id']

    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"function":"get_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('password') is None):
       output_str += ",passsword is mandatory"
       output = '{"function":"get_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
       return mqttc.publish('jts/oyo/error',output)
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
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to get premise"
       output = '{"function":"get_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
       return mqttc.publish('jts/oyo/error',output)
    try:
       if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
           output_str += ", city_id is mandatory"
           output = '{"function":"get_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           return mqttc.publish('jts/oyo/error',output)
       else:
           city_id = data1['city_id']

       sqlq2 = "SELECT PremiseId,PremiseDesc FROM AC_Premise where CityId='"+city_id+"' and ChangeBy='"+username+"'"
       #sqlq2="select Oyo_City_Premise.PremiseId, Oyo_Premise.PremiseDesc from Oyo_City_Premise inner join Oyo_Premise on Oyo_City_Premise.PremiseId=Oyo_Premise.PremiseId where CityId='"+city_id+"'"
       cursor.execute(sqlq2)
       get_premise_rec = cursor.fetchall()
       if(len(get_premise_rec) > 0):
        #{
           output = '{"function":"get_premise","session_id":"%s","error_code":"0", "Response":"Successfully got %d premise", \n "get_premise":' %(sid,len(get_premise_rec))
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
           output = '{"function":"get_premise","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the premise records, NO_DATA_FOUND"}' %(sid)
           return mqttc.publish('jts/oyo/error',output)

    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_premise","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the premise"}' %(sid)
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()
############################# get units##########################
def get_units(mosq,obj,msg):
    #print "get_units......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
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

    if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"get_units","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

    sid = data1['session_id']

    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"function":"get_units","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('password') is None):
       output_str += ",passsword is mandatory"
       output = '{"function":"get_units","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
       return mqttc.publish('jts/oyo/error',output)

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
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to get units"
       output = '{"function":"get_units","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
       return mqttc.publish('jts/oyo/error',output)
    try:
       history_all=False
       histoty_city=False
       history_premise=False
       get_units=False

       if((data1.get('history') is None) or ((data1.get('history') is not  None) and (len(data1['history']) <= 0))):
           output_str += ", history is mandatory"
           output = '{"function":"get_units","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
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
                 output = '{"function":"get_units","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
                 return mqttc.publish('jts/oyo/error',output)
              else:
                 pid = data1['premise_id']
              sqlq2="select i.UnitId,i.IpMacid,i.IpName,o.OpMacid,o.OpName,p.PMacId,p.PName,o.Fan,o.Status,i.Temp,o.setTemp,o.Voltage,o.Power,o.Current,p.Voltage,p.Power,p.Current,o.UnitId,p.UnitId from AC_Input_Units as i left join AC_Operate_Units as o USING (UnitId) left join AC_Power_Units as p USING (UnitId) where i.PremiseId='%s' or o.PremiseId='%s' or p.PremiseId='%s' union select i.UnitId,i.IpMacid,i.IpName,o.OpMacid,o.OpName,p.PMacId,p.PName,o.Fan,o.Status,i.Temp,o.setTemp,o.Voltage,o.Power,o.Current,p.Voltage,p.Power,p.Current,o.UnitId,p.UnitId from AC_Input_Units as i right join AC_Operate_Units as o USING (UnitId) right join AC_Power_Units as p USING (UnitId) where i.PremiseId='%s' or o.PremiseId='%s' or p.PremiseId='%s' union select i.UnitId,i.IpMacid,i.IpName,o.OpMacid,o.OpName,p.PMacId,p.PName,o.Fan,o.Status,i.Temp,o.setTemp,o.Voltage,o.Power,o.Current,p.Voltage,p.Power,p.Current,o.UnitId,p.UnitId from AC_Input_Units as i left join AC_Operate_Units as o USING (UnitId) right join AC_Power_Units as p USING (UnitId) where i.PremiseId='%s' or o.PremiseId='%s' or p.PremiseId='%s'" %(pid,pid,pid,pid,pid,pid,pid,pid,pid)
              '''
              sqlq2 = "SELECT * FROM AC_Input_Units left OUTER JOIN AC_Operate_Units ON(AC_Input_Units.PremiseId = AC_Operate_Units.PremiseId ) left OUTER JOIN AC_Power_Units ON(AC_Operate_Units.PremiseId = AC_Power_Units.PremiseId ) WHERE AC_Input_Units.PremiseId='%s' or AC_Operate_Units.PremiseId='%s' or AC_Power_Units.PremiseId='%s'" %(pid,pid,pid)
              sqlq2+="union select * from AC_Input_Units RIGHT OUTER JOIN AC_Operate_Units ON(AC_Input_Units.PremiseId = AC_Operate_Units.PremiseId) RIGHT OUTER JOIN AC_Power_Units ON(AC_Operate_Units.PremiseId = AC_Power_Units.PremiseId) WHERE AC_Input_Units.PremiseId='%s' or AC_Operate_Units.PremiseId='%s' or AC_Power_Units.PremiseId='%s'" %(pid,pid,pid)
              sqlq2+="union select * from AC_Input_Units right outer join AC_Operate_Units ON(AC_Input_Units.PremiseId = AC_Operate_Units.PremiseId) left OUTER JOIN AC_Power_Units ON(AC_Operate_Units.PremiseId = AC_Power_Units.PremiseId) WHERE AC_Input_Units.PremiseId='%s' or AC_Operate_Units.PremiseId='%s' or AC_Power_Units.PremiseId='%s' " %(pid,pid,pid)
              #print sqlq2
              '''
              #sqlq2="SELECT i.UnitId,i.IpMacid,i.IpName,o.OpMacid,o.OpName,p.PMacId,p.PName,o.Fan,o.Status,i.Temp,o.setTemp,o.Voltage,o.Power,o.Current,p.Voltage,p.Power,p.Current FROM AC_Input_Units AS i LEFT JOIN AC_Operate_Units AS o ON (i.UnitId=o.UnitId) LEFT JOIN AC_Power_Units AS p ON (i.UnitId=p.UnitId) WHERE i.PremiseId='%s'and i.ChangeBy='%s' ORDER BY UnitId" %(pid,username)
              #sqlq2="select AC_Input_Units.IpId,AC_Input_Units.IpMacid,AC_Operate_Units.OpMacid,AC_Operate_Units.Fan,AC_Operate_Units.Status,AC_Input_Units.Temp,AC_Operate_Units.setTemp,AC_Operate_Units.Voltage,AC_Operate_Units.Power,AC_Operate_Units.Current from  AC_Input_Units,AC_Operate_Units where AC_Input_Units.PremiseId=AC_Operate_Units.PremiseId=%s" %(pid)
#              sqlq2 = "select Oyo_Units.UnitId,Oyo_Units.IpmacId,Oyo_Units.OpmacId,Oyo_Unit_Details.Fan,Oyo_Unit_Details.Status,Oyo_Unit_Details.Temp,Oyo_Unit_Details.SetTemp,Oyo_Unit_Details.Voltage,Oyo_Unit_Details.Power,Oyo_Unit_Details.Current from Oyo_Unit_Details inner join Oyo_Units on Oyo_Unit_Details.UnitId=Oyo_Units.UnitId where PremiseId='%s'" %(pid)
              cursor.execute(sqlq2)
              get_units_rec = cursor.fetchall()
              #print get_units_rec
              if(len(get_units_rec) > 0):
             
                 output = '{"function":"premise_units","session_id":"%s","error_code":"0", "Response":"Successfully got %d units", \n "get_units":' %(sid,len(get_units_rec))
                 output += '['
                 counter = 0
                 #%(rec[0] ,pid,rec[1],rec[2],rec[3],rec[4],rec[5],rec[6],rec[7],rec[8],rec[9],rec[10],rec[11],rec[12],rec[13],rec[14],rec[15],rec[16]
                 #for row in get_units_rec:
                 for rec in get_units_rec:
# UnitId | IpMacid      | IpName    | OpMacid      | OpName    | PMacId       | PName      | Fan  | Status | Temp | setTemp | Voltage | Power | Current | Voltage | Power | Current | UnitId | UnitId |
   	
                     '''
                     unitid=''
		     if row[12] is not None :
                        unitid=row[12]
                     if row[30] is not None:
                        unitid=row[30]
                     if row[44] is not None:
                        unitid=row[44]
                     '''
                     unitid=''
                     if rec[0] is not None :
                        unitid=rec[0]
                     if rec[17] is not None:
                        unitid=rec[17]
                     if rec[18] is not None:
                        unitid=rec[18]
                     counter += 1
                     if(counter == 1):
                        #output += '{"Unit_id":"%s","premise_id":"%s","ipmacid":"%s","ipname":"%s","opmacid":"%s","opname":"%s","pmacid":"%s","pname":"%s","fan":"%s","status":"%s","temp":"%s","settemp":"%s","voltage":"%s","power":"%s","current":"%s","Evoltage":"%s","Epower":"%s","Ecurrent":"%s"}' %(unitid,pid,row[1],row[2],row[14],row[15],row[32],row[33],row[17],row[18],row[4],row[19],row[21],row[22],row[20],row[36],row[37],row[35]) 
                        #UnitId | IpMacid      | IpName  | OpMacid      | OpName | PMacId | PName | Fan  | Status | Temp | setTemp | Voltage | Power | Current | Voltage | Power | Current |
                        

                        output += '{"Unit_id":"%s","premise_id":"%s","ipmacid":"%s","ipname":"%s","opmacid":"%s","opname":"%s","pmacid":"%s","pname":"%s","fan":"%s","status":"%s","temp":"%s","settemp":"%s","voltage":"%s","power":"%s","current":"%s","Evoltage":"%s","Epower":"%s","Ecurrent":"%s"}' %(unitid ,pid,rec[1],rec[2],rec[3],rec[4],rec[5],rec[6],rec[7],rec[8],rec[9],rec[10],rec[11],rec[12],rec[13],rec[14],rec[15],rec[16])
                     else:
                        #output += ',\n {"Unit_id":"%s","premise_id":"%s","ipmacid":"%s","ipname":"%s","opmacid":"%s","opname":"%s","pmacid":"%s","pname":"%s","fan":"%s","status":"%s","temp":"%s","settemp":"%s","voltage":"%s","power":"%s","current":"%s","Evoltage":"%s","Epower":"%s","Ecurrent":"%s"}' %(unitid,pid,row[1],row[2],row[14],row[15],row[32],row[33],row[17],row[18],row[4],row[19],row[21],row[22],row[20],row[36],row[37],row[35])
                        output += ',\n {"Unit_id":"%s","premise_id":"%s","ipmacid":"%s","ipname":"%s","opmacid":"%s","opname":"%s","pmacid":"%s","pname":"%s","fan":"%s","status":"%s","temp":"%s","settemp":"%s","voltage":"%s","power":"%s","current":"%s","Evoltage":"%s","Epower":"%s","Ecurrent":"%s"}' %(unitid ,pid,rec[1],rec[2],rec[3],rec[4],rec[5],rec[6],rec[7],rec[8],rec[9],rec[10],rec[11],rec[12],rec[13],rec[14],rec[15],rec[16])
                 output += ']\n'
                 output += '}'
                 return mqttc.publish('jts/oyo/error',output)

              else:
                 output = '{"function":"get_units","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the unit records, NO_DATA_FOUND"}' %(sid)
                 return mqttc.publish('jts/oyo/error',output)

           elif history == "get_units":
            
              print "in units"
              '''
              sqlq2 = "select AC_Units.UnitId,AC_Units.IpmacId,AC_Units.OpmacId,AC_Rooms.RoomDesc,AC_Floors.FloorDesc,AC_Premise.PremiseDesc,AC_City.CityDesc,AC_Units.ChangeDate from AC_Units,AC_City,AC_Premise,AC_Floors,AC_Rooms where AC_City.CityId=AC_Units.CityId and AC_Premise.PremiseId=AC_Units.PremiseId and AC_Floors.FloorId=AC_Units.FloorId and AC_Rooms.RoomId=AC_Units.RoomId"
              cursor.execute(sqlq2)
              get_units_rec = cursor.fetchall()
              if(len(get_units_rec) > 0):
                 output = '{"function":"get_units","session_id":"%s","error_code":"0", "Response":"Successfully got %d units", \n "get_units":' %(sid,len(get_units_rec))
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
                 output = '{"function":"get_units","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the unit records, NO_DATA_FOUND"}' %(sid)
                 return mqttc.publish('jts/oyo/error',output)
              '''
           else:
              pass
              #print "nothing satisfied"
           #print "end...."
       
       if history_all == True:
          
          #sqlq = "select count(DISTINCT UnitId)  from Oyo_Unit_History where Temp between 23 and 25 GROUP BY CityId"
          sqlq = "select count(*)as count  from AC_Input_Units where Temp between 23 and 25 and ChangeBy='%s' GROUP BY CityId" %(username)
          cursor.execute(sqlq)
          good = cursor.fetchall()
          #print "response fro cld good : ",good,len(good)
          #sqlq1 = "select count(DISTINCT UnitId) from Oyo_Unit_History where Temp between 25 and 27 GROUP BY CityId"
          sqlq1="select count(*)as count  from AC_Input_Units where Temp between 25 and 27 and ChangeBy='%s' GROUP BY CityId" %(username)
          cursor.execute(sqlq1)
          warning = cursor.fetchall()
          #print "response fro cld warn : ",warning,len(warning)
          #sqlq2 = "select count(DISTINCT UnitId) from Oyo_Unit_History where Temp between 20 and 22 GROUP BY CityId"
          sqlq2="select count(*)as count  from AC_Input_Units where Temp between 20 and 22 and ChangeBy='%s' GROUP BY CityId" %(username)
          cursor.execute(sqlq2)
          problem = cursor.fetchall()
          #print "response fro cld problem : ",problem,len(problem)
          #sqlq = "select count(DISTINCT UnitId) from Oyo_Unit_History where Temp=50 GROUP BY CityId"
          sqlq3="select count(*)as count from AC_Input_Units where Temp=50 and ChangeBy='%s' GROUP BY CityId" %(username) 
          cursor.execute(sqlq3)
          main = cursor.fetchall()
          #print "response fro cld maintenance : ",main,len(main)
          
          sqlq3 = "select round(Avg(AC_Input_History.Temp))as temp from AC_Input_History,AC_Input_Units where AC_Input_History.IpId=AC_Input_Units.IpId and AC_Input_Units.ChangeBy='%s' and DATE_ADD(AC_Input_History.ChangeDate, INTERVAL 1 MINUTE) >= NOW() group by AC_Input_Units.CityId" %(username)
          cursor.execute(sqlq3)
          getcitytemp = cursor.fetchall()
          #print "Avg tem from cities : ",getcitytemp,len(getcitytemp)
          
          sqlq4="select CityId,CityDesc from AC_City where ChangeBy='%s' " %(username)
          #sqlq4="select AC_Input_Units.CityId,AC_City.CityDesc, round(avg(AC_Input_History.Temp)) as avgtemp from AC_Input_History,AC_Input_Units,AC_City where AC_Input_Units.ChangeBy='%s' GROUP BY AC_Input_Units.CityId " %(username)
          #sqlq2= "select CityId,round(avg(Temp)) as avgtemp from Oyo_Unit_History GROUP BY CityId"
          #sqlq2 = "select Oyo_City.CityId,Oyo_City.CityDesc,round(avg(Temp)) as avgtemp from Oyo_Unit_History,Oyo_City GROUP BY CityId"
          #sqlq2 = "select Oyo_City.CityId,Oyo_City.CityDesc,round(avg(Temp)) as avgtemp,(select count(Temp) from Oyo_Unit_History where Temp between 23 and 25) as good,(select count(Temp) from Oyo_Unit_History where Temp between 25 and 27) as warning,(select count(Temp) from Oyo_Unit_History where Temp between 27 and 21) as problem,(select count(Temp) from Oyo_Unit_History where Temp=99) as maintenance from Oyo_Unit_History,Oyo_City where Oyo_Unit_History.CityId=Oyo_City.CityId  GROUP BY CityId"
          #print sqlq2
          cursor.execute(sqlq4)
          get_units_rec = cursor.fetchall()
            
          #print get_units_rec,len(get_units_rec)
          
          if(len(get_units_rec) > 0):
             #desc = cursor.description
	     output = '{"function":"all_cities","session_id":"%s","error_code":"0", "Response":"Successfully got %d units", \n "get_units":' %(sid,len(get_units_rec))
             output += '['
             counter = 0
     
             goodval = "0"
             warningval = "0"
             problemval = "0"
             mainval = "0"
             citytemp="0"
             for i,rec in enumerate(get_units_rec):
                 #print "i : ",i
                 
                 if i < len(good):
                    #print "ggod if "
                    goodval=good[i][0]
                    #print "ggod if ",goodval
                 else:
                    #print "ggod else"
                    goodval="0"
                    #print "ggod else ",goodval
		 	                 
                 if i < len(warning):
                    #print "warn if"
                    warningval=warning[i][0]
                    #print "warn if ",warningval
                 else:
                    #print "wanr else"
                    warningval="0"
                    #print "warn else ",warningval
                 if i < len(problem):
                    #print "prob if"
                    problemval=problem[i][0]
                    #print "prob if",problemval
                 else:
                    #print "prob else"
                    problemval="0"
                    #print "prob else",problemval
     		 if i < len(main):
                    #print "main if"
                    mainval=main[i][0]
                    #print "main if",mainval
                 else:
                    #print "main else"
                    mainval="0"
                    #print "main else",mainval
                 if i < len(getcitytemp):
                    #print "main if"
                    citytemp=getcitytemp[i][0]
                    #print "main if",mainval
                 else:
                    #print "main else"
                    citytemp="0"
                 #print goodval,warningval,problemval,mainval,getcity[i][0]        
                 #print warningval
                 #print problemval
                 #print mainval
                 #print getcity[i][0] 
                 #cityDesc=getcity[i][0]
                 
                 counter += 1
                 if(counter == 1):
                    
                    output += '{"city_id":"%s","CityDesc":"%s","avgtemp":"%s","good":"%s","warning":"%s","problem":"%s","maintenance":"%s"}' %(rec[0] ,rec[1],citytemp,goodval,warningval,problemval,mainval)
                 else:
                    
                    output += ',\n {"city_id":"%s","CityDesc":"%s","avgtemp":"%s","good":"%s","warning":"%s","problem":"%s","maintenance":"%s"}' %(rec[0] ,rec[1],citytemp,goodval,warningval,problemval,mainval)
                 #print output
             output += ']\n'
             output += '}'
             #print output
             return mqttc.publish('jts/oyo/error',output) 
          else:
             output = '{"function":"get_units","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the unit records, NO_DATA_FOUND"}' %(sid)
             return mqttc.publish('jts/oyo/error',output)


       if history_city == True:
          #print "in city premise"
          if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
             output_str += ", city_id is mandatory"
             output = '{"function":"get_units","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
             return mqttc.publish('jts/oyo/error',output)
          else:
             cid = data1['city_id']
             sqlq="select count(*)as count  from AC_Input_Units where Temp between 23 and 25 and CityId='%s' and ChangeBy='%s' GROUP BY PremiseId" %(cid,username)
             #sqlq="select count(*)as count from AC_Input_History,AC_Input_Units where AC_Input_History.Temp between 23 and 25 and AC_Input_Units.CityId='%s' and AC_Input_Units.ChangeBy='%s' GROUP BY AC_Input_Units.IpId" %(cid,username)
             #sqlq = "select count(DISTINCT UnitId) from Oyo_Unit_History where Temp between 23 and 25 and CityId='%s' GROUP BY PremiseId" %(cid)
             cursor.execute(sqlq)
             good = cursor.fetchall()
             #print "response fro cld good : ",good,len(good)
             sqlq1="select count(*)as count  from AC_Input_Units where Temp between 25 and 27 and CityId='%s' and ChangeBy='%s' GROUP BY PremiseId" %(cid,username)
             #sqlq1="select count(*)as count from AC_Input_History,AC_Input_Units where AC_Input_History.Temp between 25 and 27 and AC_Input_Units.CityId='%s' and AC_Input_Units.ChangeBy='%s' GROUP BY AC_Input_Units.IpId" %(cid,username)

             #sqlq1 = "select count(DISTINCT UnitId) from Oyo_Unit_History where Temp between 25 and 27 and CityId='%s' GROUP BY PremiseId" %(cid)
             cursor.execute(sqlq1)
             warning = cursor.fetchall()
             #print "response fro cld warn : ",warning,len(warning)
             sqlq2="select count(*)as count  from AC_Input_Units where Temp between 20 and 22 and CityId='%s' and ChangeBy='%s' GROUP BY PremiseId" %(cid,username)
             #sqlq2="select count(*)as count from AC_Input_History,AC_Input_Units where AC_Input_History.Temp between 20 and 22 and AC_Input_Units.CityId='%s' and AC_Input_Units.ChangeBy='%s' GROUP BY AC_Input_Units.IpId" %(cid,username)
             #sqlq2 = "select count(DISTINCT UnitId) from Oyo_Unit_History where Temp between 20 and 22 and CityId='%s' GROUP BY PremiseId" %(cid)
             cursor.execute(sqlq2)
             problem = cursor.fetchall()
             #print "response fro cld problem : ",problem,len(problem)
             sqlq3="select count(*)as count  from AC_Input_Units where Temp=50 and CityId='%s' and ChangeBy='%s' GROUP BY PremiseId" %(cid,username)
             #sqlq3="select count(*)as count from AC_Input_History,AC_Input_Units where AC_Input_History.Temp=50 and AC_Input_Units.CityId='%s' and AC_Input_Units.ChangeBy='%s' GROUP BY AC_Input_Units.UnitId" %(cid,username)
             #sqlq = "select count(DISTINCT UnitId) from Oyo_Unit_History where Temp=50 and CityId='%s' GROUP BY PremiseId" %(cid)
             cursor.execute(sqlq3)
             main = cursor.fetchall()
             #print "response fro cld maintenance : ",main,len(main)
             
             sqlq3 = "select round(Avg(AC_Input_History.Temp))as temp from AC_Input_History,AC_Input_Units where AC_Input_History.IpId=AC_Input_Units.IpId and AC_Input_Units.CityId='%s' and AC_Input_Units.ChangeBy='%s' and DATE_ADD(AC_Input_History.ChangeDate, INTERVAL 1 MINUTE) >= NOW() group by AC_Input_Units.PremiseId" %(cid,username)
             cursor.execute(sqlq3)
             gettemppremise = cursor.fetchall()
             #print "avg temp : ",gettemppremise,len(gettemppremise)
             
             sqlq2="select PremiseId,PremiseDesc from AC_Premise where CityId='%s' and ChangeBy='%s'" %(cid,username)
             #sqlq2= "select PremiseId,round(avg(Temp)) as avgtemp from Oyo_Unit_History WHERE CityId='%s' GROUP BY PremiseId" %(cid)
             #sqlq2 = "select Oyo_Premise.PremiseId,Oyo_Premise.PremiseDesc,round(avg(Temp)) as avgtemp,(select count(Temp) from Oyo_Unit_History where Temp between 23 and 25) as good,(select count(Temp) from Oyo_Unit_History where Temp between 25 and 27) as warning,(select count(Temp) from Oyo_Unit_History where Temp between 27 and 21) as problem,(select count(Temp) from Oyo_Unit_History where Temp=99) as maintenance from Oyo_Unit_History,Oyo_Premise where Oyo_Unit_History.CityId='%s' and Oyo_Premise.PremiseId=Oyo_Unit_History.PremiseId GROUP BY PremiseId" %(cid)
             #print sqlq2
             cursor.execute(sqlq2)
             get_units_rec = cursor.fetchall()
             
             #print get_units_rec
             if(len(get_units_rec) > 0):
                output = '{"function":"city_premise","session_id":"%s","error_code":"0", "Response":"Successfully got %d units", \n "get_units":' %(sid,len(get_units_rec))
                output += '['
                counter = 0
                goodval = "0"
                warningval = "0"
                problemval = "0"
                mainval = "0"
                pretemp="0"
                for i,rec in enumerate(get_units_rec):
                    
 	            if i < len(good):
                       #print "ggod if "
                       goodval=good[i][0]
                       #print "ggod if ",goodval
                    else:
                       #print "ggod else"
                       goodval="0"
                       #print "ggod else ",goodval

                    if i < len(warning):
                       #print "warn if"
                       warningval=warning[i][0]
                       #print "warn if ",warningval
                    else:
                       #print "wanr else"
                       warningval="0"
                       #print "warn else ",warningval
                    if i < len(problem):
                       #print "prob if"
                       problemval=problem[i][0]
                       #print "prob if",problemval
                    else:
                       #print "prob else"
                       problemval="0"
                       #print "prob else",problemval
                    if i < len(main):
                       #print "main if"
                       mainval=main[i][0]
                       #print "main if",mainval
                    else:
                       #print "main else"
                       mainval="0"
                    if i < len(gettemppremise):
                       #print "main if"
                       pretemp=gettemppremise[i][0]
                       #print "main if",mainval
                    else:
                       #print "main else"
                       pretemp="0"


   		    #print warningval
                    #print problemval
                    #print mainval
                    #print getpremise[i][0]
                    #preDesc=getpremise[i][0]
                    
                    counter += 1
                    if(counter == 1):
                       output += '{"city_id":"%s","premise_id":"%s","premise_desc":"%s","avgtemp":"%s","good":"%s","warning":"%s","problem":"%s","maintenance":"%s"}' %(cid,rec[0] ,rec[1],pretemp,goodval,warningval,problemval,mainval)
                    else:
                       output += ',\n {"city_id":"%s","premise_id":"%s","premise_desc":"%s","avgtemp":"%s","good":"%s","warning":"%s","problem":"%s","maintenance":"%s"}' %(cid,rec[0] ,rec[1],pretemp,goodval,warningval,problemval,mainval)
                output += ']\n'
                output += '}'
                #print output
                return mqttc.publish('jts/oyo/error',output)
             else:
                output = '{"function":"get_units","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the unit records, NO_DATA_FOUND"}' %(sid)
                return mqttc.publish('jts/oyo/error',output)
   
              
       '''
       sqlq2 = "SELECT UnitId,CityId,FloorId,RoomId,IpmacId,OpmacId,Temp,ChangeDate FROM Oyo_Units where PremiseId='"+premise+"'"
       cursor.execute(sqlq2)
       get_units_rec = cursor.fetchall()
       if(len(get_units_rec) > 0):
        #{
           output = '{"function":"get_units","session_id":"%s","error_code":"0", "Response":"Successfully got %d units", \n "get_units":' %len(get_units_rec)
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
           output = '{"session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the unit records, NO_DATA_FOUND"}'
           return mqttc.publish('jts/oyo/error',output)   
       '''
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_units","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the units"}' %(sid)
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()
###################### get rooms #################################
def get_rooms(mosq,obj,msg):
    #print "get_rooms......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
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

    if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"get_rooms","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

    sid = data1['session_id']

    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"function":"get_rooms","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('password') is None):
       output_str += ",passsword is mandatory"
       output = '{"function":"get_rooms","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
       return mqttc.publish('jts/oyo/error',output)
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
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to get rooms"
       output = '{"function":"get_rooms","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
       return mqttc.publish('jts/oyo/error',output)
    try:
       if((data1.get('floor_id') is None) or ((data1.get('floor_id') is not  None) and (len(data1['floor_id']) <= 0))):
           output_str += ", floor_id is mandatory"
           output = '{"function":"get_rooms","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           fid = data1['floor_id']

       sqlq2 = "SELECT RoomId,RoomDesc from AC_Rooms WHERE FloorId='%s' and ChangeBy='%s'" %(fid,username)
       cursor.execute(sqlq2)
       get_rooms_rec = cursor.fetchall()
       if(len(get_rooms_rec) > 0):
        #{
           output = '{"function":"get_rooms","session_id":"%s","error_code":"0", "Response":"Successfully got %d rooms", \n "get_rooms":' %(sid,len(get_rooms_rec))
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
           output = '{"function":"get_rooms","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the rooms records, NO_DATA_FOUND"}' %(sid)
           return mqttc.publish('jts/oyo/error',output) 
          
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_rooms","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the rooms"}' %(sid)
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()
####################### get floors ##########################
def get_floors(mosq,obj,msg):
    #print "get_floors......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
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
        output = '{"function":"get_floors","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"get_floors","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

    sid = data1['session_id']

    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"function":"get_floors","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('password') is None):
        output_str += ",passsword is mandatory"
        output = '{"function":"get_floors","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/oyo/error',output)
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
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to get floors"
       output = '{"function":"get_floors","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
       return mqttc.publish('jts/oyo/error',output)
    try:
       if((data1.get('premise_id') is None) or ((data1.get('premise_id') is not  None) and (len(data1['premise_id']) <= 0))):
           output_str += ", premise_id is mandatory"
           output = '{"function":"get_floors","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           pid = data1['premise_id']      
       sqlq2 = "SELECT FloorId,FloorDesc FROM AC_Floors WHERE PremiseId='%s' and ChangeBy='%s'" %(pid,username)
       cursor.execute(sqlq2)
       get_floors_rec = cursor.fetchall()
       if(len(get_floors_rec) > 0): 
        #{
           output = '{"function":"get_floors","session_id":"%s","error_code":"0", "Response":"Successfully got %d floors", \n "get_floors":' %(sid,len(get_floors_rec))
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
           output = '{"function":"get_floors","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the floors records, NO_DATA_FOUND"}' %(sid)
           return mqttc.publish('jts/oyo/error',output)
            
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_floors","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the floors"}' %(sid)
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close() 
'''
############## get units details ################################
def get_units_details(mosq,obj,msg):
    #print "get_unit_details......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
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

    if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"get_units_details","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

    sid = data1['session_id']

    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"function":"get_units_details","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('password') is None):
        output_str += ",passsword is mandatory"
        output = '{"function":"get_units_details","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/oyo/error',output)

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
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to get unit details"
       output = '{"function":"get_units_details","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
       return mqttc.publish('jts/oyo/error',output)
    try:
       if((data1.get('UnitId') is None) or ((data1.get('UnitId') is not  None) and (len(data1['UnitId']) <= 0))):
           output_str += ", UnitId is mandatory"
           output = '{"function":"get_units_details","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           unitid = data1['UnitId'] 
       sqlq2 = "SELECT AC_Operate_Units.Fan,AC_Operate_Units.Status,AC_Input_Units.Temp,AC_Operate_Units.SetTemp,AC_Operate_Units.Runtime,AC_Operate_Units.Strength FROM AC_Operate_Units,AC_Input_Units WHERE AC_Operate_Units.UnitId='%s' and AC_Operate_Units.ChangeBy=AC_Input_Units.ChangeBy='%s'" %(unitid,username)
       cursor.execute(sqlq2)
       get_units_rec = cursor.fetchall()
       if(len(get_units_rec) > 0):
        #{
           output = '{"function":"get_unit_details","session_id":"%s","error_code":"0", "Response":"Successfully got %d units", \n "get_units_details":' %(sid,len(get_units_rec))
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
           output = '{"function":"get_units_details","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the unit records, NO_DATA_FOUND"}' %(sid)
           return mqttc.publish('jts/oyo/error',output)      

    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_units_details","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the units"}' %(sid)
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()
'''
################### get cities #############################################
def get_city(mosq,obj,msg):
    #print "get_cities......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
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

    if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"get_city","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

    sid = data1['session_id']


    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"function":"get_city","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('password') is None):
        output_str += ",passsword is mandatory"
        output = '{"function":"get_city","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/oyo/error',output)
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
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to get cities"
       output = '{"function":"get_city","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
       return mqttc.publish('jts/oyo/error',output)
    try:
       sqlq2 = "SELECT CityId,CityDesc FROM AC_City WHERE ChangeBy='%s'" %(username)
       cursor.execute(sqlq2)
       get_cities_rec = cursor.fetchall()
       if(len(get_cities_rec) > 0): 
        #{
           output = '{"function":"get_cities","session_id":"%s","error_code":"0", "Response":"Successfully got %d cities", \n "get_cities":' %(sid,len(get_cities_rec))
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
           output = '{"function":"get_city","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the city records, NO_DATA_FOUND"}' %(sid)
           return mqttc.publish('jts/oyo/error',output)
            
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_city","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the cities"}' %(sid)
        return mqttc.publish('jts/oyo/error',output)
    finally:
        username=''
        password=''
        cursor.close()
        db.close() 
######################## add cities ###############################################
def add_city(mosq,obj,msg):
    #print "add_city......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
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

       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"add_city","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']


       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"add_city","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"add_city","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
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
          return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('city_name') is None) or ((data1.get('city_name') is not  None) and (len(data1['city_name']) <= 0))):
           output_str += ", city_name is mandatory"
           output = '{"function":"add_city","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
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
          return mqttc.publish('jts/oyo/error',output)
          
       else:
          #print "exitig........"
          #add_rec=cursor.execute("""INSERT INTO DtD_Units(UnitDesc,Longitude,Latitude,Operation,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s)""",(name,float(long1),float(lat),'OFF',username,date))
          add_rec=cursor.execute("""INSERT INTO AC_City(CityDesc,ChangeBy,ChangeDate) VALUES (%s,%s,%s)""",(cname,username,date))
          db.commit()
          if add_rec > 0:
             #print "inserted"
             output = '{"function":"add_city","session_id":"%s","error_code":"0", "Response":"Successfully added the city : %s"}'%(sid,cname)
             return mqttc.publish('jts/oyo/error',output)
             
          else:
             #print "insert filed"
             output = '{"function":"add_city","session_id":"%s","error_code":"-2", "error_desc": "Response= Unable to add city"}'%(sid)
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
        output = '{"function":"add_city","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to add the city"}' %(sid)
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()
'''
#################### delete city ##############################################
def delete_city(mosq,obj,msg):
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
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
       return mqttc.publish('jts/oyo/error','{"session_id":"%s","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           password = data1['password']

       sqlq = "SELECT * FROM AC_Users WHERE Username='"+username+"' AND Password='"+password+"'"
       cursor.execute(sqlq)
       results = cursor.fetchone()
       if results > 0:
          print 'Login Data Existed'
          pass
       else:
          print 'Login Data not authorized so quiting ........Thanks'
          output_str += ",Your not Autherize to add unit"
          output = '{"session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
           output_str += ", city_id is mandatory"
           output = '{"session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           cid = data1['city_id']

       sqlq12= "DELETE FROM AC_City WHERE CityId="+cid
       print sqlq12
       del_rec=cursor.execute(sqlq12)
       db.commit()
       print del_rec
       if del_rec > 0:
          output = '{"session_id":"%s","error_code":"0", "Response":"Successfully deleted the city : %s"}'%cid
          return mqttc.publish('jts/oyo/error',output)
       else:
          output = '{"session_id":"%s","error_code":"-2", "error_desc": "Response= Unable to delete the city"}'
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
        output = '{"session_id":"%s","error_code":"3", "error_desc": "Response=Failed to delete the city"}'
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()
'''
############### update city  #######################################
def update_city(mosq,obj,msg):
    #print "update......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
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

       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"update_city","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']


       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"update_city","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"update_city","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
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
          return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
           output_str += ", city_id is mandatory"
           output = '{"function":"update_city","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           cid = data1['city_id']

       if((data1.get('city_name') is None) or ((data1.get('city_name') is not  None) and (len(data1['city_name']) <= 0))):
           output_str += ", city_name is mandatory"
           output = '{"function":"update_city","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           cname = data1['city_name']
       
       #sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"' AND Longitude='"+long1+"' AND Latitude='"+lat+"'"
       upd_rec=cursor.execute ("""UPDATE AC_City SET CityDesc=%s,ChangeBy=%s,ChangeDate=%s WHERE CityId=%s""", (cname,username,date,cid))
       db.commit() 
       
       if upd_rec > 0:
          output = '{"function":"update_city","session_id":"%s","error_code":"0", "Response":"Successfully updated the city : %s"}'%(sid,cid)
          return mqttc.publish('jts/oyo/error',output)
       else:
          output_str = "City not existed"
          output = '{"function":"update_city","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          return mqttc.publish('jts/oyo/error',output) 

          
          
    except MySQLdb.Error, e:
        try:
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish('jts/oyo/error',e.args[0], e.args[1])
        except IndexError:
            #print "MySQL Error: %s" % str(e)
            return mqttc.publish('jts/oyo/error',str(e))
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"update_city","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to update the city"}' %(sid)
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()

######################## add premise ###############################################
def add_premise(mosq,obj,msg):
    #print "add premise......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
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

       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"add_premise","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']

        
       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"add_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"add_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
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
          return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('premise_name') is None) or ((data1.get('premise_name') is not  None) and (len(data1['premise_name']) <= 0))):
           output_str += ", premise_name is mandatory"
           output = '{"function":"add_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           pname = data1['premise_name']

       if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
           output_str += ", city_id is mandatory"
           output = '{"function":"add_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
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
          return mqttc.publish('jts/oyo/error',output)  
       else:
          #print "exitig........"
          add_rec=cursor.execute("""INSERT INTO AC_Premise(CityId,PremiseDesc,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s)""",(cid,pname,username,date))
          db.commit()
          if add_rec > 0:
             #print "inserted"
             output = '{"function":"add_premise","session_id":"%s","error_code":"0", "Response":"Successfully added the premise : %s"}'%(sid,pname)
             return mqttc.publish('jts/oyo/error',output)
          else:
             output_str = "The premise not mapping to city"
             output = '{"function":"add_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
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
        output = '{"function":"add_premise","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to add the premise"}' %(sid)
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()
'''
#################### delete premise ##############################################
def delete_premise(mosq,obj,msg):
    print "delete premise......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
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
       return mqttc.publish('jts/oyo/error','{"function":"add_premise","session_id":"%s","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:

       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"add_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"add_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
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
          output = '{"function":"add_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('premise_id') is None) or ((data1.get('premise_id') is not  None) and (len(data1['premise_id']) <= 0))):
           output_str += ", premise_id is mandatory"
           output = '{"function":"add_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
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
          output = '{"session_id":"%s","error_code":"0", "Response":"Successfully deleted the premise"}'
          return mqttc.publish('jts/oyo/error',output)
       else:
          output = '{"session_id":"%s","error_code":"-2", "error_desc": "Response= Unable to delete the premise"}'
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
        output = '{"session_id":"%s","error_code":"3", "error_desc": "Response=Failed to delete the premise"}'
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()
'''
############### update premise  #######################################
def update_premise(mosq,obj,msg):
    #print "update premise......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
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


       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"update_premise","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']


       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"update_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"update_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
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
          return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('premise_id') is None) or ((data1.get('premise_id') is not  None) and (len(data1['premise_id']) <= 0))):
           output_str += ", premise_id is mandatory"
           output = '{"function":"update_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           pid = data1['premise_id']

       if((data1.get('premise_name') is None) or ((data1.get('premise_name') is not  None) and (len(data1['premise_name']) <= 0))):
           output_str += ", premise_name is mandatory"
           output = '{"function":"update_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           pname = data1['premise_name']
      
       #sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"' AND Longitude='"+long1+"' AND Latitude='"+lat+"'"
       upd_rec=cursor.execute ("""UPDATE AC_Premise SET PremiseDesc=%s,ChangeBy=%s,ChangeDate=%s WHERE PremiseId=%s""", (pname,username,date,pid))
       db.commit() 
       
       if upd_rec > 0:
          output = '{"function":"update_premise","session_id":"%s","error_code":"0", "Response":"Successfully updated the premise : %s"}'%(sid,pid)
          return mqttc.publish('jts/oyo/error',output)
       else:
          output_str = "premise not existed"
          output = '{"function":"update_premise","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
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
        output = '{"function":"update_premise","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to update the premise"}' %(sid)
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()

###################### optional testing ################################
def test01(mosq,obj,msg):
    print "test01......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
    cursor = db.cursor()
    output_str = "get_units_details - Unable to Authenticate/get_units_details... "
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    sqlq = "select (@row_number:=@row_number + 1) AS num ,Time(ChangeDate) from AC_Operate_History,(SELECT @row_number:=0) AS t where MotionE=1 and ChangeDate>Curdate() and OpId=19"
    cursor.execute(sqlq)
    results = cursor.fetchall()    
    print len(results)
    
    for row in range(len(results)):
        print row
        print "row='%s',date='%s'" %(results[row][0],results[row][1])
################## add floor #####################################
def add_floor(mosq,obj,msg):
    #print "add_floor......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
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

       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"add_floor","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']


       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"add_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"add_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
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
          return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('floor_name') is None) or ((data1.get('floor_name') is not  None) and (len(data1['floor_name']) <= 0))):
           output_str += ", floor_name is mandatory"
           output = '{"function":"add_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           fname = data1['floor_name']

       if((data1.get('premise_id') is None) or ((data1.get('premise_id') is not  None) and (len(data1['premise_id']) <= 0))):
           output_str += ", premise_id is mandatory"
           output = '{"function":"add_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           pid = data1['premise_id']

       if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
           output_str += ", city_id is mandatory"
           output = '{"function":"add_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
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
          return mqttc.publish('jts/oyo/error',output)

          
       else:
          #print "inserted"
          add_rec1=cursor.execute("""INSERT INTO AC_Floors(CityId,PremiseId,FloorDesc,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s)""",(cid,pid,fname,username,date))
          db.commit()
          if add_rec1 > 0:
             output = '{"function":"add_floor","session_id":"%s","error_code":"0", "Response":"Successfully added the floor : %s"}'%(sid,fname)
             return mqttc.publish('jts/oyo/error',output)
          else:
             output_str = "The floor not mapping to premise"
             output = '{"function":"add_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
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
        output = '{"function":"add_floor","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to add the floor"}'
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()

######################### update floor ####################################
def update_floor(mosq,obj,msg):
    #print "update_floor......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
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

       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"update_floor","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']


       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"update_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"update_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
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
          return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('floor_id') is None) or ((data1.get('floor_id') is not  None) and (len(data1['floor_id']) <= 0))):
           output_str += ", floor_id is mandatory"
           output = '{"function":"update_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           fid = data1['floor_id']

       if((data1.get('floor_name') is None) or ((data1.get('floor_name') is not  None) and (len(data1['floor_name']) <= 0))):
           output_str += ", floor_name is mandatory"
           output = '{"function":"update_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           fname = data1['floor_name']
      
       #sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"' AND Longitude='"+long1+"' AND Latitude='"+lat+"'"
       upd_rec=cursor.execute ("""UPDATE AC_Floors SET FloorDesc=%s,ChangeBy=%s,ChangeDate=%s WHERE FloorId=%s""", (fname,username,date,fid))
       db.commit() 
       
       if upd_rec > 0:
          output = '{"function":"update_floor","session_id":"%s","error_code":"0", "Response":"Successfully updated the floor "}' %(sid)
          return mqttc.publish('jts/oyo/error',output)
       else:
          output_str = "premise not existed"
          output = '{"function":"update_floor","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
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
        output = '{"function":"update_floor","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to update the floor"}' %(sid)
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()
################### add room ####################################################
def add_room(mosq,obj,msg):
    #print "add_rooom......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
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

       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"add_room","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']


       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"add_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"add_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
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
          return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('room_name') is None) or ((data1.get('room_name') is not  None) and (len(data1['room_name']) <= 0))):
           output_str += ", room_name is mandatory"
           output = '{"function":"add_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           rname = data1['room_name']

       if((data1.get('floor_id') is None) or ((data1.get('floor_id') is not  None) and (len(data1['floor_id']) <= 0))):
           output_str += ", floor_id is mandatory"
           output = '{"function":"add_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           fid = data1['floor_id']

       if((data1.get('premise_id') is None) or ((data1.get('premise_id') is not  None) and (len(data1['premise_id']) <= 0))):
           output_str += ", premise_id is mandatory"
           output = '{"function":"add_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           pid = data1['premise_id']

       if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
           output_str += ", city_id is mandatory"
           output = '{"function":"add_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
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
          return mqttc.publish('jts/oyo/error',output)
          
       else:
          #print "exitig........"
          add_rec=cursor.execute("""INSERT INTO AC_Rooms(CityId,PremiseId,RoomDesc,FloorId,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s)""",(cid,pid,rname,fid,username,date))
          db.commit()
          if add_rec > 0:
             #print "inserted"
             output = '{"function":"add_room","session_id":"%s","error_code":"0", "Response":"Successfully added the room: %s"}'%(sid,rname)
             return mqttc.publish('jts/oyo/error',output)
          else:
             output_str = "The room not mapping to floor"
             output = '{"session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
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
        output = '{"function":"add_room","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to add the room"}' %(sid)
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()

######################### update room ####################################
def update_room(mosq,obj,msg):
    #print "update_room......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
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

       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"update_room","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']


       if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
           output_str += ", username is mandatory"
           output = '{"function":"update_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           username = data1['username']

       if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
           output_str += ", password is mandatory"
           output = '{"function":"update_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
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
          return mqttc.publish('jts/oyo/error',output)      
      
       if((data1.get('room_id') is None) or ((data1.get('room_id') is not  None) and (len(data1['room_id']) <= 0))):
           output_str += ", room_id is mandatory"
           output = '{"function":"update_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           rid = data1['room_id']

       if((data1.get('room_name') is None) or ((data1.get('room_name') is not  None) and (len(data1['room_name']) <= 0))):
           output_str += ", room_name is mandatory"
           output = '{"function":"update_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           rname = data1['room_name']
      
       #sqlq1 = "SELECT * FROM DtD_Units WHERE UnitDesc='"+name+"' AND Longitude='"+long1+"' AND Latitude='"+lat+"'"
       upd_rec=cursor.execute ("""UPDATE AC_Rooms SET RoomDesc=%s,ChangeBy=%s,ChangeDate=%s WHERE RoomId=%s""", (rname,username,date,rid))
       db.commit() 
       
       if upd_rec > 0:
          output = '{"function":"update_room","session_id":"%s","error_code":"0", "Response":"Successfully updated the room "}' %(sid)
          return mqttc.publish('jts/oyo/error',output)
       else:
          output_str = "room not existed"
          output = '{"function":"update_room","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
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
        output = '{"function":"update_room","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to update the room"}' %(sid)
        return mqttc.publish('jts/oyo/error',output)
    finally:
        cursor.close()
        db.close()

###################### delete unit ##########################################
def delete_unit(mosq,obj,msg):

       #print "delete unit......."

       try:
          data1 = json.loads(msg.payload)
       #print data1
       except ValueError:
          return mqttc.publish('jts/oyo/error','{"function":"delete_unit","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')

       if(data1.get('session_id') is None):
          output_str = "session_id is mandatory"
          output = '{"function":"delete_unit","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']


       
       req2 = requests.post(url = 'http://localhost:5904/GenericACApp/delete_unit/', data = msg.payload,verify=False)
       res = req2.text
       res = json.loads(res)
       res["session_id"]="%s" %(sid)
       res =json.dumps(res)
       #print "response fro cld: ",res
       return mqttc.publish('jts/oyo/error',str(res))
       
###################### delete unit type##########################################
def delete_unit_utype(mosq,obj,msg):

       #print "delete unit......."

       try:
          data1 = json.loads(msg.payload)
       #print data1
       except ValueError:
          return mqttc.publish('jts/oyo/error','{"function":"delete_unit","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')

       if(data1.get('session_id') is None):
          output_str = "session_id is mandatory"
          output = '{"function":"delete_unit","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']



       req2 = requests.post(url = 'http://localhost:5904/GenericACApp/delete_unit_utype/', data = msg.payload,verify=False)
       res = req2.text
       res = json.loads(res)
       res["session_id"]="%s" %(sid)
       res =json.dumps(res)
       #print "response fro cld: ",res
       return mqttc.publish('jts/oyo/error',str(res))
###################### delete room ##########################################
def delete_room(mosq,obj,msg):
       #print "delete room......."
       try:
          data1 = json.loads(msg.payload)
       #print data1
       except ValueError:
          return mqttc.publish('jts/oyo/error','{"function":"delete_room","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')

       if(data1.get('session_id') is None):
          output_str = "session_id is mandatory"
          output = '{"function":"delete_room","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']
       
       req2 = requests.post(url = 'http://localhost:5904/GenericACApp/delete_room/', data = msg.payload,verify=False)
       res = req2.text
       res = json.loads(res)
       res["session_id"]="%s" %(sid)
       res =json.dumps(res)
       #res = json.loads(res)
       #print "response fro cld: ",res
       return mqttc.publish('jts/oyo/error',str(res))
       
###################### delete floor ##########################################
def delete_floor(mosq,obj,msg):
       #print "delete floor......."
       try:
          data1 = json.loads(msg.payload)
       #print data1
       except ValueError:
          return mqttc.publish('jts/oyo/error','{"function":"delete_floor","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"delete_floor","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']
       req2 = requests.post(url = 'http://localhost:5904/GenericACApp/delete_floor/', data = msg.payload,verify=False)
       res = req2.text
       res = json.loads(res)
       res["session_id"]="%s" %(sid)
       res =json.dumps(res)
       #res = json.loads(res)
       #print "response fro cld: ",res
       return mqttc.publish('jts/oyo/error',str(res))
       

###################### delete city ##########################################
def delete_city(mosq,obj,msg):
       #print "delete city......."
       try:
          data1 = json.loads(msg.payload)
       #print data1
       except ValueError:
          return mqttc.publish('jts/oyo/error','{"function":"delete_city","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"delete_city","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']
     
       req2 = requests.post(url = 'http://localhost:5904/GenericACApp/delete_city/', data = msg.payload,verify=False)
       res = req2.text
       res = json.loads(res)
       res["session_id"]="%s" %(sid)
       res =json.dumps(res)
       #res = json.loads(res)
       #print "response fro cld: ",res
       return mqttc.publish('jts/oyo/error',str(res))
       
################# delete premise ##############################################
def delete_premise(mosq,obj,msg):
       #print "delete premise......."
       try:
          data1 = json.loads(msg.payload)
       #print data1
       except ValueError:
          return mqttc.publish('jts/oyo/error','{"function":"delete_premise","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"delete_premise","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']

       req2 = requests.post(url = 'http://localhost:5904/GenericACApp/delete_premise/', data = msg.payload,verify=False)
       res = req2.text
       res = json.loads(res)
       res["session_id"]="%s" %(sid)
       res =json.dumps(res)
       #res = json.loads(res)
       #print "response fro cld: ",res
       return mqttc.publish('jts/oyo/error',str(res))
################get power #######################################
def get_power(mosq,obj,msg):
       #print "get power......."
       db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
       cursor = db.cursor()
       try:
          data1 = json.loads(msg.payload)
       #print data1
       except ValueError:
          return mqttc.publish('jts/oyo/error','{"function":"get_power","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
       try:
          if(data1.get('session_id') is None):
             output_str += ",session_id is mandatory"
             output = '{"function":"get_power","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
             return mqttc.publish('jts/oyo/error',output)

          sid = data1['session_id']
          if((data1.get('username') is None) or ((data1.get('username') is not  None) and (len(data1['username']) <= 0))):
             output_str += ", username is mandatory"
             output = '{"function":"get_power","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
             return mqttc.publish('jts/oyo/error',output)
          else:
             username = data1['username']

          if((data1.get('password') is None) or ((data1.get('password') is not  None) and (len(data1['password']) <= 0))):
             output_str += ", password is mandatory"
             output = '{"function":"get_power","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
             return mqttc.publish('jts/oyo/error',output)
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
             output_str += ",Your not Autherize to get_power"
             output = '{"function":"get_power","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
          ipid=False
          opid=False
          ppid=False
          iid=''
          oid=''
          pid=''
          if((data1.get('unit_id') is None) or ((data1.get('unit_id') is not  None) and (len(data1['unit_id']) <= 0))):
             output_str += ", unit_id is mandatory"
             output = '{"function":"get_power","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
             return mqttc.publish('jts/oyo/error',output)
          else:
             uid = data1['unit_id']
          #print "here" 
          sqlq12="select i.IpId,o.OpId,p.PId from AC_Input_Units as i left join AC_Operate_Units as o USING (UnitId) left join AC_Power_Units as p USING (UnitId) where UnitId='%s' union select i.IpId,o.OpId,p.PId  from AC_Input_Units as i right join AC_Operate_Units as o USING (UnitId) right join AC_Power_Units as p USING (UnitId) where UnitId='%s' union select i.IpId,o.OpId,p.PId  from AC_Input_Units as i left join AC_Operate_Units as o USING (UnitId) right join AC_Power_Units as p USING (UnitId) where UnitId='%s'" %(uid,uid,uid)
          '''
          sqlq12="""SELECT * FROM AC_Input_Units left OUTER JOIN AC_Operate_Units 
                    ON(AC_Input_Units.PremiseId = AC_Operate_Units.PremiseId ) left OUTER JOIN AC_Power_Units
                    ON(AC_Operate_Units.PremiseId = AC_Power_Units.PremiseId ) WHERE AC_Input_Units.UnitId='%s' or AC_Operate_Units.UnitId='%s' or AC_Power_Units.UnitId='%s'
                union
                select * from AC_Input_Units
                RIGHT OUTER JOIN AC_Operate_Units 
                ON(AC_Input_Units.PremiseId = AC_Operate_Units.PremiseId) RIGHT OUTER JOIN AC_Power_Units
                ON(AC_Operate_Units.PremiseId = AC_Power_Units.PremiseId) WHERE AC_Input_Units.UnitId='%s' or AC_Operate_Units.UnitId='%s' or AC_Power_Units.UnitId='%s'
                union
                select * from AC_Input_Units right outer join AC_Operate_Units
                ON(AC_Input_Units.PremiseId = AC_Operate_Units.PremiseId) left OUTER JOIN AC_Power_Units
                ON(AC_Operate_Units.PremiseId = AC_Power_Units.PremiseId) WHERE AC_Input_Units.UnitId='%s' or AC_Operate_Units.UnitId='%s' or AC_Power_Units.UnitId='%s' """ %(uid,uid,uid,uid,uid,uid,uid,uid,uid)
          '''
          #sqlq12="SELECT i.IpId,o.OpId,p.PId FROM AC_Input_Units AS i LEFT JOIN AC_Operate_Units AS o ON (i.CityId=o.CityId and i.PremiseId=o.PremiseId and i.FloorId=o.FloorId and i.RoomId=o.RoomId) LEFT JOIN AC_Power_Units AS p ON (i.CityId=p.CityId and i.PremiseId=p.PremiseId and i.FloorId=p.FloorId and i.RoomId=p.RoomId) WHERE (i.UnitId='%s' and i.ChangeBy='%s') or (o.UnitId='%s' and o.ChangeBy='%s') or (p.UnitId='%s' and p.ChangeBy='%s')" %(uid,username,uid,username,uid,username)      
          cursor.execute(sqlq12)
          get_unit = cursor.fetchone()
          #print "uint .....",get_unit
          if get_unit[0] is None:
             pass
          else:
             iid=get_unit[0]
             ipid=True
          if get_unit[1] is None:
             pass
          else:
             oid=get_unit[1]
             opid=True
          if get_unit[2] is None:
             pass
          else:
             pid=get_unit[2]
             ppid=True
          #print iid,oid,pid,ipid,opid,ppid
          if ipid==True and opid==True and ppid==True:
             pass
          else:
             output_str += ", should have input,operate and power units."
             output = '{"function":"get_power","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
             return mqttc.publish('jts/oyo/error',output)
          meavg1=''
          meavg0=''
          tot_time=''
          #sqlq1="select AC_Operate_History.MotionE,AC_Operate_History.ChangeDate,AC_Power_History.Power,AC_Power_History.ChangeDate from AC_Operate_History,AC_Power_History where (DATE(AC_Operate_History.ChangeDate) > DATE_SUB(CURDATE(), INTERVAL 3 DAY) AND DATE(AC_Operate_History.ChangeDate) < CURDATE()) and (DATE(AC_Power_History.ChangeDate) > DATE_SUB(CURDATE(), INTERVAL 3 DAY) AND DATE(AC_Power_History.ChangeDate) < CURDATE())  and AC_Power_History.PId='%s' and AC_Operate_History.MotionE=1 group by hour(AC_Operate_History.ChangeDate),hour(AC_Power_History.ChangeDate)" %(pid)
          sqlq1="SELECT sum(AC_Power_History.Power)/3000 as avgPower FROM AC_Power_History INNER JOIN ( SELECT * FROM AC_Operate_History WHERE MotionE = '1' and OpId='%s' and (DATE(AC_Operate_History.ChangeDate) > DATE_SUB(CURDATE(), INTERVAL 4 DAY) AND DATE(AC_Operate_History.ChangeDate) < CURDATE()) ) foo ON AC_Power_History.ChangeDate = foo.ChangeDate where AC_Power_History.PId='%s' and (DATE(AC_Power_History.ChangeDate) > DATE_SUB(CURDATE(), INTERVAL 4 DAY) AND DATE(AC_Power_History.ChangeDate) < CURDATE()) " %(oid,pid)
          cursor.execute(sqlq1)
          get_avg1 = cursor.fetchone()
          #print "avg 1 .....",get_avg1
          if get_avg1[0] is None:
             meavg1='0.1'
          else:
             meavg1=get_avg1[0]
          #print meavg1
          sqlq2="SELECT sum(AC_Power_History.Power)/3000 as avgPower FROM AC_Power_History INNER JOIN ( SELECT * FROM AC_Operate_History WHERE MotionE = '0' and OpId='%s' and (DATE(AC_Operate_History.ChangeDate) > DATE_SUB(CURDATE(), INTERVAL 4 DAY) AND DATE(AC_Operate_History.ChangeDate) < CURDATE()) ) foo ON AC_Power_History.ChangeDate = foo.ChangeDate where AC_Power_History.PId='%s' and (DATE(AC_Power_History.ChangeDate) > DATE_SUB(CURDATE(), INTERVAL 4 DAY) AND DATE(AC_Power_History.ChangeDate) < CURDATE()) " %(oid,pid)
          cursor.execute(sqlq2)
          get_avg0 = cursor.fetchone()
          #print "avg 0 .....",get_avg0
          if get_avg0[0] is None:
             meavg0='0.0'
          else:
             meavg0=get_avg0[0]
          #print meavg0
          sqlq3="SELECT count(*) as time FROM AC_Power_History INNER JOIN ( SELECT * FROM AC_Operate_History WHERE MotionE = '0' and OpId='%s' and ChangeDate > CURDATE() ) foo ON AC_Power_History.ChangeDate = foo.ChangeDate where AC_Power_History.PId='%s' and AC_Power_History.ChangeDate > CURDATE() " %(oid,pid)
          #sqlq3="SELECT count(*) from AC_Power_History where PId='%s' and ChangeDate > curdate() " %(pid)
          cursor.execute(sqlq3)
          get_time = cursor.fetchone()
          #print "time .....",get_time
          if get_time[0] is None:
             tot_time='1'
          else:
             tot_time=get_time[0]
          #print tot_time
          #avgdiff=get_avg1[0]-get_avg0[0]
          #avgdiff=float(meavg1)-float(meavg0)
          #print "diff time ",avgdiff
          #print avgdiff*float(tot_time)
          #print "final is ",float(tot_time),float(meavg1),float(meavg0)
          power_save=float(tot_time)*(float(meavg1)-float(meavg0))
          #print power_save
          #print power_save*10
          power_save1=power_save*10
          output = '{"error_code":"0","function":"get_power","session_id":"%s","saved_units":"%s","Rs":"%s"}' %(sid,power_save,power_save1)
          return mqttc.publish('jts/oyo/error',output)
          '''
          sqlq1 = "select avg(Power) from AC_Operate_History where MotionE=1 and DATE_ADD(ChangeDate, INTERVAL 60 MINUTE) >= NOW() and ChangeBy='%s'" %(username)
          cursor.execute(sqlq1)
          results1 = cursor.fetchone()
          sqlq2 = "select avg(Power) from AC_Operate_History where MotionE=0 and DATE_ADD(ChangeDate, INTERVAL 60 MINUTE) >= NOW() and ChangeBy='%s'" %(username)
          cursor.execute(sqlq2)
          results2 = cursor.fetchone()
          #print results1,results2
          if results1 >0 or results2 > 0:
             NP=results1[0]
             EP=results2[0]
             Diff=NP-EP
             output = '{"error_code":"0","function":"get_power","session_id":"%s","NP":"%s","EP":"%s","Diff":"%s"}' %(sid,NP,EP,Diff)
             return mqttc.publish('jts/oyo/error',output)

          else:
             output = '{"function":"get_power","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the power"}' %(sid)
             return mqttc.publish('jts/oyo/error',output)
          '''
       except Exception, e:
           cursor.close()
           db.close()
           output = '{"function":"get_power","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the power"}' %(sid)
           return mqttc.publish('jts/oyo/error',output)   
       
       #output = '{"error_code":"0","function":"get_power","session_id":"%s","NP":"35","EP":"23","Diff":"12"}' %(sid)
       #return mqttc.publish('jts/oyo/error',output)
###################### delete city ##########################################
def get_units_test(mosq,obj,msg):
    print "get units test ......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
    cursor = db.cursor()
    sqlq = "select i.IpId,i.UnitId,o.OpId,o.UnitId,p.PId,p.UnitId from AC_Input_Units as i left join AC_Operate_Units as o USING (UnitId) left join AC_Power_Units as p USING (UnitId)  group by i.UnitId,o.UnitId,p.UnitId  union select i.IpId,i.UnitId,o.OpId,o.UnitId,p.PId,p.UnitId  from AC_Input_Units as i right join AC_Operate_Units as o USING (UnitId) right join AC_Power_Units as p USING (UnitId) group by i.UnitId,o.UnitId,p.UnitId  union select i.IpId,i.UnitId,o.OpId,o.UnitId,p.PId,p.UnitId  from AC_Input_Units as i left join AC_Operate_Units as o USING (UnitId) right join AC_Power_Units as p USING (UnitId) group by i.UnitId,o.UnitId,p.UnitId"
    cursor.execute(sqlq)
    units = cursor.fetchall()
    print "get all units ",units
    '''
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
    '''
######################## get temp history ######################################
def get_temp_history(mosq,obj,msg):
    #print "get_roles......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
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


    if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"get_roles","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

    sid = data1['session_id']

    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"function":"get_roles","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('password') is None):
        output_str += ",passsword is mandatory"
        output = '{"function":"get_roles","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/oyo/error',output)

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
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to get roles"
       output = '{"function":"get_temp_history","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
       return mqttc.publish('jts/oyo/error',output)
    try:
       if((data1.get('stdate') is None) or ((data1.get('stdate') is not  None) and (len(data1['stdate']) <= 0))):
          output_str += ", stdate is mandatory"
          output = '{"function":"get_temp_history","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
          return mqttc.publish('jts/oyo/error',output)
       else:
          stdate = data1['stdate']

       if((data1.get('enddate') is None) or ((data1.get('enddate') is not  None) and (len(data1['enddate']) <= 0))):
          output_str += ", enddate is mandatory"
          output = '{"function":"get_temp_history","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
          return mqttc.publish('jts/oyo/error',output)
       else:
          enddate = data1['enddate']
       st_date='2018-09-24 00:00'
       end_date='2018-09-24 23:59'
       #print end_date.strftime('%Y-%m-%d %H:%M')
       stdate = datetime.strptime(st_date, '%Y-%m-%d %H:%M')
       stdate= datetime.strftime(stdate, '%Y-%m-%d %H:%M') 
       enddate = datetime.strptime(end_date, '%Y-%m-%d %H:%M')
       enddate = datetime.strftime(enddate, '%Y-%m-%d %H:%M')
       #print stdate,enddate
       sqlq2="SELECT UnitId,format(avg(Temp),0) as temp ,ChangeDate FROM AC_Input_History WHERE ChangeDate>='%s' and ChangeDate <= '%s' and ChangeBy='%s' GROUP BY HOUR(ChangeDate),MINUTE(ChangeDate) ORDER BY IpId" %(stdate,enddate,username)
       #sqlq2 = "SELECT UnitId,format(avg(Temp),0) as temp,date_format(ChangeDate,'%Y-%m-%d %H:%i')as process_time FROM Oyo_Unit_History where ChangeDate >= '%s' and ChangeDate <= '%s' group by UnitId, date_format(ChangeDate,'%Y%m%d %H:%i')" %(stdate,enddate'))
       #print sqlq2
       cursor.execute(sqlq2)
       get_roles_rec = cursor.fetchall()
       #print len(get_roles_rec)
       if(len(get_roles_rec) > 0):
        #{
          output = '{"function":"get_temp_history","session_id":"%s","error_code":"0", "Response":"Successfully got %d history", \n "get_roles":' %(sid,len(get_roles_rec))
          output += '['
          counter = 0
          for rec in get_roles_rec:
          #{
             counter += 1
             #rec[2]=rec[2].strptime('%Y-%m-%d %H:%M')
             if(counter == 1):
               output += '{"UnitId":"%s","Temp":"%s","ChangeDate":"%s"}' %(rec[0] ,rec[1],rec[2])
             else:
               output += ',\n {"UnitId":"%s","Temp":"%s","ChangeDate":"%s"}' %(rec[0] ,rec[1],rec[2])
          #}
          output += ']\n'
          output += '}'
          cursor.close()
          db.close()
          return mqttc.publish('jts/oyo/error',output)
       else:
           output = '{"function":"get_temp_history","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the role records, NO_DATA_FOUND"}' %(sid)
           return mqttc.publish('jts/oyo/error',output)
      
          
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_temp_history","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the roles"}' %(sid)
        return mqttc.publish('jts/oyo/error',output)


################ add_input_unit ############################
def add_unit(mosq,obj,msg):
    #print "add_input_unit......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
    cursor = db.cursor()
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
       return mqttc.publish('jts/oyo/error','{"function":"add_unit","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:
      
       uinput=False
       uoperate=False
       upower=False

       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"add_unit","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']

       if(data1.get('username') is None):
          output_str += ",username is mandatory"
          output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          return mqttc.publish('jts/oyo/error',output)
       
       if(data1.get('password') is None):
          output_str += ",password is mandatory"
          output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          return mqttc.publish('jts/oyo/error',output)

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
          cursor.close()
          db.close()
          output_str += ",Your not Autherize to Register a Device"
          output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          return mqttc.publish('jts/oyo/error',output)

       if((data1.get('utype') is None) or ((data1.get('utype') is not  None) and (len(data1['utype']) <= 0))):
           output_str += ", utype is mandatory"
           output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
           return mqttc.publish('jts/oyo/error',output)
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
             return mqttc.publish('jts/oyo/error',output)
          else:
             ipmacid = data1['ipmacid'] 
           		 
          if((data1.get('ipname') is None) or ((data1.get('ipname') is not  None) and (len(data1['ipname']) <= 0))):
             output_str += ", ipname is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
          else:
             ipname = data1['ipname'] 
           
          if((data1.get('abrivation') is None) or ((data1.get('abrivation') is not  None) and (len(data1['abrivation']) <= 0))):
             output_str += ", abrivation is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
          else:
             abrivation = data1['abrivation']

          if((data1.get('floor_id') is None) or ((data1.get('floor_id') is not  None) and (len(data1['floor_id']) <= 0))):
             output_str += ", floor_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
          else:
             floor = data1['floor_id']
           
          if((data1.get('room_id') is None) or ((data1.get('room_id') is not  None) and (len(data1['room_id']) <= 0))):
             output_str += ", room_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
          else:
             room = data1['room_id']
          
          if((data1.get('premise_id') is None) or ((data1.get('premise_id') is not  None) and (len(data1['premise_id']) <= 0))):
             output_str += ", premise_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
          else:
             premise = data1['premise_id']

          if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
             output_str += ", city_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
          else:
             city = data1['city_id']

          if((data1.get('unit_id') is None) or ((data1.get('unit_id') is not  None) and (len(data1['unit_id']) <= 0))):
             output_str += ", unit_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
          else:
             uid = data1['unit_id']
       
          #DB
       
          sqlq2="SELECT * FROM AC_Input_Units WHERE IpMacid='%s' and ChangeBy='%s'" %(ipmacid,username)
          cursor.execute(sqlq2)
          results2 = cursor.fetchone()
          #print "mapping data from db: ",results2
          if results2 > 0:
             #print 'maping data exits......... so quiting ..Thanks'
             cursor.close()
             db.close()
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=input unit exits"}' %(sid)
             return mqttc.publish('jts/oyo/error',output)
          else:
             #print 'start mapping .......'
             add_rec1=cursor.execute("""INSERT INTO AC_Input_Units(CityId,FloorId,RoomId,PremiseId,UnitId,IpmacId,IpName,Abrivation,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(city,floor,room,premise,uid,ipmacid,ipname,abrivation,username,date))
             db.commit()
             if add_rec1 > 0:
                #print 'input mac  rec mapped in DB.'
                output = '{"function":"add_unit","session_id":"%s","error_code":"0", "Response":"Successfully added Input Unit "}' %(sid)
	        return mqttc.publish('jts/oyo/error',output)
             else:
                cursor.close()
                db.close()
                output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=Falied to add a input Units"}' %(sid)
                return mqttc.publish('jts/oyo/error',output)

       if uoperate==True:
          if((data1.get('opmacid') is None) or ((data1.get('opmacid') is not  None) and (len(data1['opmacid']) <= 0))):
             output_str += ", opmacid is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
          else:
             opmacid = data1['opmacid'] 
           		 
          if((data1.get('opname') is None) or ((data1.get('opname') is not  None) and (len(data1['opname']) <= 0))):
             output_str += ", opname is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
          else:
             opname = data1['opname'] 

          if((data1.get('abrivation') is None) or ((data1.get('abrivation') is not  None) and (len(data1['abrivation']) <= 0))):
             output_str += ", abrivation is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
          else:
             abrivation = data1['abrivation']
           
          if((data1.get('floor_id') is None) or ((data1.get('floor_id') is not  None) and (len(data1['floor_id']) <= 0))):
             output_str += ", floor_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
          else:
             floor = data1['floor_id']
           
          if((data1.get('room_id') is None) or ((data1.get('room_id') is not  None) and (len(data1['room_id']) <= 0))):
             output_str += ", room_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
          else:
             room = data1['room_id']
          
          if((data1.get('premise_id') is None) or ((data1.get('premise_id') is not  None) and (len(data1['premise_id']) <= 0))):
             output_str += ", premise_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
          else:
             premise = data1['premise_id']

          if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
             output_str += ", city_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
          else:
             city = data1['city_id']       
         
          if((data1.get('unit_id') is None) or ((data1.get('unit_id') is not  None) and (len(data1['unit_id']) <= 0))):
             output_str += ", unit_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
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
             return mqttc.publish('jts/oyo/error',output)
          sqlq2="SELECT * FROM AC_Operate_Units WHERE OpMacid='%s' and ChangeBy='%s'" %(opmacid,username)
          cursor.execute(sqlq2)
          results2 = cursor.fetchone()
          #print "mapping data from db: ",results2
          if results2 > 0:
             #print 'maping data exits......... so quiting ..Thanks'
             cursor.close()
             db.close()
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=operate unit exits"}' %(sid)
             return mqttc.publish('jts/oyo/error',output)
          else:
             #print 'start mapping .......'
             add_rec1=cursor.execute("""INSERT INTO AC_Operate_Units(CityId,FloorId,RoomId,PremiseId,UnitId,OpMacid,OpName,Abrivation,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(city,floor,room,premise,uid,opmacid,opname,abrivation,username,date))
             db.commit()
             if add_rec1 > 0:
                #print 'operate  mac rec mapped in DB.'
                for ipmacid in results12:
                    output1 = '{"function":"add_unit","error_code":"0", "Response":"Succes","opmacid":"%s"}'%(opmacid)
                    mqttc.publish('jts/oyo/'+ipmacid,output1)

                output = '{"function":"add_unit","session_id":"%s","error_code":"0", "Response":"Successfully added Operate Unit "}' %(sid)
	        return mqttc.publish('jts/oyo/error',output)
             else:
                cursor.close()
                db.close()
                output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=Falied to add operate Units"}' %(sid)
                return mqttc.publish('jts/oyo/error',output)
       
       if upower==True:

          if((data1.get('pmacid') is None) or ((data1.get('pmacid') is not  None) and (len(data1['pmacid']) <= 0))):
             output_str += ", pmacid is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
          else:
             pmacid = data1['pmacid'] 
           		 
          if((data1.get('pname') is None) or ((data1.get('pname') is not  None) and (len(data1['pname']) <= 0))):
             output_str += ", pname is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
          else:
             pname = data1['pname'] 
         
          if((data1.get('abrivation') is None) or ((data1.get('abrivation') is not  None) and (len(data1['abrivation']) <= 0))):
             output_str += ", abrivation is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
          else:
             abrivation = data1['abrivation']
           
          if((data1.get('floor_id') is None) or ((data1.get('floor_id') is not  None) and (len(data1['floor_id']) <= 0))):
             output_str += ", floor_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
          else:
             floor = data1['floor_id']
           
          if((data1.get('room_id') is None) or ((data1.get('room_id') is not  None) and (len(data1['room_id']) <= 0))):
             output_str += ", room_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
          else:
             room = data1['room_id']
          
          if((data1.get('premise_id') is None) or ((data1.get('premise_id') is not  None) and (len(data1['premise_id']) <= 0))):
             output_str += ", premise_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
          else:
             premise = data1['premise_id']

          if((data1.get('city_id') is None) or ((data1.get('city_id') is not  None) and (len(data1['city_id']) <= 0))):
             output_str += ", city_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
          else:
             city = data1['city_id']
       
          if((data1.get('unit_id') is None) or ((data1.get('unit_id') is not  None) and (len(data1['unit_id']) <= 0))):
             output_str += ", unit_id is mandatory"
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
          else:
             uid = data1['unit_id']
          #DB
       
          sqlq2="SELECT * FROM AC_Power_Units WHERE PMacid='%s' and ChangeBy='%s'" %(pmacid,username)
          cursor.execute(sqlq2)
          results2 = cursor.fetchone()
          #print "mapping data from db: ",results2
          if results2 > 0:
             #print 'maping data exits......... so quiting ..Thanks'
             cursor.close()
             db.close()
             output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=power unit exits"}' %(sid)
             return mqttc.publish('jts/oyo/error',output)
          else:
             #print 'start mapping .......'
             add_rec1=cursor.execute("""INSERT INTO AC_Power_Units(CityId,FloorId,RoomId,PremiseId,UnitId,PMacid,PName,Abrivation,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(city,floor,room,premise,uid,pmacid,pname,abrivation,username,date))
             db.commit()
             if add_rec1 > 0:
                #print 'power  mac rec mapped in DB.'
                output = '{"function":"add_unit","session_id":"%s","error_code":"0", "Response":"Successfully added Power Unit "}' %(sid)
	        return mqttc.publish('jts/oyo/error',output)
             else:
                cursor.close()
                db.close()
                output = '{"function":"add_unit","session_id":"%s","error_code":"2", "error_desc": "Response=Falied to add power Units"}' %(sid)
                return mqttc.publish('jts/oyo/error',output)
 


    except MySQLdb.Error, e:
        try:
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]) )
        except IndexError:
            #print "MySQL Error: %s" % str(e)
            return mqttc.publish('jts/oyo/error',str(e)) 
       
    except Exception, e:  
        cursor.close()
        db.close()
	output = '{"function":"add_unit","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to add the unit"}' %(sid) 
        return mqttc.publish('jts/oyo/error',output)

######################## add_new_unit #########################
def add_new_unit(mosq,obj,msg):
    #print "add_new_unit......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
    cursor = db.cursor()
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
       return mqttc.publish('jts/oyo/error','{"function":"add_new_unit","error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')
    try:
      
       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"add_new_unit","error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']

       if(data1.get('username') is None):
          output_str += ",username is mandatory"
          output = '{"function":"add_new_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          return mqttc.publish('jts/oyo/error',output)
       
       if(data1.get('password') is None):
          output_str += ",password is mandatory"
          output = '{"function":"add_new_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          return mqttc.publish('jts/oyo/error',output)

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
          cursor.close()
          db.close()
          output_str += ",Your not Autherize to Register a Device"
          output = '{"function":"add_new_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          return mqttc.publish('jts/oyo/error',output)
       dummyvalue=id_generator()     
       add_rec1=cursor.execute("""INSERT INTO AC_Units(UnitDesc,ChangeBy,ChangeDate) VALUES (%s,%s,%s)""",(str(dummyvalue),username,date))
       db.commit() 
       if add_rec1 > 0:
          sqlq = "SELECT UnitId FROM AC_Units WHERE UnitDesc='"+dummyvalue+"'"
          cursor.execute(sqlq)
          results = cursor.fetchone()
          if results > 0 :
             output = '{"error_code":"0","function":"add_new_unit","session_id":"%s","Response":"Succesfully Generated id for new unit","Unit_id":"%s"}' %(sid,results[0])
             cursor.close()
             db.close()
             return mqttc.publish('jts/oyo/error',output)  
          else:
             output_str += ",Unable to get id"
             output = '{"function":"add_new_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
             return mqttc.publish('jts/oyo/error',output)
       else:
          output_str += ",Unable to generate id"
          output = '{"function":"add_new_unit","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          return mqttc.publish('jts/oyo/error',output)


    except MySQLdb.Error, e:
        try:
            #print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            return mqttc.publish('jts/oyo/error',str(e.args[0])+str(e.args[1]) )
        except IndexError:
            #print "MySQL Error: %s" % str(e)
            return mqttc.publish('jts/oyo/error',str(e)) 
       
    except Exception, e:  
        cursor.close()
        db.close()
	output = '{"function":"add_new_unit","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to generate unit id"}' %(sid) 
        return mqttc.publish('jts/oyo/error',output)



###################### update sensor units value ##################
def update_sensor_data(mosq,obj,msg):
    #print "update_sensor_data........ "
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
    cursor = db.cursor()
    output_str = "update_sensor_data - Unable to Authenticate/update_sensor_data... " 
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M')
    #print "time",date
    #date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}' )
    try:
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
                return mqttc.publish('jts/oyo/error',output)
             else:
                output = '{"error_code":"2", "error_desc": "Response=Unable to update Operate unit details"}'
                cursor.close()
                db.close()
                return mqttc.publish('jts/oyo/error',output)
    
          else:
             print 'mac id not avilable , so configure....Thanks.'
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
             #print temp
             if str(temp)==str('-127.0'):
                output_str += ", temp is  wrong"
                output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
                return mqttc.publish('jts/oyo/error',output)
             else:
                temp = data1['temp']
       
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
             if temp > 30 and refrige=="Refrigerator" and changeby=="production":
                #print "inside sms "
                #url = 'https://api.textlocal.in/send/?apikey=irlz29yH7zo-7Yxe1P8dAA7ylIwBatMOwf9xJbukjg&numbers=9133156641&sender=JTSIOT&message=Alert:%20Your%20Temparature%20is%20too%20high/low,please%20check.'
	        #serialized_data = urllib2.urlopen(url).read()
                #data = json.loads(serialized_data)
                pass
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
                return mqttc.publish('jts/oyo/error',output)
             else:
                output = '{"error_code":"2", "error_desc": "Response=Unable to update temp"}'
                cursor.close()
                db.close()
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
             upd_rec=cursor.execute ("""UPDATE AC_Power_Units SET Voltage=%s,Power=%s,Current=%s,ChangeDate=%s,ChangeBy=%s WHERE PId=%s""", (voltage,power,current,date,changeby,unitid))
             db.commit()
             add_rec2=cursor.execute("""INSERT INTO AC_Power_History(PId,PMacid,Voltage,Power,Current,ChangeBy,ChangeDate,UnitId) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",(unitid,macid,voltage,power,current,changeby,date,uid))
             db.commit()
             if len(str(upd_rec)) > 0 and len(str(add_rec2)):
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
             #print 'mac id not avilable , so configure....Thanks.'
             output = '{"error_code":"2", "error_desc": "Response= macid not exists.please Configure it."}'
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
        output = '{"error_code":"3", "error_desc": "Response=Failed to add the temp"}'
        return mqttc.publish('jts/oyo/error',output)

################### get room units ############################################
def get_room_units(mosq,obj,msg):
    #print "get_get_room_units......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
    cursor = db.cursor()
    output_str = "get_room_units- Unable to Authenticate/get_room_units... "
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')

    if(not data1):
        output_str += ", details are mandatory"
        output = '{"function":"get_room_units","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
        return mqttc.publish('jts/oyo/error',output)


    if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"get_room_units","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

    sid = data1['session_id']

    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"function":"get_room_units","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('password') is None):
        output_str += ",passsword is mandatory"
        output = '{"function":"get_room_units","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/oyo/error',output)

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
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to get get_room_units"
       output = '{"function":"get_room_units","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
       return mqttc.publish('jts/oyo/error',output)
    try:
       if((data1.get('unit_id') is None) or ((data1.get('unit_id') is not  None) and (len(data1['unit_id']) <= 0))):
          output_str += ", unit_id is mandatory"
          output = '{"function":"get_room_units","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
          return mqttc.publish('jts/oyo/error',output)
       else:
          uid = data1['unit_id'] 

       sqlq2="SELECT IpId,IpMacid,IpName,Abrivation FROM AC_Input_Units WHERE UnitId=%s and ChangeBy='%s'" %(uid,username)
       #print sqlq2
       cursor.execute(sqlq2)
       get_ip_rec = cursor.fetchall()
       sqlq3="SELECT OpId,OpMacid,OpName,Abrivation FROM AC_Operate_Units WHERE UnitId=%s and ChangeBy='%s'" %(uid,username)
       #print sqlq3
       cursor.execute(sqlq3)
       get_op_rec = cursor.fetchall()
       #print len(get_op_rec)
       sqlq4="SELECT PId,PMacid,PName,Abrivation FROM AC_Power_Units WHERE UnitId=%s and ChangeBy='%s'" %(uid,username)
       #print sqlq4
       cursor.execute(sqlq4)
       get_po_rec = cursor.fetchall()
       #print len(get_po_rec)

       if len(get_ip_rec) > 0 :
        #{
          #print "in if"
          output = '{"function":"get_room_units","session_id":"%s","error_code":"0", "Response":"Success",' %(sid)
          if len(get_ip_rec) > 0:
             output +='\n "get_input_units":'
             output += '['
             counter = 0
             #print "in ip"
             for rec in get_ip_rec:
             #{
                 counter += 1
                 if(counter == 1):
                    output += '{"uid":"%s","macid":"%s","name":"%s","abrivation":"%s","utype":"Input"}' %(rec[0] ,rec[1],rec[2],rec[3])
                 else:
                    output += ',\n {"uid":"%s","macid":"%s","name":"%s","abrivation":"%s","utype":"Input"}' %(rec[0] ,rec[1],rec[2],rec[3])
             #}
             output += '],\n'
          #print "ip ....",output
          if len(get_op_rec) > 0:
             output +='\n "get_operate_units":'
             output += '['
             counter = 0
             for rec in get_op_rec:
             #{
                 counter += 1
                 if(counter == 1):
                    output += '{"uid":"%s","macid":"%s","name":"%s","abrivation":"%s","utype":"Operate"}' %(rec[0] ,rec[1],rec[2],rec[3])
                 else:
                    output += ',\n {"uid":"%s","macid":"%s","name":"%s","abrivation":"%s","utype":"Operate"}' %(rec[0] ,rec[1],rec[2],rec[3])
             #}
             output += '],\n'
          #print "op ....",output
          if len(get_po_rec) > 0:
             output +='\n "get_power_units":'
             output += '['
             counter = 0
             for rec in get_po_rec:
             #{
                 counter += 1
                 if(counter == 1):
                    output += '{"uid":"%s","macid":"%s","name":"%s","abrivation":"%s","utype":"Power"}' %(rec[0] ,rec[1],rec[2],rec[3])
                 else:
                    output += ',\n {"uid":"%s","macid":"%s","name":"%s","abrivation":"%s","utype":"Power"}' %(rec[0] ,rec[1],rec[2],rec[3])
             #}
             output += ']\n'
          #print "po ....",output
          output += '}'
          cursor.close()
          db.close()
          return mqttc.publish('jts/oyo/error',output)
       else:
           output = '{"function":"get_room_units","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the room units, NO_DATA_FOUND"}' %(sid)
           return mqttc.publish('jts/oyo/error',output)
      
          
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_room_units","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the room units"}' %(sid)
        return mqttc.publish('jts/oyo/error',output)

###################### get Unit Details ############################################
def get_unit_details(mosq,obj,msg):
    #print "get_unit_details......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
    cursor = db.cursor()
    output_str = "get_unit_details- Unable to Authenticate/get_unit_details... "
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')

    if(not data1):
        output_str += ", details are mandatory"
        output = '{"function":"get_unit_details","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
        return mqttc.publish('jts/oyo/error',output)


    if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"get_unit_details","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

    sid = data1['session_id']

    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"function":"get_unit_details","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('password') is None):
        output_str += ",passsword is mandatory"
        output = '{"function":"get_unit_details","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/oyo/error',output)

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
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to get roles"
       output = '{"function":"get_unit_details","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
       return mqttc.publish('jts/oyo/error',output)
    try:

       if((data1.get('unit_id') is None) or ((data1.get('unit_id') is not  None) and (len(data1['unit_id']) <= 0))):
          output_str += ", unit_id is mandatory"
          output = '{"function":"get_unit_details","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
          return mqttc.publish('jts/oyo/error',output)
       else:
          uid = data1['unit_id']
       sqlq5=""
       sqlq2 = "SELECT * FROM AC_Input_Units WHERE UnitId='%s' and ChangeBy='%s'" %(uid,username)
       cursor.execute(sqlq2)
       get_ip_rec = cursor.fetchall()
       if(len(get_ip_rec) > 0):
          sqlq5 = "SELECT i.CityId,i.PremiseId,i.FloorId,i.RoomId,i.IpId,i.IpMacid,o.OpId,o.OpMacid,p.PId,p.PMacid FROM AC_Input_Units AS i LEFT JOIN AC_Operate_Units AS o ON (i.CityId=o.CityId and i.PremiseId=o.PremiseId and i.FloorId=o.FloorId and i.RoomId=o.RoomId) LEFT JOIN AC_Power_Units AS p ON (i.CityId=p.CityId and i.PremiseId=p.PremiseId and i.FloorId=p.FloorId and i.RoomId=p.RoomId) WHERE i.UnitId='%s' and i.ChangeBy='%s'" %(uid,username)
       else:
          sqlq3 = "SELECT * FROM AC_Operate_Units WHERE UnitId='%s' and ChangeBy='%s'" %(uid,username)
          cursor.execute(sqlq3)
          get_op_rec = cursor.fetchall()
          if(len(get_op_rec) > 0):
             sqlq5 = "SELECT o.CityId,o.PremiseId,o.FloorId,o.RoomId,i.IpId,i.IpMacid,o.OpId,o.OpMacid,p.PId,p.PMacid FROM AC_Input_Units AS i LEFT JOIN AC_Operate_Units AS o ON (i.CityId=o.CityId and i.PremiseId=o.PremiseId and i.FloorId=o.FloorId and i.RoomId=o.RoomId) LEFT JOIN AC_Power_Units AS p ON (i.CityId=p.CityId and i.PremiseId=p.PremiseId and i.FloorId=p.FloorId and i.RoomId=p.RoomId) WHERE o.UnitId='%s' and o.ChangeBy='%s'" %(uid,username)
          else:
             sqlq4 = "SELECT * FROM AC_Power_Units WHERE UnitId='%s' and ChangeBy='%s'" %(uid,username)
             cursor.execute(sqlq4)
             get_p_rec = cursor.fetchall()
             if(len(get_p_rec) > 0):
                sqlq5 = "SELECT p.CityId,p.PremiseId,p.FloorId,p.RoomId,i.IpId,i.IpMacid,o.OpId,o.OpMacid,p.PId,p.PMacid FROM AC_Input_Units AS i LEFT JOIN AC_Operate_Units AS o ON (i.CityId=o.CityId and i.PremiseId=o.PremiseId and i.FloorId=o.FloorId and i.RoomId=o.RoomId) LEFT JOIN AC_Power_Units AS p ON (i.CityId=p.CityId and i.PremiseId=p.PremiseId and i.FloorId=p.FloorId and i.RoomId=p.RoomId) WHERE p.UnitId='%s' and p.ChangeBy='%s'" %(uid,username)
             else:
                output_str = "Unit_id is not exists"
                output = '{"function":"get_unit_details","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
                return mqttc.publish('jts/oyo/error',output)



       cursor.execute(sqlq5)
       get_unit_rec = cursor.fetchall()
       if(len(get_unit_rec) > 0):
        #{
          output = '{"function":"get_unit_details","session_id":"%s","error_code":"0", "Response":"Successfully got %d recs", \n "get_unit_details":' %(sid,len(get_unit_rec))
          output += '['
          counter = 0
          for rec in get_unit_rec:
          #{
             counter += 1
             if(counter == 1):
               output += '{"city_id":"%s","premise_id":"%s","floor_id":"%s","room_id":"%s","ip_id":"%s","ip_macid":"%s","op_id":"%s","op_macid":"%s","p_id":"%s","p_macid":"%s"}' %(rec[0] ,rec[1],rec[2],rec[3],rec[4],rec[5],rec[6],rec[7],rec[8],rec[9])
             else:
               output += ',\n {"city_id":"%s","premise_id":"%s","floor_id":"%s","room_id":"%s","ip_id":"%s","ip_macid":"%s","op_id":"%s","op_macid":"%s","p_id":"%s","p_macid":"%s"}' %(rec[0] ,rec[1],rec[2],rec[3],rec[4],rec[5],rec[6],rec[7],rec[8],rec[9])
          #}
          output += ']\n'
          output += '}'
          cursor.close()
          db.close()
          return mqttc.publish('jts/oyo/error',output)
       else:
           output = '{"function":"get_unit_details","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the unit records, NO_DATA_FOUND"}' %(sid)
           return mqttc.publish('jts/oyo/error',output)
      
          
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_unit_details","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the data"}' %(sid)
        return mqttc.publish('jts/oyo/error',output)

#################### get Graph history############################################
def get_graph_history(mosq,obj,msg):
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
    cursor = db.cursor()
    output_str = "get_graph_history - Unable to Authenticate/get_graph_history... "
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"error_code":"2","function":"get_graph_history","error_desc":"Response=invalid input, no proper JSON request"}')


    try:
       if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"get_graph_history","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

       sid = data1['session_id']

       if(data1.get('username') is None):
          output_str += ",username is mandatory"
          output = '{"function":"get_graph_history","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          return mqttc.publish('jts/oyo/error',output)

       if(data1.get('password') is None):
          output_str += ",passsword is mandatory"
          output = '{"function":"get_graph_history","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          return mqttc.publish('jts/oyo/error',output)

       username    = data1['username']
       password    = data1['password']
       sqlq = "SELECT * FROM AC_Users WHERE Username='"+username+"' AND Password='"+password+"'"
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
          output = '{"function":"get_graph_history","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
          return mqttc.publish('jts/oyo/error',output)
       graph_Day=False
       graph_Month=False
       graph_Year=False
       ipid=False
       opid=False
       ppid=False
       iid=''
       oid=''
       pid=''
       kwargs={}
       if((data1.get('unit_id') is None) or ((data1.get('unit_id') is not  None) and (len(data1['unit_id']) <= 0))):
          output_str += ", unit_id is mandatory"
          output = '{"function":"get_graph_history","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
          return mqttc.publish('jts/oyo/error',output)
       else:
          uid = data1['unit_id']
          sqlq12="select a.IpId,b.OpId,c.PId from AC_Input_Units as a left join AC_Operate_Units as b USING (UnitId) left join AC_Power_Units as c USING (UnitId) where UnitId='%s' union select a.IpId,b.OpId,c.PId from AC_Input_Units as a right join AC_Operate_Units as b USING (UnitId) right join AC_Power_Units as c USING (UnitId) where UnitId='%s' union select a.IpId,b.OpId,c.PId from AC_Input_Units as a left join AC_Operate_Units as b USING (UnitId) right join AC_Power_Units as c USING (UnitId) where UnitId='%s'" %(uid,uid,uid)
          '''
          sqlq12="""SELECT * FROM AC_Input_Units left OUTER JOIN AC_Operate_Units 
	            ON(AC_Input_Units.PremiseId = AC_Operate_Units.PremiseId ) left OUTER JOIN AC_Power_Units
		    ON(AC_Operate_Units.PremiseId = AC_Power_Units.PremiseId ) WHERE AC_Input_Units.UnitId='%s' or AC_Operate_Units.UnitId='%s' or AC_Power_Units.UnitId='%s'
		union
		select * from AC_Input_Units
		RIGHT OUTER JOIN AC_Operate_Units 
		ON(AC_Input_Units.PremiseId = AC_Operate_Units.PremiseId) RIGHT OUTER JOIN AC_Power_Units
		ON(AC_Operate_Units.PremiseId = AC_Power_Units.PremiseId) WHERE AC_Input_Units.UnitId='%s' or AC_Operate_Units.UnitId='%s' or AC_Power_Units.UnitId='%s'
		union
		select * from AC_Input_Units right outer join AC_Operate_Units
		ON(AC_Input_Units.PremiseId = AC_Operate_Units.PremiseId) left OUTER JOIN AC_Power_Units
		ON(AC_Operate_Units.PremiseId = AC_Power_Units.PremiseId) WHERE AC_Input_Units.UnitId='%s' or AC_Operate_Units.UnitId='%s' or AC_Power_Units.UnitId='%s' """ %(uid,uid,uid,uid,uid,uid,uid,uid,uid)
          '''
          #sqlq12="SELECT i.IpId,o.OpId,p.PId FROM AC_Input_Units AS i LEFT JOIN AC_Operate_Units AS o ON (i.CityId=o.CityId and i.PremiseId=o.PremiseId and i.FloorId=o.FloorId and i.RoomId=o.RoomId) LEFT JOIN AC_Power_Units AS p ON (i.CityId=p.CityId and i.PremiseId=p.PremiseId and i.FloorId=p.FloorId and i.RoomId=p.RoomId) WHERE (i.UnitId='%s' and i.ChangeBy='%s') or (o.UnitId='%s' and o.ChangeBy='%s') or (p.UnitId='%s' and p.ChangeBy='%s')" %(uid,username,uid,username,uid,username)      
          cursor.execute(sqlq12)
          get_unit = cursor.fetchone()
          #print "uint .....",get_unit
          '''
          if get_unit[0] is None: 
             pass             
          else:
	     iid=get_unit[0]
             ipid=True
          if get_unit[13] is None:
             pass
          else:
             oid=get_unit[13]
             opid=True
          if get_unit[31] is None:
             pass            
          else:
             pid=get_unit[31]
             ppid=True
          print iid,oid,pid,ipid,opid,ppid
          '''
          if get_unit[0] is None:
             pass
          else:
             iid=get_unit[0]
             ipid=True
          if get_unit[1] is None:
             pass
          else:
             oid=get_unit[1]
             opid=True
          if get_unit[2] is None:
             pass
          else:
             pid=get_unit[2]
             ppid=True
          #print iid,oid,pid,ipid,opid,ppid
       if((data1.get('function') is None) or ((data1.get('function') is not  None) and (len(data1['function']) <= 0))):
          output_str += ", function is mandatory"
          output = '{"function":"get_graph_history","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
          return mqttc.publish('jts/oyo/error',output)
       else:
          function = data1['function']
          if function=="Day":
             graph_Day=True
          if function=="Month":
             graph_Month=True
          if function=="Year":
             graph_Year=True
       if graph_Month==True:
          #print "inside the month"
          today = datetime.now().date()
          #print(today)
          curr_day=today.day
          #print(curr_day)
          first = today.replace(day=1) #first day of the current month
          #print(first)
          lastMonth = first - timedelta(days=1) #end of the previous month
          #print(lastMonth)
          month_old_date = '%s-%s-%s' %(lastMonth.year, lastMonth.month, curr_day)
          #print(month_old_date)
          if ipid==True:
             sqlq1="select Avg(Temp) as Temp,ChangeDate from AC_Input_History WHERE IpId='%s' and ChangeDate >= '%s' GROUP BY DAY(ChangeDate) ORDER BY IHId" %(iid,month_old_date)
             #print sqlq1
             cursor.execute(sqlq1)
             get_temp = cursor.fetchall()
             #print "temp .....",get_temp
          else:
             get_temp=''
          if opid==True:
             sqlq2="select Avg(Power) as power,ChangeDate from AC_Operate_History WHERE OpId='%s' and ChangeDate >= '%s' GROUP BY DAY(ChangeDate) ORDER BY OHId" %(oid,month_old_date)
             cursor.execute(sqlq2)
             get_power = cursor.fetchall()
             #print "operate Power.......",get_power
             sqlq3="select Avg(MotionE) as motion,ChangeDate from AC_Operate_History WHERE OpId='%s' and ChangeDate >= '%s' GROUP BY DAY(ChangeDate) ORDER BY OHId" %(oid,month_old_date)
             cursor.execute(sqlq3)
             get_voltage = cursor.fetchall()
             #print "operate voltage .......",get_voltage
          else:
             get_power=''
             get_voltage=''
          if ppid==True:
             sqlq2="select Avg(Power) as power,ChangeDate from AC_Power_History WHERE PId='%s' and ChangeDate >= '%s' GROUP BY DAY(ChangeDate) ORDER BY PHId" %(pid,month_old_date)
             cursor.execute(sqlq2)
             get_power = cursor.fetchall()
             #print "Power.......",get_power
             #sqlq3="select Avg(Voltage) as voltage,ChangeDate from AC_Power_History WHERE PId='%s' and ChangeDate >= '%s' GROUP BY DAY(ChangeDate) ORDER BY PHId" %(pid,month_old_date)
             #cursor.execute(sqlq3)
             #get_voltage = cursor.fetchall()
             #print "Power.......",get_voltage
          else:
             get_power=''
             #get_voltage=''

          
          if len(get_temp) > 0 or len(get_power) > 0 or len(get_voltage) > 0:
             output = '{"function":"get_graph_history","session_id":"%s","error_code":"0", "Response":"Success",' %(sid)
             output +='\n "get_temp_history":'
             output += '['
             if len(get_temp) > 0:
                #output +='\n "get_temp_history":'
                #output += '['
                counter = 0
                for rec in get_temp:
                    stdate = datetime.strftime(rec[1], '%Y-%m-%d %H:%M')
                    counter += 1
                    if(counter == 1):
                       output += '{"temp":"%s","changedate":"%s"}' %(rec[0] ,stdate)
                    else:
                       output += ',\n {"temp":"%s","changedate":"%s"}' %(rec[0] ,stdate)
             
             output += '],\n'
             #print "Temp ....",output
             output +='\n "get_power_history":'
             output += '['
             if len(get_power) > 0:
                #output +='\n "get_power_history":'
                #output += '['
                counter = 0
                for rec in get_power:
                    counter += 1
                    stdate = datetime.strftime(rec[1], '%Y-%m-%d %H:%M')
                    if(counter == 1):
                       output += '{"power":"%s","changedate":"%s"}' %(rec[0] ,stdate)
                    else:
                       output += ',\n {"power":"%s","changedate":"%s"}' %(rec[0] ,stdate)
             output += '],\n'
             #print "Power ....",output
             output +='\n "get_ecomode_history":'
             output += '['
             if len(get_voltage) > 0:
                #output +='\n "get_voltage_history":'
                #output += '['
                counter = 0
                for rec in get_voltage:
                    counter += 1
                   
                    stdate = datetime.strftime(rec[1], '%Y-%m-%d %H:%M')
                    if(counter == 1):
                       output += '{"motion":"%s","changedate":"%s"}' %(rec[0] ,stdate)
                    else:
                       output += ',\n {"motion":"%s","changedate":"%s"}' %(rec[0] ,stdate)
             
             output += ']\n'
             #print "voltage ....",output
             output += '}'
             #print "final optput ",output
             cursor.close()
             db.close()
             return mqttc.publish('jts/oyo/error',output)
      
          else:
             output = '{"function":"get_graph_history","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the history records, NO_DATA_FOUND"}' %(sid)
             return mqttc.publish('jts/oyo/error',output)

       if graph_Day==True:
          #print "inside the day"
          today = datetime.now().date()
          #print(today)
          if ipid==True:
             #FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(ChangeDate)/(10*60))*(10*60)) AS time_int     10 mns time interval
             sqlq1="select Avg(Temp) as Temp,FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(ChangeDate)/(10*60))*(10*60)) AS time_int from AC_Input_History WHERE IpId='%s' and ChangeDate > CURDATE() GROUP BY time_int ORDER BY IHId" %(iid)
             #sqlq1="select Avg(Temp) as Temp,ChangeDate from AC_Input_History WHERE IpId='%s' and ChangeDate > CURDATE() GROUP BY HOUR(ChangeDate),MINUTE(ChangeDate) ORDER BY IHId" %(iid)
             #print sqlq1
             cursor.execute(sqlq1)
             get_temp = cursor.fetchall()
             #print "temp .....",get_temp
          else:
             get_temp=''
          if opid==True:
             sqlq2="select Avg(Power) as power,FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(ChangeDate)/(10*60))*(10*60)) AS time_int from AC_Operate_History WHERE OpId='%s' and ChangeDate > CURDATE() GROUP BY time_int ORDER BY OHId" %(oid)
             cursor.execute(sqlq2)
             get_power = cursor.fetchall()
             #print "operate Power.......",get_power
             sqlq3="select Avg(MotionE) as motion,FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(ChangeDate)/(10*60))*(10*60)) AS time_int from AC_Operate_History WHERE OpId='%s' and ChangeDate > CURDATE() GROUP BY time_int ORDER BY OHId" %(oid)
             cursor.execute(sqlq3)
             get_voltage = cursor.fetchall()
             #print "operate voltage .......",get_voltage
          else:
             get_power=''
             get_voltage=''
          if ppid==True:
             sqlq2="select Avg(Power),FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(ChangeDate)/(10*60))*(10*60)) AS time_int from AC_Power_History WHERE PId='%s' and ChangeDate > CURDATE() GROUP BY time_int ORDER BY PHId" %(pid)
             cursor.execute(sqlq2)
             get_power = cursor.fetchall()
             #print "Power.......",get_power
             #sqlq3="select Avg(Voltage),ChangeDate from AC_Power_History WHERE PId='%s' and ChangeDate > CURDATE() GROUP BY MINUTE(ChangeDate) ORDER BY PHId" %(pid)
             #cursor.execute(sqlq3)
             #get_voltage = cursor.fetchall()
          else:
             get_power=''
             #get_voltage=''
          '''
          sqlq1="select round(Avg(Temp)) as Temp,ChangeDate from AC_Input_History WHERE IpId='%s' and ChangeDate > CURDATE() GROUP BY HOUR(ChangeDate) ORDER BY IHId" %(uid)
          print sqlq1
          cursor.execute(sqlq1)
          get_temp = cursor.fetchall()
          print "temp .....",get_temp
          
          sqlq2="select Power,ChangeDate from AC_Operate_History WHERE OpId=18 and ChangeDate = '%s' GROUP BY HOUR(ChangeDate) ORDER BY OHId" %(today)
          cursor.execute(sqlq2)
          get_power = cursor.fetchall()
          get_power=0
          #print "Power.......",get_power
          sqlq3="select Voltage,ChangeDate from AC_Operate_History WHERE OpId=18 and ChangeDate >= '%s' GROUP BY HOUR(ChangeDate) ORDER BY OHId" %(today)
          cursor.execute(sqlq3)
          get_voltage = cursor.fetchall()
          '''
       
          #print "voltage .......",get_voltage
          if len(get_temp) > 0  or len(get_power) > 0 or len(get_voltage) > 0:
             output = '{"function":"get_graph_history","session_id":"%s","error_code":"0", "Response":"Success",' %(sid)
             output +='\n "get_temp_history":'
             output += '['
             if len(get_temp) > 0:
                #output +='\n "get_temp_history":'
                #output += '['
                counter = 0
                for rec in get_temp:
                    counter += 1
                    #print rec[1]
                    #stdate = datetime.strftime(rec[1], '%Y-%m-%d %H:%M')
                    #print stdate
		    stdate= datetime.strftime(rec[1], '%Y-%m-%d %H:%M')
                    #print stdate
                    if(counter == 1):
                       output += '{"temp":"%s","changedate":"%s"}' %(rec[0] ,stdate)
                    else:
                       output += ',\n {"temp":"%s","changedate":"%s"}' %(rec[0] ,stdate)
             
             output += '],\n'
             #print "Temp ....",output
             output +='\n "get_power_history":'
             output += '['
             if len(get_power) > 0:
                #output +='\n "get_power_history":'
                #output += '['
                counter = 0
                for rec in get_power:
                    counter += 1
                    stdate = datetime.strftime(rec[1], '%Y-%m-%d %H:%M')
                    #print t1
                    if(counter == 1):
                       output += '{"power":"%s","changedate":"%s"}' %(rec[0] ,stdate)
                    else:
                       output += ',\n {"power":"%s","changedate":"%s"}' %(rec[0] ,stdate)
             output += '],\n'
             #print "Power ....",output
             output +='\n "get_ecomode_history":'
             output += '['
             #print "Power ....",output
             if len(get_voltage) > 0:
                #output +='\n "get_voltage_history":'
                #output += '['
                counter = 0
                for rec in get_voltage:
                    counter += 1
                    stdate = datetime.strftime(rec[1], '%Y-%m-%d %H:%M')
                    if(counter == 1):
                       output += '{"motion":"%s","changedate":"%s"}' %(rec[0] ,stdate)
                    else:
                       output += ',\n {"motion":"%s","changedate":"%s"}' %(rec[0] ,stdate)
             
             output += ']\n'
             #print "voltage ....",output
             output += '}'
             #print "output ....",len(output)
             #print "byte",output
             cursor.close()
             db.close()
             return mqttc.publish('jts/oyo/error',output)
      
          else:
             output = '{"function":"get_graph_history","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the history records, NO_DATA_FOUND"}' %(sid)
             return mqttc.publish('jts/oyo/error',output)
  
       if graph_Year==True:
          #print "inside the year"
          today = datetime.now().date()
          #print(today)
          year_old_date = '%s-%s-%s' %(today.year -1 , today.month, today.day)
          #print(year_old_date)
          if ipid==True:
             sqlq1="select Avg(Temp) as Temp,ChangeDate from AC_Input_History WHERE IpId='%s' and ChangeDate >= '%s' GROUP BY MONTH(ChangeDate) ORDER BY IHId" %(iid,year_old_date)
             #print sqlq1
             cursor.execute(sqlq1)
             get_temp = cursor.fetchall()
             #print "temp .....",get_temp
          else:
             get_temp=''
          if opid==True:
             sqlq2="select Avg(Power) as power,ChangeDate from AC_Operate_History WHERE OpId='%s' and ChangeDate >= '%s' GROUP BY MONTH(ChangeDate) ORDER BY OHId" %(oid,year_old_date)
             cursor.execute(sqlq2)
             get_power = cursor.fetchall()
             #print "operate Power.......",get_power
             sqlq3="select Avg(MotionE) as motion,ChangeDate from AC_Operate_History WHERE OpId='%s' and ChangeDate >= '%s' GROUP BY MONTH(ChangeDate) ORDER BY OHId" %(oid,year_old_date)
             cursor.execute(sqlq3)
             get_voltage = cursor.fetchall()
             #print "operate voltage .......",get_voltage
          else:
             get_power=''
             get_voltage=''
          if ppid==True:
             sqlq2="select Avg(Power),ChangeDate from AC_Power_History WHERE PId='%s' and ChangeDate >= '%s'  GROUP BY MONTH(ChangeDate) ORDER BY PHId" %(pid,year_old_date)
             cursor.execute(sqlq2)
             get_power = cursor.fetchall()
             #print "Power.......",get_power
             #sqlq3="select Avg(Voltage),ChangeDate from AC_Power_History WHERE PId='%s' and ChangeDate >= '%s' GROUP BY MONTH(ChangeDate) ORDER BY PHId" %(pid,year_old_date)
             #cursor.execute(sqlq3)
             #get_voltage = cursor.fetchall()
          else:
             get_power=''
             #get_voltage=''
          '''
          sqlq1="select round(Avg(Temp)) as Temp,ChangeDate from AC_Input_History WHERE IpId='%s' and ChangeDate >= '%s' GROUP BY MONTH(ChangeDate) ORDER BY IHId" %(uid,year_old_date)
          cursor.execute(sqlq1)
          get_temp = cursor.fetchall()
          #print "temp .....",get_temp
          
          sqlq2="select Power,ChangeDate from AC_Operate_History WHERE OpId=18 and ChangeDate = '%s' GROUP BY MONTH(ChangeDate) ORDER BY OHId" %(year_old_date)
          cursor.execute(sqlq2)
          get_power = cursor.fetchall()
          get_power=0
          #print "Power.......",get_power
          sqlq3="select Voltage,ChangeDate from AC_Operate_History WHERE OpId=18 and ChangeDate >= '%s' GROUP BY MONTH(ChangeDate) ORDER BY OHId" %(year_old_date)
          cursor.execute(sqlq3)
          get_voltage = cursor.fetchall()
          '''
          get_power=''
          get_voltage=''
          #print "voltage .......",get_voltage
          if len(get_temp) > 0  or len(get_power) > 0 or len(get_voltage) > 0:
             output = '{"function":"get_graph_history","session_id":"%s","error_code":"0", "Response":"Success",' %(sid)
             output +='\n "get_temp_history":'
             output += '['
             if len(get_temp) > 0:
                #output +='\n "get_temp_history":'
                #output += '['
                counter = 0
                for rec in get_temp:
                    stdate = datetime.strftime(rec[1], '%Y-%m-%d %H:%M')
                    counter += 1
                    if(counter == 1):
                       output += '{"temp":"%s","changedate":"%s"}' %(rec[0] ,stdate)
                    else:
                       output += ',\n {"temp":"%s","changedate":"%s"}' %(rec[0] ,stdate)
             
             output += '],\n'
             output +='\n "get_power_history":'
             output += '['
             #print "Temp ....",output
             if len(get_power) > 0:
                #output +='\n "get_power_history":'
                #output += '['
                counter = 0
                for rec in get_power:
                    counter += 1
                    stdate = datetime.strftime(rec[1], '%Y-%m-%d %H:%M')
                    if(counter == 1):
                       output += '{"power":"%s","changedate":"%s"}' %(rec[0] ,stdate)
                    else:
                       output += ',\n {"power":"%s","changedate":"%s"}' %(rec[0] ,stdate)
             output += '],\n'
             #print "Power ....",output
             output +='\n "get_ecomode_history":'
             output += '['
             if len(get_voltage) > 0:
                #output +='\n "get_voltage_history":'
                #output += '['
                counter = 0
                for rec in get_voltage:
                    counter += 1
                    stdate = datetime.strftime(rec[1], '%Y-%m-%d %H:%M')
                    if(counter == 1):
                       output += '{"motion":"%s","changedate":"%s"}' %(rec[0] ,stdate)
                    else:
                       output += ',\n {"motion":"%s","changedate":"%s"}' %(rec[0] ,stdate)
             
             output += ']\n'
             #print "voltage ....",output
             output += '}'
             cursor.close()
             db.close()
             #print "output ....",output
             return mqttc.publish('jts/oyo/error',output)
      
          else:
             output = '{"function":"get_graph_history","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the history records, NO_DATA_FOUND"}' %(sid)
             return mqttc.publish('jts/oyo/error',output)
           
        
         
       '''
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
       #get_graph_historys_rec = cursor.fetchall()
       #print len(get_graph_historys_rec)
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
          output = '{"function":"get_graph_history","error_code":"0",  \n "Top_History":'
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
          return mqttc.publish('jts/oyo/error',output)
       else:
           output = '{"function":"get_graph_history","error_code":"3", "error_desc": "Response=Failed to get the history records, NO_DATA_FOUND"}'
           return mqttc.publish('jts/oyo/error',output)

       '''
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_graph_history","error_code":"3", "error_desc": "Response=Failed to get the history"}'
        return mqttc.publish('jts/oyo/error',output)

################################ get_clients#########################
def get_clients(mosq,obj,msg):
    #print "get_clients......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
    cursor = db.cursor()
    output_str = "get_clients - Unable to Authenticate/get_clients... "
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')

    if(not data1):
        output_str += ", details are mandatory"
        output = '{"function":"get_clients","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
        return mqttc.publish('jts/oyo/error',output)


    if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"get_clients","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

    sid = data1['session_id']

    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"function":"get_clients","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('password') is None):
        output_str += ",passsword is mandatory"
        output = '{"function":"get_clients","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/oyo/error',output)

    username    = data1['username']
    password    = data1['password']
    sqlq = "SELECT ClientId FROM AC_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
    #sqlq = "SELECT * FROM Oyo_Users"
    #print "Checking Credentials : : ",sqlq
    cursor.execute(sqlq)
    results = cursor.fetchone()
    #print "Checking Data exists or not : ",results
    cid=''
    if results > 0:
      cid=results[0]
      #print 'Login Data Existed',cid
    else:
       #print 'Login Data not authorized so quiting ........Thanks'
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to get roles"
       output = '{"function":"get_clients","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
       return mqttc.publish('jts/oyo/error',output)
    try:
       sqlq2=''
       if str(cid)==str(1):
          sqlq2 = "SELECT ClientId,ClientName FROM AC_Clients "
           
       else:
          sqlq2 = "SELECT u.ClientId,c.ClientName FROM AC_Users AS u LEFT JOIN AC_Clients AS c ON (u.ClientId=c.ClientId) where u.Username='%s'" %(username)
       cursor.execute(sqlq2)
       get_clients_rec = cursor.fetchall()
       #print get_clients_rec
       if(len(get_clients_rec) > 0):
        #{
          output = '{"function":"get_clients","session_id":"%s","error_code":"0", "Response":"Successfully got %d clients", \n "get_clients":' %(sid,len(get_clients_rec))
          output += '['
          counter = 0
          for rec in get_clients_rec:
          #{
             counter += 1
             if(counter == 1):
               output += '{"client_id":"%s","client_desc":"%s"}' %(rec[0] ,rec[1])
             else:
               output += ',\n {"client_id":"%s","client_desc":"%s"}' %(rec[0] ,rec[1])
          #}
          output += ']\n'
          output += '}'
          cursor.close()
          db.close()
          return mqttc.publish('jts/oyo/error',output)
       else:
           output = '{"function":"get_clients","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the clients records, NO_DATA_FOUND"}' %(sid)
           return mqttc.publish('jts/oyo/error',output)
      
          
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_clients","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the clients"}' %(sid)
        return mqttc.publish('jts/oyo/error',output)

################################ get_images#########################
def get_images(mosq,obj,msg):
    #print "get_clients......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
    cursor = db.cursor()
    output_str = "get_images - Unable to Authenticate/get_images... "
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')

    if(not data1):
        output_str += ", details are mandatory"
        output = '{"function":"get_images","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
        return mqttc.publish('jts/oyo/error',output)


    if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"get_images","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

    sid = data1['session_id']

    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"function":"get_images","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('password') is None):
        output_str += ",passsword is mandatory"
        output = '{"function":"get_images","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/oyo/error',output)

    username    = data1['username']
    password    = data1['password']
    sqlq = "SELECT ClientId FROM AC_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
    #sqlq = "SELECT * FROM Oyo_Users"
    #print "Checking Credentials : : ",sqlq
    cursor.execute(sqlq)
    results = cursor.fetchone()
    #print "Checking Data exists or not : ",results
    cid=''
    if results > 0:
      cid=results[0]
      #print 'Login Data Existed',cid
    else:
       #print 'Login Data not authorized so quiting ........Thanks'
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to get roles"
       output = '{"function":"get_images","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
       return mqttc.publish('jts/oyo/error',output)
    try:
       sqlq2=''
       if str(cid)==str(1):
          sqlq2 = "SELECT ImgId,Image FROM AC_Images "
           
       else:
          sqlq2 = "SELECT u.ImgId,i.Image FROM AC_Users AS u LEFT JOIN AC_Images AS i ON (u.ImgId=i.ImgId) where u.Username='%s'" %(username)
       cursor.execute(sqlq2)
       get_images_rec = cursor.fetchall()
       #print get_clients_rec
       if(len(get_images_rec) > 0):
        #{
          output = '{"function":"get_images","session_id":"%s","error_code":"0", "Response":"Successfully got %d images", \n "get_images":' %(sid,len(get_images_rec))
          output += '['
          counter = 0
          for rec in get_images_rec:
          #{
             counter += 1
             if(counter == 1):
               output += '{"image_id":"%s","image_desc":"%s"}' %(rec[0] ,rec[1])
             else:
               output += ',\n {"image_id":"%s","image_desc":"%s"}' %(rec[0] ,rec[1])
          #}
          output += ']\n'
          output += '}'
          cursor.close()
          db.close()
          return mqttc.publish('jts/oyo/error',output)
       else:
           output = '{"function":"get_images","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the images records, NO_DATA_FOUND"}' %(sid)
           return mqttc.publish('jts/oyo/error',output)
      
          
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_images","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the images"}' %(sid)
        return mqttc.publish('jts/oyo/error',output)

################################ get_saved_units #########################
def get_saved_units(mosq,obj,msg):
    #print "get_saved_units......."
    db = MySQLdb.connect("localhost","jtsac","ptljtsac","JtsAcDB")
    cursor = db.cursor()
    output_str = "get_saved_units - Unable to Authenticate/get_saved_units... "
    date1 = datetime.today()
    date = datetime.strftime(date1, '%Y-%m-%d %H:%M:%S.%f')
    try:
       data1 = json.loads(msg.payload)
       #print data1
    except ValueError:
       return mqttc.publish('jts/oyo/error','{"error_code":"2","error_desc":"Response=invalid input, no proper JSON request"}')

    if(not data1):
        output_str += ", details are mandatory"
        output = '{"function":"get_saved_units","error_code":"2", "error_desc": "Response=%s"}' %(output_str )
        return mqttc.publish('jts/oyo/error',output)


    if(data1.get('session_id') is None):
          output_str += ",session_id is mandatory"
          output = '{"function":"get_saved_units","error_code":"2", "error_desc": "Response=%s"}' %(output_str)
          return mqttc.publish('jts/oyo/error',output)

    sid = data1['session_id']

    if(data1.get('username') is None):
        output_str += ",username is mandatory"
        output = '{"function":"get_saved_units","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/oyo/error',output)

    if(data1.get('password') is None):
        output_str += ",passsword is mandatory"
        output = '{"function":"get_saved_units","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
        return mqttc.publish('jts/oyo/error',output)

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
       cursor.close()
       db.close()
       output_str += ",Your not Autherize to get roles"
       output = '{"function":"get_saved_units","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str)
       return mqttc.publish('jts/oyo/error',output)
    try:
       graph_Day=False
       graph_Month=False
       graph_Year=False
       ipid=False
       opid=False
       ppid=False
       iid=''
       oid=''
       pid=''
       kwargs={}
       if((data1.get('unit_id') is None) or ((data1.get('unit_id') is not  None) and (len(data1['unit_id']) <= 0))):
          output_str += ", unit_id is mandatory"
          output = '{"function":"get_graph_history","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
          return mqttc.publish('jts/oyo/error',output)
       else:
          uid = data1['unit_id']
          sqlq12="select a.IpId,b.OpId,c.PId from AC_Input_Units as a left join AC_Operate_Units as b USING (UnitId) left join AC_Power_Units as c USING (UnitId) where UnitId='%s' union select a.IpId,b.OpId,c.PId from AC_Input_Units as a right join AC_Operate_Units as b USING (UnitId) right join AC_Power_Units as c USING (UnitId) where UnitId='%s' union select a.IpId,b.OpId,c.PId from AC_Input_Units as a left join AC_Operate_Units as b USING (UnitId) right join AC_Power_Units as c USING (UnitId) where UnitId='%s'" %(uid,uid,uid)
          cursor.execute(sqlq12)
          get_unit = cursor.fetchone()
          #print "uint .....",get_unit
          if get_unit[0] is None:
             pass
          else:
             iid=get_unit[0]
             ipid=True
          if get_unit[1] is None:
             pass
          else:
             oid=get_unit[1]
             opid=True
          if get_unit[2] is None:
             pass
          else:
             pid=get_unit[2]
             ppid=True
          #print iid,oid,pid,ipid,opid,ppid
       if((data1.get('function') is None) or ((data1.get('function') is not  None) and (len(data1['function']) <= 0))):
          output_str += ", function is mandatory"
          output = '{"function":"get_graph_history","session_id":"%s","error_code":"2", "error_desc": "Response=%s"}' %(sid,output_str )
          return mqttc.publish('jts/oyo/error',output)
       else:
          function = data1['function']
          #if function=="Day":
          #   graph_Day=True
          if function=="Month":
             graph_Month=True
          if function=="Year":
             graph_Year=True
       if graph_Month==True:
          sqlq2 = "select sum(Power)/1000 as total_units , date(ChangeDate) as Date from AC_Power_History where PId='%s' and ChangeDate<curdate() group by day(ChangeDate) " %(pid)
          cursor.execute(sqlq2)
          get_power_rec = cursor.fetchall()
          #print get_power_rec[1]
          sqlq3 = " select Saved,date(ChangeDate) as Date from AC_Saved_Units where UnitId='%s' and ChangeDate<curdate()" %(uid)
          cursor.execute(sqlq3)
          get_saved_units_rec = cursor.fetchall()
          #print get_saved_units_rec[1]
          if(len(get_power_rec) > 0):
             output = '{"function":"get_saved_units","session_id":"%s","error_code":"0", "Response":"Successfully got %d recs", \n "get_saved_units":' %(sid,len(get_power_rec))
             output += '['
             counter = 0
             saved=0
             for i,rec in enumerate(get_power_rec):
                 if i in range(0,len(get_saved_units_rec)):
                    saved=get_saved_units_rec[i][0]
                 counter += 1
                 if(counter == 1):
                    output += '{"actual_units":"%s","saved_units":"%s","ChangeDate":"%s"}' %(get_power_rec[i][0] ,saved,get_power_rec[i][1])
                 else:
                    output += ',\n {"actual_units":"%s","saved_units":"%s","ChangeDate":"%s"}' %(get_power_rec[i][0] ,saved,get_power_rec[i][1])
          
             output += ']\n'
             output += '}'
             cursor.close()
             db.close()
             return mqttc.publish('jts/oyo/error',output)
          else:
             output = '{"function":"get_saved_units","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the get_saved_units records, NO_DATA_FOUND"}' %(sid)
             return mqttc.publish('jts/oyo/error',output)

       if graph_Year==True:
          sqlq2 = "select sum(Power)/1000 as total_units , date(ChangeDate) as Date from AC_Power_History where PId='%s' and ChangeDate<curdate() group by month(ChangeDate) " %(pid)
          cursor.execute(sqlq2)
          get_power_rec = cursor.fetchall()
          #print get_power_rec[1]
          sqlq3 = " select Saved,date(ChangeDate) as Date from AC_Saved_Units where UnitId='%s' and ChangeDate<curdate() group by month(ChangeDate)" %(uid)
          cursor.execute(sqlq3)
          get_saved_units_rec = cursor.fetchall()
          #print get_saved_units_rec[1]
          if(len(get_power_rec) > 0):
             output = '{"function":"get_saved_units","session_id":"%s","error_code":"0", "Response":"Successfully got %d recs", \n "get_saved_units":' %(sid,len(get_power_rec))
             output += '['
             counter = 0
             saved=0
             for i,rec in enumerate(get_power_rec):
                 if i in range(0,len(get_saved_units_rec)):
                    saved=get_saved_units_rec[i][0]
                 counter += 1
                 if(counter == 1):
                    output += '{"actual_units":"%s","saved_units":"%s","ChangeDate":"%s"}' %(get_power_rec[i][0] ,saved,get_power_rec[i][1])
                 else:
                    output += ',\n {"actual_units":"%s","saved_units":"%s","ChangeDate":"%s"}' %(get_power_rec[i][0] ,saved,get_power_rec[i][1])

             output += ']\n'
             output += '}'
             cursor.close()
             db.close()
             return mqttc.publish('jts/oyo/error',output)
          else:
             output = '{"function":"get_saved_units","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the get_saved_units records, NO_DATA_FOUND"}' %(sid)
             return mqttc.publish('jts/oyo/error',output)

    except Exception, e:
        cursor.close()
        db.close()
        output = '{"function":"get_saved_units","session_id":"%s","error_code":"3", "error_desc": "Response=Failed to get the get_saved_units"}' %(sid)
        return mqttc.publish('jts/oyo/error',output)



################## publish response #################################################
def on_publish(client, userdata, result):
        #print "data published \n"
        pass

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
mqttc.message_callback_add('jts/oyo/temp',temp1)
mqttc.message_callback_add('jts/oyo/op',operations)
######## web calls###############################
mqttc.message_callback_add('jts/oyo/validate_login',login)
mqttc.message_callback_add('jts/oyo/get_roles',get_roles)
mqttc.message_callback_add('jts/oyo/add_user',add_user)
mqttc.message_callback_add('jts/oyo/get_clients',get_clients)
mqttc.message_callback_add('jts/oyo/get_images',get_images)


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

mqttc.message_callback_add('jts/oyo/add_unit',add_unit)
mqttc.message_callback_add('jts/oyo/add_new_unit',add_new_unit)
mqttc.message_callback_add('jts/oyo/get_units_all',get_units)
mqttc.message_callback_add('jts/oyo/delete_unit',delete_unit)
mqttc.message_callback_add('jts/oyo/get_units_test',get_units_test)
mqttc.message_callback_add('jts/oyo/get_unit_details',get_unit_details)
mqttc.message_callback_add('jts/oyo/get_power',get_power)
mqttc.message_callback_add('jts/oyo/get_temp_history',get_temp_history)
mqttc.message_callback_add('jts/oyo/delete_unit_utype',delete_unit_utype)
mqttc.message_callback_add('jts/oyo/get_graph_history',get_graph_history)
mqttc.message_callback_add('jts/oyo/get_saved_units',get_saved_units)

#mqttc.message_callback_add('jts/oyo/add_roles',add_roles)
#mqttc.message_callback_add('jts/oyo/mod_roles',mod_roles)
#mqttc.message_callback_add('jts/oyo/del_roles',del_roles)
mqttc.message_callback_add('jts/oyo/test01',test01)

mqttc.message_callback_add('jts/oyo/get_room_units',get_room_units)
mqttc.message_callback_add('jts/oyo/temp',update_sensor_data)


mqttc.on_publish = on_publish
mqttc.username_pw_set('esp', 'ptlesp01')
mqttc.connect("cld003.jts-prod.in", 1883, 60)
mqttc.on_disconnect = on_disconnect
mqttc.on_connect = on_connect
mqttc.subscribe("jts/oyo/#")
mqttc.loop_forever()
#mqttc.username_pw_set('esp', 'ptlesp01')




