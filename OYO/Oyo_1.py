import paho.mqtt.client as mqtt
import MySQLdb
import json
#import datetime
import sys
#import time
from datetime import datetime

#On messege Payload
def register(mosq,obj,msg):
    db = MySQLdb.connect("localhost","oyoroom","ptljtsoyo","oyoDB")
    cursor = db.cursor()
    output_str = "register - Unable to Authenticate/register... " 
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
       if(data1.get('username') is None):
          output_str += ",username is mandatory"
          output = '{"error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)
       
       if(data1.get('password') is None):
          output_str += ",password is mandatory"
          output = '{"error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)
       username    = data1['username']
       password    = data1['password']
       sqlq = "SELECT * FROM Oyo_Users WHERE Username='"+username+"' AND Password='"+password+"' AND Role=1"
       #sqlq = "SELECT * FROM Oyo_Users"
       print "Checking Credentials : : ",sqlq
       cursor.execute(sqlq)
       results = cursor.fetchone()
       print "Checking Data exists or not : ",results
       if results > 0:
          print 'Login Data Existed'
       else:
          print 'Login Data not authorized so quiting ........Thanks'
          cursor.close()
          db.close()
          output_str += ",Your not Autherize to Register a Device"
          output = '{"error_code":"2", "error_desc": "Response=%s"}' %output_str
          return mqttc.publish('jts/oyo/error',output)
   
       if((data1.get('ipmacid') is None) or ((data1.get('ipmacid') is not  None) and (len(data1['ipmacid']) <= 0))):
           output_str += ", ipmacid is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           ipmacid = data1['ipmacid'] 
           		 
       if((data1.get('opmacid') is None) or ((data1.get('opmacid') is not  None) and (len(data1['opmacid']) <= 0))):
           output_str += ", opmacid is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           opmacid = data1['opmacid'] 
           
       if((data1.get('floor') is None) or ((data1.get('floor') is not  None) and (len(data1['floor']) <= 0))):
           output_str += ", floor is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           floor = data1['floor']
           
       if((data1.get('room') is None) or ((data1.get('room') is not  None) and (len(data1['room']) <= 0))):
           output_str += ", room is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           room = data1['room']
          

       if((data1.get('premise') is None) or ((data1.get('premise') is not  None) and (len(data1['premise']) <= 0))):
           output_str += ", premise is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           premise = data1['premise']
           
       #DB
       # input unit
       sqlq1 = "SELECT * FROM Oyo_Input_Units WHERE IpmacId='"+ipmacid+"'"
       #sqlq = "SELECT * FROM Oyo_Users"
       print "Checking input mac id : ",sqlq1
       cursor.execute(sqlq1)
       results = cursor.fetchone()
       print "input mac id from DB : ",results
       if results > 0:
          print 'input mac id is existed .... gohead'
       else:
          print 'input mac not there so adding ..........'
          add_rec=cursor.execute("""INSERT INTO Oyo_Input_Units(IpmacId,IpDesc,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s)""",(ipmacid,ipmacid,username,date))
          db.commit()
          if add_rec > 0:
             print 'input mac id rec added to db'
          else:
             print 'input mac id not added'
       #opearete unit
       sqlq1 = "SELECT * FROM Oyo_Operate_Units WHERE OpmacId='"+opmacid+"'"
       print "Checking operate mac id  : ",sqlq1
       cursor.execute(sqlq1)
       results1 = cursor.fetchone()
       print "operate mac id from db: ",results1
       if results1 > 0:
          print 'operate mac id is exists'
       else:
          print 'opearate macid not there so adding ..........'
          add_rec=cursor.execute("""INSERT INTO Oyo_Operate_Units(OpmacId,OpDesc,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s)""",(opmacid,opmacid,username,date))
          db.commit()
          if add_rec > 0:
             print 'operate macid rec added'
          else:
             print 'opearte not added'
       #oyo units mapping
       sqlq2 = "SELECT * FROM Oyo_Units WHERE IpmacId='"+ipmacid+"' AND OpmacId='"+opmacid+"'"
       print "input and opearte unit macid mapping ....... :",sqlq2
       cursor.execute(sqlq2)
       results2 = cursor.fetchone()
       print "mapping data from db: ",results2
       if results2 > 0:
          print 'maping data exits......... so quiting ..Thanks'
          cursor.close()
          db.close()
          output = '{"error_code":"-2", "error_desc": "Response=Operate macid asociate with this only, no need to add"}'
          return mqttc.publish('jts/oyo/error',output)
       else:
          print 'start mapping .......'
          #print(floor,premise,ipmacid,opmacid,username,date)
          #print"""INSERT INTO Oyo_Units(FloorId,PremiseId,IpmacId,OpmacId,ChangeBy,ChangeDate) VALUES (%d,%d,%d,%d,%s,%s)""",(int(floor),int(premise),int(ipmacid),int(opmacid),username,date)
          add_rec1=cursor.execute("""INSERT INTO Oyo_Units(FloorId,RoomId,PremiseId,IpmacId,OpmacId,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s)""",(floor,room,premise,ipmacid,opmacid,username,date))
          db.commit()
          if add_rec1 > 0:
             print 'input mac and operate  mac rec mapped in DB.'
             cursor.close()
             db.close()
             output = '{"error_code":"0", "Response":"Successfully Registered Input Unit and Operate Unit"}'
	     output1 = '{"error_code":"0", "Response":"Succes","opmacid":"%s"}'%(opmacid)
             mqttc.publish('jts/oyo/'+ipmacid,output1)
             return mqttc.publish('jts/oyo/error',output)
          else:
             print 'mapping issues .....check it.'
             cursor.close()
             db.close()
             output = '{"error_code":"2", "error_desc": "Response=Falied to add a Units"}'
             return mqttc.publish('jts/oyo/error',output)

       cursor.close()
       db.close()
    except Exception, e:  
        cursor.close()
        db.close()
	output = '{"error_code":"3", "error_desc": "Response=Failed to add the Register"}' 
        return mqttc.publish('jts/oyo/error',output)

def operations(mosq,obj,msg):
    print "this is operation",str(msg.payload)
    #mqttc.publish('jts/oyo/res','{"user":"mahesh1","pass":"babu1"}')

def temp(mosq,obj,msg):
    print "this is Temp from sensors : ",str(msg.payload)
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
          
       if((data1.get('macid') is None) or ((data1.get('macid') is not  None) and (len(data1['macid']) <= 0))):
           output_str += ", macid is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           macid = data1['macid'] 

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

       if((data1.get('Temp') is None) or ((data1.get('Temp') is not  None) and (len(data1['Temp']) <= 0))):
           output_str += ", Temp is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           temp = data1['Temp'] 

       if((data1.get('setTemp') is None) or ((data1.get('setTemp') is not  None) and (len(data1['setTemp']) <= 0))):
           output_str += ", settemp is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           settemp = data1['setTemp']
       '''
       if((data1.get('runTime') is None) or ((data1.get('runTime') is not  None) and (len(data1['runTime']) <= 0))):
           output_str += ", runTime is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           runtime = data1['runTime']

       if((data1.get('Strength') is None) or ((data1.get('Strength') is not  None) and (len(data1['Strength']) <= 0))):
           output_str += ", Strength is mandatory"
           output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str )
           return mqttc.publish('jts/oyo/error',output)
       else:
           strength = data1['Strength']
       '''

       sqlq1 = "SELECT * FROM Oyo_Operate_Units WHERE OpmacId='"+macid+"'"
       #sqlq = "SELECT * FROM Oyo_Users"
       print "Checking input mac id ....... :",sqlq1
       cursor.execute(sqlq1)
       results = cursor.fetchone()
       print "checking in opreate unit ..... :",results
       if results > 0:
          sqlq2 = "SELECT UnitId FROM Oyo_Units WHERE OpmacId='"+macid+"'"
          cursor.execute(sqlq2)
	  results1 = cursor.fetchone()
          print "checking in units...",results1[0]
          unitid=int(results1[0])
	  print unitid
          if results1>0:
             print "date",unitid
             upd_rec=cursor.execute ("""UPDATE Oyo_Units SET Temp=%s,ChangeDate=%s WHERE OpmacId=%s""", (temp,date,macid))
 	     
             sqlq3 = "SELECT * FROM Oyo_Unit_Details WHERE UnitId=%d"
             print sqlq3
	     cursor.execute("SELECT * FROM Oyo_Unit_Details WHERE UnitId=%s",[unitid])
             results2 = cursor.fetchone()
             print "checking in units details ...",results2		
             if results2>0:
                print "update"
                print 'id is there. so updating .........Thanks.'
                
	        upd_rec=cursor.execute ("""UPDATE Oyo_Unit_Details SET Fan=%s,Status=%s,Temp=%s,SetTemp=%s,ChangeDate=%s,ChangeBy=%s WHERE UnitId=%s""", (fan,status,temp,settemp,date,'jtsadmin',unitid))
                output = '{"error_code":"0", "Response":"Succesfully updated temp}' 
                db.commit()
                cursor.close()
                db.close()
                return mqttc.publish('jts/oyo/error',output)
             else:
                print "inserting ........."
                add_rec1=cursor.execute("""INSERT INTO Oyo_Unit_Details(UnitId,Fan,Status,Temp,SetTemp,ChangeBy,ChangeDate) VALUES (%s,%s,%s,%s,%s,%s,%s)""",(unitid,fan,status,temp,settemp,'jtsadmin',date))
         	db.commit()
                if add_rec1>0:
                   output = '{"error_code":"0", "Response":"Succesfully added temp}'
                   db.commit()
                   cursor.close()
                   db.close()
                   return mqttc.publish('jts/oyo/error',output) 
                
          else:
             print 'mac id not avilable , so configure....Thanks.'
             output = '{"error_code":"2", "error_desc": "Response= macid not exists.please Configure it."}'
             return mqttc.publish('jts/oyo/error',output)          
     
       else:
          print 'mac id not avilable , so configure....Thanks.'
  	  output = '{"error_code":"2", "error_desc": "Response= macid not exists.please Configure it."}'
          return mqttc.publish('jts/oyo/error',output)     
    except Exception, e:
        cursor.close()
        db.close()
        output = '{"error_code":"3", "error_desc": "Response=Failed to add the Register"}'
        return mqttc.publish('jts/oyo/error',output)

def on_publish(client, userdata, result):
        print "data published \n"

mqttc = mqtt.Client()

#Add message callbacks that will only trigger on a specific   subscription    match
mqttc.message_callback_add('jts/oyo/register', register)
mqttc.message_callback_add('jts/oyo/temp', temp)
mqttc.message_callback_add('jts/oyo/op', operations)
mqttc.on_publish = on_publish
mqttc.username_pw_set('esp', 'ptlesp01')
mqttc.connect("cld003.jts-prod.in", 1883, 60)
mqttc.subscribe("jts/oyo/#")
mqttc.loop_forever()
#mqttc.username_pw_set('esp', 'ptlesp01')




