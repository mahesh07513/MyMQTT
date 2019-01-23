import paho.mqtt.client as mqttClient
import time
import json
import MySQLdb
def on_connect(client, userdata, flags, rc):
 
    if rc == 0:
 
        print("Connected to broker")
 
        global Connected                #Use global variable
        Connected = True                #Signal connection 
 
    else:
 
        print("Connection failed")
 
def on_message(client, userdata, message):
    print "Message : "  + message.payload
    print  "Topic : "+message.topic
    print "-----------------------------------------------------------------------------"
    j = json.loads(message.payload)
    print j['ipmacid']
    print j['topAcc']
    print j['bottomAcc']
    print j['Theft']
    db = MySQLdb.connect("localhost", "root", "root", "myPrototype")
    cursor = db.cursor()
    sqlq="insert into SeatProto(MacID,AccTop,AccBottom,Theft) values ('" +j['ipmacid']+"'"+','+j['topAcc']+','+j['bottomAcc']+','+j['Theft']+")"
    print(sqlq)
    cursor.execute(sqlq)
    db.commit();
    results = cursor.fetchone()
    print (results)
 
Connected = False   #global variable for the state of the connection
 
broker_address= "cld003.jts-prod.in"  #Broker address
port = 1883                      #Broker port
user = "esp"                    #Connection username
password = "ptlesp01"            #Connection password
 
client = mqttClient.Client("SeatRandger")               #create new instance
client.username_pw_set(user, password=password)    #set username and password
client.on_connect= on_connect                      #attach function to callback
client.on_message= on_message                      #attach function to callback
 
client.connect(broker_address, port=port)          #connect to broker
client.loop_start()        #start the loop
client.subscribe("/test/e2s/data")
while Connected != True:    #Wait for connection
    time.sleep(0.1)
 
try:
    while True:
        time.sleep(4)
        
 
except KeyboardInterrupt:
    print "exiting"
    client.disconnect()
    client.loop_stop()

