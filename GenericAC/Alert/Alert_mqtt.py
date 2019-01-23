from datetime import datetime
from datetime import timedelta
import paho.mqtt.client as mqtt

#import datetime
import sys
import time
import requests
import string
import random
import urllib2
#################### mqtt alert  ##################
def alert(mosq,obj,msg):
    #print "in alert........ "
     
    try:
        timest=datetime.now()
	#print msg.payload
        time.sleep(3)
        mqttc.publish('jts/alertTest/mqtt','hello this is mqtt test program : %s' %(timest) )
             
    except Exception, e:
	#print "mqtt error"        
        pass

################## publish response #################################################
def on_publish(client, userdata, result):
        #print "data published \n"
        pass

def on_connect(client, userdata, flags, rc):
    #print("Connected with result code "+str(rc))
    mqttc.subscribe("jts/alertTest/mqtt")
    mqttc.publish('jts/alertTest/mqtt','hello this is mqtt test program')
def on_disconnect(client, userdata, rc):
    if rc != 0:
        time1=datetime.now()
        #print "Unexpected MQTT disconnection. Will auto-reconnect"
	url ="https://api.textlocal.in/send/?apikey=irlz29yH7zo-7Yxe1P8dAA7ylIwBatMOwf9xJbukjg&numbers=9014339577&sender=JTSIOT&message=Alert:%20%20mqtt%20Connection%20Disconnected%20"
        url+=str(time1)
        print time1
        serialized_data = urllib2.urlopen(url).read()
        
######################### mqtt methods ####################################
mqttc = mqtt.Client()
########################### version 2 API's ##############################

mqttc.message_callback_add('jts/alertTest/mqtt',alert)
mqttc.on_publish = on_publish
mqttc.username_pw_set('esp', 'ptlesp01')
mqttc.connect("cld003.jts-prod.in", 1883, 60)
mqttc.on_disconnect = on_disconnect
mqttc.on_connect = on_connect
mqttc.subscribe("jts/alertTest/mqtt")
mqttc.publish('jts/alertTest/mqtt','hello this is mqtt test program')
mqttc.loop_forever()
#s.enter(3, 1, DB_Alert, (s,))
#s.enter(3, 1, Alert_program, (s,))
#s.run()




