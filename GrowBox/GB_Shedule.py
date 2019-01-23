from datetime import datetime
from datetime import timedelta
import MySQLdb
import json
#import datetime
import sys
import time
import requests
import string
import random
import urllib2
import schedule
import paho.mqtt.client as mqtt
###########################################################
def data_publish(topic,msg):
    mqttc=mqtt.Client()
    mqttc.username_pw_set('esp','ptlesp01')
    mqttc.connect("cld003.jts-prod.in",1883,60)
    mqttc.loop_start()
    mqttc.publish(topic,msg)
    mqttc.disconnect()
    mqttc.loop_stop()


############################ power savings dayonce ################################
def Switch_on_off():
    msg='toggle'
    topic='GsmClientTest/led'
    data_publish(topic,msg)

schedule.every().day.at("05:00").do(Switch_on_off)
schedule.every().day.at("18:00").do(Switch_on_off)
while True:
    #print "inside true"
    schedule.run_pending()
    time.sleep(1)
####################################################

