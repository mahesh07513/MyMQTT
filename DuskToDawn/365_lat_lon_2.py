import paho.mqtt.client as mqtt
import json
import datetime
import requests

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("jts/dtd/Request")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    try:
        data1 = json.loads(msg.payload)
        print data1
        if((data1.get('macid') is None) or ((data1.get('macid') is not  None) and (len(data1['macid']) <= 0))):
                output_str = " macid is mandatory"
                output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str)
                return mqttc.publish('jts/oyo/response',output)
        else:
                macid = data1['macid']
        d1 = datetime.date(2018, 1, 1)
        d2 = datetime.date(2018, 12, 31)
        days = [d1 + datetime.timedelta(days=x) for x in range((d2-d1).days + 1)]
        for day in days:
            date = day.strftime('%Y-%m-%d')
            day = day.strftime('%m-%d')
            
            #print(day)
            r = requests.get('http://api.sunrise-sunset.org/json?lat=17.440081&lng=78.348915&date=%s' %(date))
            res = r.text
            res = json.loads(res)
            sunrise = res['results']['sunrise']
            sunset = res['results']['sunset']
            ctb = res['results']['civil_twilight_begin']
            cte = res['results']['civil_twilight_end']
            output = '{"date":"%s","sr":"%s","ss":"%s","ctb":"%s","cte":"%s"}'%(day,sunrise,sunset,ctb,cte)
            print output
            client.publish('jts/dtd/'+macid,output,qos=0)
    except ValueError:
        output_str = " failed is mandatory"
        output = '{"error_code":"2", "error_desc": "Response=%s"}' %(output_str)
        return mqttc.publish('jts/oyo/response',output)

def on_publish(client, userdata, result):
        print "data published \n"

client = mqtt.Client()
client.on_connect = on_connect
client.on_publish= on_publish
client.on_message = on_message
client.username_pw_set('esp', 'ptlesp01')
client.connect("cld003.jts-prod.in", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
