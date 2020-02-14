# Import required libraries
import paho.mqtt.client as mqtt
from sensorDataHandler import sensorDataHandler
import traceback

# MQTT Server Details
MQTT_Broker = ""
MQTT_Port = 1883
Keep_Alive_Interval = 60
MQTT_Topic = "iot/data/#"

#Subscribe to all Sensors at Base Topic
def on_connect(client, userdata, flags, rc):
	if rc == 0:
		print("Connected OK")
		mqttc.subscribe(MQTT_Topic, 0)
	else:
		print("Bad connection, RC = ", rc)


# Pass recieved MQTT data into the processor
def on_message(client, userdata, msg):
	try:
		#print("MQTT Data Recieved")
		print("MQTT Topic: " + msg.topic)
		print("Payload: " + str(msg.payload))
		sensorDataHandler(msg.topic, msg.payload)
	except:
		print("error")
		traceback.print_exc()

#def on_subscribe(mosq, obj, mid, granted_qos):
	#print("Subscribed")
	#pass


mqttc = mqtt.Client()

# Assign event callbacks
mqttc.on_message = on_message
mqttc.on_connect = on_connect
#mqttc.on_subscribe = on_subscribe

# Connect to the MQTT broker
mqttc.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval))

# Keep the MQTT connection looping
mqttc.loop_forever(timeout=30)