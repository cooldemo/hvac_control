import os
import xml.etree.ElementTree as ET
import dvh5x
import json
import urllib
import urllib3
import MySQLdb
import time
import paho.mqtt.client as mqtt
import sys

MQTT_TOPIC = "emon/emeter1"

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
	print("Connected with result code "+str(rc))

        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
#        client.subscribe(MQTT_TOPIC + "/request_power")

        # The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
	print(msg.topic+" "+str(msg.payload.decode("utf-8")) + " type=")
#        request_power = int(msg.payload.decode("ascii"))*1


def postMysql(dic):
	q = "INSERT INTO grid SET time = NOW(), "
	for key, value in dic.items():
		q += " `%s` = '%s'," % (key, str(value))

	q = q[:-1] + ";"


#	print(q)
#	try:
#		_db = MySQLdb.connect(host="192.168.1.1", user="demo", passwd="aUbBpW7M633NMQLD", db="demo_energo", connect_timeout = 3)
#		c = _db.cursor()
#		c.execute(q)
#		c.close()
#		_db.close()
#	except Exception as e:
#		print(e)
#		print("DB error")

	postJSON(dic)


def postJSON(postData):
	try:
		print("Post JSON")
		values = {'node' : 'e-meter',
		'apikey' : '242a69f08e94a13de318b8605babed1f' }

		values['fulljson'] = json.dumps(postData)
		postdata = urllib.parse.urlencode(values)
		print(postdata)
		reqjson = "http://192.168.0.2/emoncms/input/post"
		req = urllib.request.urlopen(reqjson, postdata.encode())
		#print req.info()
		print(req.read())
		req.close()
	except Exception as e:
		print(e)
		print("JSON error")

def postMQTT(postData):
	try:
		for key in postData:
			mqttc.publish(MQTT_TOPIC+"/"+key,postData[key])
		
	except Exception as e:
		print(e)
		print("MQTT error")


def getValue(mbusdata, text):
	for ble in mbusdata.body.interpreted['records']:
#	print ble
		if ble['type'] == text:
			return float(ble['value'])
    
    # <Unit>Voltage instantaneous phase 1</Unit>
	a = root.findall(".//*[Unit='"+text+"']")
	a = a[0] #.Element('Value')
	ret = a.find('Value').text
	print ("Search ", text, " - found ", ret)
	return ret


def procMysql(mbusdata):
	dicMySQL = {}
	cosphi = float(getValue(mbusdata, 'Power factor instantaneous'))
	dicMySQL["cosPhi"] = cosphi

	volt1 = getValue(mbusdata, 'Voltage instantaneous phase 1')
	volt2 = getValue(mbusdata, 'Voltage instantaneous phase 2')
	volt3 = getValue(mbusdata, 'Voltage instantaneous phase 3')
	curr1 = getValue(mbusdata, 'Current instantaneous phase 1')
	curr2 = getValue(mbusdata, 'Current instantaneous phase 2')
	curr3 = getValue(mbusdata, 'Current instantaneous phase 3')
	powIn = getValue(mbusdata, 'Power active import P+ instantaneous')
	powOut = getValue(mbusdata, 'Power active import P- instantaneous')
	dicMySQL['P1'] = cosphi*volt1*curr1/1000
	dicMySQL['P2'] = cosphi*volt2*curr2/1000
	dicMySQL['P3'] = cosphi*volt3*curr3/1000
	dicMySQL['PTotal'] = (powIn - powOut)/1000
	dicMySQL['QIn1'] = getValue(mbusdata, 'Index cumulative P+ tariff 1 (Wh)')/1000
	dicMySQL['QIn2'] = getValue(mbusdata, 'Index cumulative P+ tariff 2 (Wh)')/1000
	dicMySQL["QInTotal"] = dicMySQL['QIn2'] + dicMySQL['QIn1']
	dicMySQL['QOut1'] = getValue(mbusdata, 'Index cumulative P- tariff 1 (Wh)')/1000
	dicMySQL['QOut2'] = getValue(mbusdata, 'Index cumulative P- tariff 2 (Wh)')/1000
	dicMySQL["QOutTotal"] = dicMySQL['QOut2'] + dicMySQL['QOut1']

	postMysql(dicMySQL)

def procJSON(mbusdata):
	dic = {}
	cosphi = float(getValue(mbusdata, 'Power factor instantaneous'))
	dic["cosPhi"] = cosphi
	volt1 = getValue(mbusdata, 'Voltage instantaneous phase 1')
	volt2 = getValue(mbusdata, 'Voltage instantaneous phase 2')
	volt3 = getValue(mbusdata, 'Voltage instantaneous phase 3')
	curr1 = getValue(mbusdata, 'Current instantaneous phase 1')
	curr2 = getValue(mbusdata, 'Current instantaneous phase 2')
	curr3 = getValue(mbusdata, 'Current instantaneous phase 3')
	powIn = getValue(mbusdata, 'Power active import P+ instantaneous')
	powOut = getValue(mbusdata, 'Power active import P- instantaneous')
    
	dic['P1'] = cosphi*volt1*curr1
	dic['P2'] = cosphi*volt2*curr2
	dic['P3'] = cosphi*volt3*curr3
	dic['PTotal'] = powIn - powOut

	dic['QIn1'] = getValue(mbusdata, 'Index cumulative P+ tariff 1 (Wh)')/1000
	dic['QIn2'] = getValue(mbusdata, 'Index cumulative P+ tariff 2 (Wh)')/1000
	dic["QInTotal"] = dic['QIn2'] + dic['QIn1']
	dic['QOut1'] = getValue(mbusdata, 'Index cumulative P- tariff 1 (Wh)')/1000
	dic['QOut2'] = getValue(mbusdata, 'Index cumulative P- tariff 2 (Wh)')/1000
	dic["QOutTotal"] = dic['QOut1'] + dic['QOut2']
	print(dic)
#    postJSON(dic)
	postMQTT(dic)


i = 0
mqttc = mqtt.Client()
mqttc.on_connect = on_connect
mqttc.on_message = on_message
mqttc.connect("192.168.0.2", 1883, 60)

mqttc.loop_start()

while True:
	try:
		time.sleep(1)
		mbusdata=dvh5x.dvh5x_read(1, '/dev/serial/by-id/usb-Silicon_Labs_CP2102_USB_to_UART_Bridge_Controller_0001-if00-port0', 9600)
	#print("mbusdata: " + mbusdata.to_JSON())
		if mbusdata is not None:
			procJSON(mbusdata)
			i += 1
			if i == 10:
				procMysql(mbusdata)
				i = 0
			time.sleep(3)
		else:
			print("No data received")

	except Exception as e:
	#print "Unexpected error:", sys.exc_info()[0]
		exc_type, exc_obj, exc_tb = sys.exc_info()
		fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
		print("Exception:", exc_type, fname, exc_tb.tb_lineno)        
		time.sleep(0.1)
	


