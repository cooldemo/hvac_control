import serial
import MySQLdb
import time
import datetime
import json
import urllib3
import urllib
import re
import paho.mqtt.client as mqtt

ser = serial.Serial(    port='/dev/serial/by-id/usb-Prolific_Technology_Inc._USB-Serial_Controller_D-if00-port0',    baudrate=2400,        timeout=0.1)


MQTT_TOPIC = "emon/solar1"

charging_enabled = -1
ac_charging_current = -1
em_current_power = -1

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
	print("Connected with result code "+str(rc))

        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
	client.subscribe(MQTT_TOPIC + "/charging_enabled")
	client.subscribe(MQTT_TOPIC + "/ac_charging_current")
	client.subscribe("emon/emeter1/PTotal")

        # The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
	global charging_enabled, ac_charging_current, em_current_power
	print(msg.topic+" "+str(msg.payload.decode("utf-8")) + " type=")
	if re.search(MQTT_TOPIC + "/charging_enabled", msg.topic):
		data = int(msg.payload.decode("utf-8"))
#               print(data)
		charging_enabled = data
	elif re.search(MQTT_TOPIC + "/ac_charging_current", msg.topic):
		data = int(msg.payload.decode("utf-8"))
#               print(data)
		ac_charging_current = data
	elif re.search("emon/emeter1/PTotal", msg.topic):
		data = float(msg.payload.decode("utf-8"))
#               print(data)
		em_current_power = int(data)



#print("connected to: " + ser.portstr)

def postJSON(postData):
	try:
		print("Post JSON")
		values = {'node' : 'solar',
		'apikey' : '242a69f08e94a13de318b8605babed1f' }
          
		values['fulljson'] = json.dumps(postData)
		postdata = urllib.urlencode(values)
		print (postdata)
		reqjson = "http://192.168.0.2/emoncms/input/post"
		req = urllib2.urlopen(reqjson, postdata)
	#print req.info()
		print (req.read())
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


def query(cmd, addCRC = False):
	if addCRC:
		fullcmd = '^P'.encode()+str(3+len(cmd+b'\r')).zfill(3).encode()+cmd
		fullcmd += ("%03d" % (sumStr(fullcmd))).encode()
		print (fullcmd)
	else:
		fullcmd = '^P'.encode()+str(len(cmd+b'\r')).zfill(3).encode()+cmd
		print (fullcmd)
	ser.write(fullcmd+b'\r')
	resp = ser.readline()
	print (cmd+":\t".encode()+resp)
	return resp

def set(cmd, addCRC = False):
	if addCRC:
		fullcmd = b'^S'+str(3+len(cmd+b'\r')).zfill(3).encode()+cmd
		fullcmd += "%03d" % (sumStr(fullcmd))
		print (fullcmd)
	else:
		fullcmd = b'^S'+str(len(cmd+b'\r')).zfill(3).encode()+cmd
		print (fullcmd)
	ser.write(fullcmd+b'\r')
	resp = ser.readline()
	print (cmd+b':\t'+resp)
	return resp

def sumStr(string):
	suma = 0
	for c in string:
#		suma += ord(c)
		suma += int(c)
	return suma & 0xFF


#def queryEnergyYear(year):
#    cmd = "EY" + str(year)
#    print cmd
#    sum = sum

def splitMsg(msg):
#	print(msg)
	msg1 = msg[5:-3]
#	print(msg1)
#	print(msg1.split(b','))
	return msg1.split(b',')

def fieldToInt(field):
	try:
		num = field.replace(b'\'', b'')
		return int(num)
		
	except:
		print("parse failed for %s" % field)
		return 0
    

def filterParams(dic, params):
	for x in params:
		filterParam(dic, x)
#	print filterParam.filterVals

def filterParam(dic, param):
	if not hasattr(filterParam, "filterVals"):
		filterParam.filterVals = {}
	if not param in filterParam.filterVals:
		filterParam.filterVals[param] = dic[param]
	else:
		dic[param] = (filterParam.filterVals[param]*8 +  dic[param]*2)/10
		filterParam.filterVals[param] = dic[param]

def parseFields(dic, fields, cmd):
	print ("Parsing commnand: %s" % cmd)
	if cmd == b'GS':
		dic["PVvolt1"] = fieldToInt(fields[0]) / 10.0
		dic["PVcurr1"] = fieldToInt(fields[2]) / 100.0
		dic["PVvolt2"] = fieldToInt(fields[1]) / 10.0
		dic["PVcurr2"] = fieldToInt(fields[3]) / 100.0
		dic["BATvolt"] = fieldToInt(fields[4]) / 10.0
		dic["BATperc"] = fieldToInt(fields[5]) 
		dic["BATcurr"] = fieldToInt(fields[6]) / 10.0
		dic["ACvoltR"] = fieldToInt(fields[7]) / 10.0
		dic["ACvoltS"] = fieldToInt(fields[8]) / 10.0
		dic["ACvoltT"] = fieldToInt(fields[9]) / 10.0
		dic["ACfreq"] = fieldToInt(fields[10]) / 100.0
		dic["AOvoltR"] = fieldToInt(fields[14]) / 10.0
		dic["AOvoltS"] = fieldToInt(fields[15]) / 10.0
		dic["AOvoltT"] = fieldToInt(fields[16]) / 10.0
		dic["AOfreq"] = fieldToInt(fields[17]) / 100.0
		dic["temp1"] = fieldToInt(fields[21]) 
		dic["temp2"] = fieldToInt(fields[22]) 
	if cmd == b'PS':
		dic["PVpwr1"] = fieldToInt(fields[0]) / 1.0
		dic["PVpwr2"] = fieldToInt(fields[1]) / 1.0
		dic["AOactpR"] = fieldToInt(fields[7]) / 1.0
		dic["AOactpS"] = fieldToInt(fields[8]) / 1.0
		dic["AOactpT"] = fieldToInt(fields[9]) / 1.0
		dic["AOactpo"] = fieldToInt(fields[10]) / 1.0
		dic["AOaparR"] = fieldToInt(fields[11]) / 1.0
		dic["AOaparS"] = fieldToInt(fields[12]) / 1.0
		dic["AOaparT"] = fieldToInt(fields[13]) / 1.0
	if cmd == b'ET':
		dic["EnergyTotal"] = fieldToInt(fields[0]) 
	if cmd == b'ED':
		dic["EnergyDay"] = fieldToInt(fields[0]) 


#def EMset(power):
#	if power >= 0:
#		print splitMsg(set("REMINFO%05d,%05d,1,%05d" % (0,0,power)))
#	else:
#		print splitMsg(set("REMINFO%05d,%05d,0,%05d" % (0,-power,-power)))

#def EMset(power):
#	if power >= 0:
#		print (splitMsg(set("EMINFO%05d,%05d,1,%05d" % (0,0,power))))
#	else:
#		print (splitMsg(set("EMINFO%05d,%05d,0,%05d" % (0,-power,-power))))

def batCharging(state):
	if state:
		print (splitMsg(set(b'ACCB1,0576' )))
		print (splitMsg(set(b'EDB1' )))
	else:
#		print splitMsg(set(b'ACCB1,0500' ))
		print (splitMsg(set(b'EDB0' )))
	

testMsgGS="^D1104542,0000,0079,0000,0000,000,+00000,2298,2172,2129,5002,0000,0000,0000,0000,0000,0000,0000,,,,040,041,000,0___"
testMsgPS="^D07700363,00000,,,,,,0000,0000,0000,00000,0000,0000,0000,00000,000,0,1,0,0,2,2___"
cmds = [
b'PIGS',
b'MPPTV',
]

cmds2 = [
b'INGS',
b'GS',
b'CFS',
b'MOD',
b'T',
b'WS',
]


cmds1 = [
b'AAPF',
b'FPADJ',
b'INGS',
b'EMINFO',
b'HECS',
b'MAR',
b'BATS',
b'DI',
b'GOF',
b'GOV',
b'FLAG',
b'WS',
b'PS',
b'GS',
b'PIRI',
]

allcmds = [
b'AAPF',
b'FPADJ',
b'INGS',
b'EMINFO',
b'TEST',
b'VFWT',
b'FPPF',
b'ACLT',
b'FT',
b'FET',
b'GLTHV',
b'HECS',
b'CFS',
b'MAR',
b'DM',
b'BATS',
b'DI',
b'LST',
b'SV',
b'MPPTV',
b'GPMP',
b'OPMP',
b'GOF',
b'GOV',
b'ET',
b'T',
b'FLAG',
b'WS',
b'MOD',
b'PS',
b'GS',
b'PIRI',
b'MD',
b'PI',
b'ID'
]



#print splitMsg(set("MPPTLV3000"))
#print splitMsg(query("MPPTV"))
#print splitMsg(set("GPMP001000"))
print (splitMsg(query(b'T')))
#print splitMsg(query("GPMP"))
print (splitMsg(query(b'HECS')))
#print (splitMsg(set(b'ED'+chr(0)+chr(0))))
print (splitMsg(query(b'HECS')))
print (splitMsg(query(b'EMINFO')))

#EMset(-500)
#batCharging(False)

# 101,1,0,1,1,1,0,0,0
#print splitMsg(query("LDPR-00120"))
#print splitMsg(query("ED"+datetime.date.today().strftime("%Y%m%d"), True))
# set current time
print (splitMsg(set(b'DAT'+datetime.datetime.now().strftime("%y%m%d%H%M%S").encode(), False)))
#exit()
itr = 0

batteryWh = 0.0
batteryAh = -10000.0
lastBatteryWhTime = time.time()
batteryWhResetTimer = 0

mqttc = mqtt.Client()
mqttc.on_connect = on_connect
mqttc.on_message = on_message

mqttc.connect("192.168.0.2", 1883, 60)

mqttc.loop_start()


while True:
#    for cmd in allcmds:
#	query(cmd)
#    print('----\n')
#    
#  


	try:
#    if True:
		time.sleep(4)

		dic = {}
		parseFields(dic, splitMsg(query(b'GS')), b'GS')
		parseFields(dic, splitMsg(query(b'PS')), b'PS')
#	print ("before filter:")
#	print dic

		deltaTime = time.time() - lastBatteryWhTime
		lastBatteryWhTime = time.time()
		batteryWh += dic["BATvolt"] * dic["BATcurr"] * deltaTime / 3600
		batteryAh += dic["BATcurr"] * deltaTime / 3600
		batteryWh -= dic["BATvolt"] * deltaTime / 3600 # internal inverter discharge correction
		batteryAh -= deltaTime / 3600

	# estimate battery SOC
		if batteryAh < -5000:
			voltage = dic["BATvolt"] - dic["BATcurr"] / 100
			if voltage >= 57.6:
				batteryAh = 0
			elif voltage >= 56.0:
				batteryAh = -20
			elif voltage >= 54.0:
				batteryAh = -60
			elif voltage >= 53.0:
				batteryAh = -80
			elif voltage >= 52.0:
				batteryAh = -150
			elif voltage >= 51.5:
				batteryAh = -320
			else:
				batteryAh = -400

		if dic["BATvolt"] >= 57.6 and batteryAh > 0:
			batteryAh = -4
			batteryWh = batteryAh * dic["BATvolt"]


		if dic["BATvolt"] >= 57.6 and dic["BATcurr"] < 10.0 and dic["BATcurr"] >= 0.0:
			batteryWhResetTimer += 1
		else:
			batteryWhResetTimer = 0
		if batteryWhResetTimer > 10:
			batteryWhResetTimer = 10
			batteryWh = 0
			batteryAh = 0
	
	
	
		print ("deltaTime %d , energyWh = %f\n" % (deltaTime, batteryWh))

		dict2 = {}
		dict2['PVpwr1_realtime'] = dic['PVpwr1']
		dict2['PVpwr2_realtime'] = dic['PVpwr2']
		dict2['AOactpo_realtime'] = dic['AOactpo']
		dict2['BATvolt_realtime'] = dic['BATvolt']
		dict2['BATcurr_realtime'] = dic['BATcurr']
		
		filterParams(dic, ["PVvolt1","PVcurr1","PVvolt2","PVcurr2","BATvolt","BATcurr","ACvoltR","ACvoltS","ACvoltT","ACfreq","AOvoltR","AOvoltS","AOvoltT","AOfreq","temp1","temp2","PVpwr1","PVpwr2","AOactpR","AOactpS","AOactpT","AOactpo","AOaparR","AOaparS","AOaparT"])
#	print ("after filter:")
#        print dic

	

		itr += 1
#	postJSON(dict2)
		postMQTT(dict2)

		print ("charging_enabled %d" % charging_enabled )
		if charging_enabled == 1:
			print ("start charging_enabled %d" % charging_enabled )
			batCharging(True)
			charging_enabled = -1
		elif charging_enabled == 0:
			print( "stop charging_enabled %d" % charging_enabled )
			batCharging(False)
			charging_enabled = -1
	
#			if em_current_power != 65535:
#		EMset(em_current_power)
#		em_current_power = 65535
	

		if itr % 5 != 0:
			continue
		parseFields(dic, splitMsg(query(b'ET')), b'ET')
		parseFields(dic, splitMsg(query(b'ED'+datetime.date.today().strftime("%Y%m%d").encode(), True)), b'ED')
		dic["AhBatt"] = batteryAh
		dic["energyBatt"] = batteryWh
	
		q = "INSERT INTO mppsolar SET time = NOW(), "
		for key, value in dic.items():
			q += " `%s` = '%s'," % (key, str(value))

		q = q[:-1] + ";"


		print (q)
		try:
			db = MySQLdb.connect(host="192.168.1.1", user="demo", passwd="aUbBpW7M633NMQLD", db="demo_energo")
			c = db.cursor()
			c.execute(q)
			c.close()
			db.close()
		except Exception as e:
			print(e)
			print("DB error")

#	postJSON(dic)
		postMQTT(dic)

#	time.sleep(55)
	except Exception as e:
		print(e)
		time.sleep(60)


ser.close()















