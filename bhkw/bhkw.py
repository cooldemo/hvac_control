import serial
import struct
import socketserver
import crc16
import time
import paho.mqtt.client as mqtt
import sys


#a = b'\x00\x70\x06\x93\xc1\x74'
#b = b'\x00\x60\x06\x23\x25\xcc'
#c = b'\x00\x70\x08\xd1\x01\x00\xf9\xd0'
a = b'\x00\x70\x06\x93\xc1\x74'
b = b'\x00\x60\x06\x23\x25\xcc'
c = b'\x00\x70\x08\xd1\x01\x00\xf9\xd0'
ser = serial.Serial('/dev/serial/by-id/usb-Twin_Auto_Twin_-_universal_converter_3038KAOV-if00-port0', baudrate=9600, timeout=0.2)


MQTT_TOPIC = "emon/bhkw1"

MIN_RPM = 1400
MAX_RPM = 3000

request_power = -1
last_request = 0

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
        print("Connected with result code "+str(rc))

        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        client.subscribe(MQTT_TOPIC + "/request_power")

        # The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
        global request_power,last_request
        print(msg.topic+" "+str(msg.payload.decode("utf-8")) + " type=")
        request_power = int(msg.payload.decode("ascii"))*1
        last_request = time.time()


HOST, PORT = "192.168.1.77", 502


stat = {}

MAX_AGE = 5


ids = {}
ids['CTRL'] = b'\x70'
ids['INVT'] = b'\x60'
ids['MOTR'] = b'\x50'


def decode_CTRL20(pkt):
	if len(pkt['payload']) != 59:
		return False
	payload = pkt['payload']
	pkt['data']['rpm_request'] = struct.unpack('>H', payload[12:14])[0]
	pkt['data']['rpm2'] = struct.unpack('>H', payload[19:21])[0]
	pkt['data']['rpm_actual'] = struct.unpack('>H', payload[21:23])[0]
	pkt['data']['rpm4'] = struct.unpack('>H', payload[31:33])[0]
	pkt['data']['coolant'] = struct.unpack('>B', payload[33:34])[0]
	pkt['data']['buffer_middle'] = struct.unpack('>H', payload[42:44])[0]/10
	pkt['data']['buffer_top'] = struct.unpack('>H', payload[44:46])[0]/10
	pkt['data']['t_return'] = struct.unpack('>H', payload[48:50])[0]/10
	pkt['data']['el_power'] = struct.unpack('>H', payload[23:25])[0]
	pkt['data']['th_power'] = struct.unpack('>H', payload[54:56])[0]
	if payload[4] == 1:
		pkt['data']['mode'] = b'AUTO'
	elif payload[4] == 3:
		pkt['data']['mode'] = b'RS232'
	else:
		 pkt['data']['mode'] = b'UNKN'
	print(pkt['data'])
	return True

def writereadT_rs232_packet(tup):
	packet = b'\x00'
	packet += ids[tup[0]]
	packet += struct.pack('>B', len(tup[1]) + 5)
	packet += tup[1]
	crc_calc = crc16.crc16xmodem(packet)
	packet += struct.pack('>H', crc_calc)
	print(get_target(packet[1]), "\t", end = ''),
	print_in_hex(packet[3::][:-2])
	ser.write(packet)
	ret = read_rs232_packet()
	print_in_hex(ret)
	if ret != None and get_target(packet[1]) != "----":
		stat[(get_target(packet[1]), packet[3::][:-2])] = ret
	return ret

def update_stats(mqttc):
	global last_request,request_power
	for s in stats:
		if s['last_time'] + s['max_age'] < time.time():
			s['last_time'] = time.time()
			ret = writereadT_rs232_packet(s['packet'])
			s['payload'] = ret
			if(s['decode'](s)):
				s['last_time'] = time.time()
				for key in s['data']:
#					print(s['data'][key])
					mqttc.publish(MQTT_TOPIC+"/"+key,s['data'][key])
	set_RS232()
	
	if time.time() > last_request + 60 and request_power > 1700:
		print("Request_power too old, BHKW min power ")
		request_power = 1700
		last_request = time.time()
	elif time.time() > last_request + 60 and request_power == 1700:
		print("Request_power too old, BHKW shutdown ")
		request_power = 0
		last_request = time.time()
	print("Request_power = ", request_power)
	current_power = s['data']['el_power']
	if request_power == 0 and current_power != 0:
		set_rpm(0)
		print("Adjust RPM to 0 ")
	elif request_power > 0:
		rpm_request = s['data']['rpm_request']
		if current_power < request_power-45:
			next_request = rpm_request + 25
			if next_request > MAX_RPM:
				next_request = MAX_RPM
			if next_request < MIN_RPM:
				next_request = MIN_RPM
			print("Adjust RPM from ", rpm_request, " to ", next_request)
			set_rpm(next_request)
		elif request_power <= 1700:
			next_request = MIN_RPM
			if next_request > MAX_RPM:
				next_request = MAX_RPM
			if next_request < MIN_RPM:
				next_request = MIN_RPM
			print("Adjust RPM from ", rpm_request, " to ", next_request)
			set_rpm(next_request)
		elif current_power > request_power+450:
			next_request = rpm_request - 300
			if next_request > MAX_RPM:
				next_request = MAX_RPM
			if next_request < MIN_RPM:
				next_request = MIN_RPM
			print("Adjust RPM from ", rpm_request, " to ", next_request)
			set_rpm(next_request)
		elif current_power > request_power+200:
			next_request = rpm_request - 150
			if next_request > MAX_RPM:
				next_request = MAX_RPM
			if next_request < MIN_RPM:
				next_request = MIN_RPM
			print("Adjust RPM from ", rpm_request, " to ", next_request)
			set_rpm(next_request)
		elif current_power > request_power+75:
			next_request = rpm_request - 50
			if next_request > MAX_RPM:
				next_request = MAX_RPM
			if next_request < MIN_RPM:
				next_request = MIN_RPM
			print("Adjust RPM from ", rpm_request, " to ", next_request)
			set_rpm(next_request)

def set_rpm(rpm_req):
	packet = b'\x30'
	packet += struct.pack('>H', rpm_req)
	ret = writereadT_rs232_packet(('CTRL', packet))
	
def set_RS232():
	packet = b'\x90\x03\x01'
	ret = writereadT_rs232_packet(('CTRL', packet))
	

def read_rs232_packet():
	c = ser.read()
	if  c != b'\x00':
#		print("Not recv 00")
#		print(c)
		return None
	id = ser.read()
	if  id != b'\x50' and id != b'\x60' and id != b'\x70' :
		print("Not recv ID")
		print(id)
		return None
	len = struct.unpack('>B', ser.read())[0]
	if  len <= 2 :
		print("Len too short")
		return None
	packet = b'\x00'
	packet += id
	packet += struct.pack('>B', len)
	packet += ser.read(len-5)
	crc_recv = struct.unpack('>H', ser.read(2))[0]
	crc_calc = crc16.crc16xmodem(packet)
	if  crc_recv != crc_calc :
		print("CRC fail")
		print(crc_recv, " ", crc_calc)
		return None
#	print_in_hex(packet[3:])
	return packet+struct.pack('>H', crc_calc)

def print_in_hex(by):
	len = 20
	res = ""
	if by is None:
		return
	for b in by:
		res += "%02x" % b
		len -= 1
	for x in range(len):
		res += "  " 
	print(res, end = ''),

def get_target(t):
	if t == 0x70:
		return "CTRL"
	elif t == 0x60:
		return "INVT"
	elif t == 0x50:
		return "MOTR"
	return "----"
	


def print_data():
	print("*********************")
	print(chr(27)+'[2j')
	print('\033c')
	print('\x1bc')
	for key in sorted(stat.keys()) :
		print(key[0], end = '')
		print_in_hex(key[1])
		print_in_hex(stat[key][3:-2])
		print("")

#writeread_rs232_packet(a)
#writeread_rs232_packet(b)
#writeread_rs232_packet(c)

def writeread_rs232_packet(p):
	ser.write(p)
	print(get_target(p[1]), "\t", end = ''),
	print_in_hex(p[3::][:-2])
	ret = read_rs232_packet()
	print("")
	
	if ret != None and get_target(p[1]) != "----":
		stat[(get_target(p[1]), p[3::][:-2])] = ret
	print_data()
	return ret

class MyTCPHandler(socketserver.BaseRequestHandler):
    """
    The RequestHandler class for our server.

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.
    """
    timeout = 1
    def handle(self):
        # self.request is the TCP socket connected to the client
        while True:
            self.data = self.request.recv(1024).strip()
#            print("{} wrote:".format(self.client_address[0]))
#            print(self.data)
            # just send back the same data, but upper-cased
            ret = writeread_rs232_packet(self.data)
            if ret is not None:
#                print("data sending", ret)
                self.request.sendall(ret)
#            else:
#                print("data empty")
#            self.request.sendall('')


stats = []
d = {}
d['packet'] = ('CTRL', b'\x20')
d['decode'] = decode_CTRL20
d['last_time'] = 0
d['max_age'] = MAX_AGE
d['payload'] = None
d['data'] = {}
stats.append(d)



if __name__ == "__main__":
	HOST, PORT = "192.168.0.2", 9876

    # Create the server, binding to localhost on port 9999
	server = socketserver.TCPServer((HOST, PORT), MyTCPHandler)
#	socketserver.request.settimeout(1)
    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    #server.serve_forever()
	server.timeout = 1

	client = mqtt.Client()
	client.on_connect = on_connect
	client.on_message = on_message

	client.connect("192.168.0.2", 1883, 60)

	client.loop_start()

	while True:
		try:
			update_stats(client)
	#		server.handle_request()
			time.sleep(1)
		except serial.SerialException as e:
			client.loop_stop(force=True)
			sys.exit()
		except Exception as e:
			print (str(e))
		except KeyboardInterrupt:
			raise
	client.loop_stop(force=False)





