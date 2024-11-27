import serial
import struct
import socketserver
#import crc16
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



CRC16_XMODEM_TABLE = [
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
    0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
    0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
    0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
    0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
    0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
    0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
    0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
    0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
    0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
    0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
    0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
    0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
    0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
    0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
    0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
    0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
    0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
    0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
    0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
    0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
    0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
]



def crc16xmodem(data, crc=0):
    """Calculates the CRC-CCITT (XModem) variant of the CRC16 checksum.
    Parameters
    ----------
    data : bytes
        The data used for calculating the CRC checksum.
    crc : int, optional
        The initial value.
    Returns
    -------
    crc : int
        The calculated CRC16-XModem checksum.
    References
    ----------
    https://reveng.sourceforge.io/crc-catalogue/all.htm#crc.cat.crc-16-xmodem
    """
    for byte in data:
        crc = ((crc << 8) & 0xFF00) ^ CRC16_XMODEM_TABLE[((crc >> 8) & 0xFF) ^ byte]
    return crc & 0xFFFF



def decode_CTRL20(pkt):
	if pkt['payload'] is None:
		return False
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
#	crc_calc = crc16.crc16xmodem(packet)
	crc_calc = crc16xmodem(packet)
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
#	crc_calc = crc16.crc16xmodem(packet)
	crc_calc = crc16xmodem(packet)
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

	client.connect("localhost", 1883, 60)

	client.loop_start()

	while True:
		try:
			update_stats(client)
	#		server.handle_request()
			time.sleep(1)
		except serial.SerialException as e:
			client.loop_stop(force=True)
			sys.exit()
#		except Exception as e:
#			print (str(e))
		except KeyboardInterrupt:
			raise
	client.loop_stop(force=False)





