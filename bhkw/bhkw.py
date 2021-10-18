#!/usr/bin/env python3

"""
install dependencies on debian:
sudo apt install python3-{serial,paho-mqtt,crcelk}
"""

import serial
import crcelk
import paho.mqtt.client

import struct
import socketserver
import time
import sys
from types import SimpleNamespace as Namespace
import json

config = Namespace(

	#serial_device = '/dev/serial/by-id/usb-Twin_Auto_Twin_-_universal_converter_3038KAOV-if00-port0',
	serial_device = 'auto',

	#serial2tcp_host = "192.168.1.77", serial2tcp_port = 502,
	#serial2tcp_host = "192.168.0.2", serial2tcp_port = 9876,
	serial2tcp_host = "127.0.0.1", serial2tcp_port = 9876,

	# start mqtt server ("broker")
	#   mosquitto -v
	# default port is 1883
	#mqtt_server_host = "192.168.0.2", mqtt_server_port = 1883,
	mqtt_server_host = "127.0.0.1", mqtt_server_port = 1883,

	MQTT_TOPIC = "emon/bhkw1",

	MIN_RPM = 1400,
	MAX_RPM = 3000,

	MAX_AGE = 5, # publish stats every x seconds
)

# global app state
state = Namespace(
	request_power = -1,
	last_request = 0,
	status = dict(),
	stats_list = []
)

def main():

	# connect to machine
	serial_connection = serial.Serial(config.serial_device, baudrate=9600, timeout=0.2)

	# Create the server, binding to localhost on port 9999
	serial2tcp_server = socketserver.TCPServer((config.serial2tcp_host, config.serial2tcp_port), Serial2TCPRequestHandler)
#	socketserver.request.settimeout(1)
		# Activate the server; this will keep running until you
		# interrupt the program with Ctrl-C
		#serial2tcp_server.serve_forever()
	serial2tcp_server.timeout = 1

	mqtt_client = paho.mqtt.client.Client()
	mqtt_client.on_connect = mqtt_client_on_connect
	mqtt_client.on_message = mqtt_client_on_message

	mqtt_client.connect(config.mqtt_server_host, config.mqtt_server_port, 60)

	mqtt_client.loop_start()

	while True:
		try:
			update_stats(mqtt_client)
	#		serial2tcp_server.handle_request()
			time.sleep(1)
		except serial.SerialException as e:
			mqtt_client.loop_stop(force=True)
			sys.exit()
		except Exception as e:
			print (str(e))
		except KeyboardInterrupt:
			raise
	mqtt_client.loop_stop(force=False)



ctrl_code = Namespace(
	get_status = b'\x20',
	set_rpm = b'\x30',
	set_todo1 = b'\x90', # TODO better name
)

byte_of_target = dict(
	CTRL = b'\x70', # "control"
	INVT = b'\x60', # inverter
	MOTR = b'\x50', # motor
)

mode_of_byte = dict()
mode_of_byte[b'\x01'] = b'AUTO'
mode_of_byte[b'\x03'] = b'RS232'

stats_list = [
	Namespace(
		request_body = ('CTRL', ctrl_code.get_status),
		decode_response = decode_status_response,
		last_time = 0,
		max_age = config.MAX_AGE,
		payload = None,
		data = dict(),
	),
	# TODO get more stats from machine?
]

def crcsum(_bytes):
	return crcelk.CRC_XMODEM.calc_bytes(_bytes)

def pretty_json(_object):
	return json.dumps(_object, indent=2, sort_keys=False)

if config.serial_device == 'auto':
	# guess serial device. if only one serial device, use it
	import glob
	usb_terminals = glob.glob('/dev/ttyUSB*', recursive=True)
	if len(usb_terminals) != 1:
		print('error: not found serial device: /dev/ttyUSB0')
		sys.exit(1)
	config.serial_device = usb_terminals[0]
	print('auto-detected serial device: %s' % config.serial_device)
else:
	if not os.path.exists(config.serial_device):
		print('error: not found serial device: %s' % config.serial_device)
		sys.exit(1)

# The callback for when the client receives a CONNACK response from the server.
def mqtt_client_on_connect(client, userdata, flags, rc):
	print("Connected with result code "+str(rc))
	# Subscribing in on_connect() means that if we lose the connection and
	# reconnect then subscriptions will be renewed.
	client.subscribe(config.MQTT_TOPIC + "/request_power")

# The callback for when a PUBLISH message is received from the server.
def mqtt_client_on_message(client, userdata, msg):
	global state
	print(msg.topic+" "+str(msg.payload.decode("utf-8")) + " type=")
	state.request_power = int(msg.payload.decode("ascii"))*1
	state.last_request = time.time()

target_of_byte = dict()
for key, val in byte_of_target.items():
	target_of_byte[val] = key

valid_target_bytes = set(byte_of_target.values())

def mode_of_payload(payload):
	byte = payload[4]
	if byte in mode_of_byte:
		return mode_of_byte[byte]
	print("unknown mode 0x%02x of payload: %s" % (payload[4], hexstr(payload)))
	return b'UNKN'

def decode_status_response(stats):
	if len(stats.payload) != 59:
		print("could not decode payload: %s" % hexstr(stats.payload))
		return False
	u = struct.unpack
	p = payload
	stats.data = dict(
		rpm_request = u('>H', p[12:14])[0],
		rpm2 = u('>H', p[19:21])[0],
		rpm_actual = u('>H', p[21:23])[0],
		rpm4 = u('>H', p[31:33])[0],
		coolant = u('>B', p[33:34])[0],
		buffer_middle = u('>H', p[42:44])[0]/10,
		buffer_top = u('>H', p[44:46])[0]/10,
		t_return = u('>H', p[48:50])[0]/10,
		el_power = u('>H', p[23:25])[0],
		th_power = u('>H', p[54:56])[0],
		mode = mode_of_payload(p),
	)
	print(pretty_json(stats.data))
	return True


def update_stats(mqtt_client):
	global state
	for stats in stats_list:
		if stats.last_time + stats.max_age > time.time():
			continue
		stats.last_time = time.time()
		ret = response_of_request_body(**stats.request_body)
		stats.payload = ret
		if not stats.decode_response(stats):
			continue
		stats.last_time = time.time()
		for key in stats.data:
#					print(stats.data[key])
			mqtt_client.publish(config.MQTT_TOPIC+"/"+key,stats.data[key])
	set_RS232()
	# TODO make this shorter ...
	if time.time() > state.last_request + 60 and state.request_power > 1700:
		print("Request_power too old, BHKW min power ")
		state.request_power = 1700
		state.last_request = time.time()
	elif time.time() > state.last_request + 60 and state.request_power == 1700:
		print("Request_power too old, BHKW shutdown ")
		state.request_power = 0
		state.last_request = time.time()
	print("Request_power = ", state.request_power)
	current_power = stats.data['el_power']
	if state.request_power == 0 and current_power != 0:
		set_rpm(0)
		print("Adjust RPM to 0 ")
	elif state.request_power > 0:
		rpm_request = stats.data['rpm_request']
		if current_power < state.request_power-45:
			next_request = rpm_request + 25
			if next_request > config.MAX_RPM:
				next_request = config.MAX_RPM
			if next_request < config.MIN_RPM:
				next_request = config.MIN_RPM
			print("Adjust RPM from ", rpm_request, " to ", next_request)
			set_rpm(next_request)
		elif state.request_power <= 1700:
			next_request = config.MIN_RPM
			if next_request > config.MAX_RPM:
				next_request = config.MAX_RPM
			if next_request < config.MIN_RPM:
				next_request = config.MIN_RPM
			print("Adjust RPM from ", rpm_request, " to ", next_request)
			set_rpm(next_request)
		elif current_power > state.request_power+450:
			next_request = rpm_request - 300
			if next_request > config.MAX_RPM:
				next_request = config.MAX_RPM
			if next_request < config.MIN_RPM:
				next_request = config.MIN_RPM
			print("Adjust RPM from ", rpm_request, " to ", next_request)
			set_rpm(next_request)
		elif current_power > state.request_power+200:
			next_request = rpm_request - 150
			if next_request > config.MAX_RPM:
				next_request = config.MAX_RPM
			if next_request < config.MIN_RPM:
				next_request = config.MIN_RPM
			print("Adjust RPM from ", rpm_request, " to ", next_request)
			set_rpm(next_request)
		elif current_power > state.request_power+75:
			next_request = rpm_request - 50
			if next_request > config.MAX_RPM:
				next_request = config.MAX_RPM
			if next_request < config.MIN_RPM:
				next_request = config.MIN_RPM
			print("Adjust RPM from ", rpm_request, " to ", next_request)
			set_rpm(next_request)

def set_rpm(rpm_req):
	request_body = ctrl_code.set_rpm + struct.pack('>H', rpm_req)
	ret = response_of_request_body('CTRL', request_body)

def set_RS232():
	request_body = ctrl_code.set_todo1 + b'\x03\x01'
	ret = response_of_request_body('CTRL', request_body)

def get_response():
	c = serial_connection.read()
	if  c != b'\x00':
		print("read: Not recv 00 in first byte: %02x" % c)
		return None
	target_byte = serial_connection.read()
	if not target_byte in valid_target_bytes:
		print("read: Ignore target ID 0x%x" % target_byte)
		return None
	len = struct.unpack('>B', serial_connection.read())[0]
	if  len <= 2 :
		print(f"read: Len {len} too short")
		return None
	request_body = b'\x00'
	request_body += target_byte
	request_body += struct.pack('>B', len)
	request_body += serial_connection.read(len-5)
	crc_recv = struct.unpack('>H', serial_connection.read(2))[0]
	crc_calc = crcsum(request_body)
	if  crc_recv != crc_calc :
		print("read: CRC fail")
		print(crc_recv, " ", crc_calc)
		return None
#	print_in_hex(request_body[3:])
	return request_body+struct.pack('>H', crc_calc)

# get hex string of bytes
def hexstr(_bytes):
	len = 20
	res = ""
	if _bytes is None:
		return
	for b in _bytes:
		res += "%02x" % b
		len -= 1
	for x in range(len):
		res += "  " 
	return res

def print_in_hex(_bytes):
	print(hexstr(_bytes), end='')

def get_target(byte):
	if byte in target_of_byte:
		return target_of_byte[byte]
	return "----"

def print_full_status():
	print("*********************")
	if sys.stdout.isatty():
		# clear terminal screen
		print(chr(27)+'[2j')
		print('\033c')
		print('\x1bc')
	# else: writing to pipe: ./bhkw.py | tee -a output.txt
	for request_id in sorted(state.status.keys()) :
		request_target, request_body = request_id
		print(request_target, end = '')
		print_in_hex(request_body)
		response_body = state.status[request_id][3:-2]
		print_in_hex(response_body)
		print("")

def response_of_request_body(request_target, body_bytes):
	if not request_target in byte_of_target:
		print("error: invalid request_target: %s" % request_target)
	request_bytes = b'\x00'
	request_bytes += byte_of_target[request_target]
	request_bytes += struct.pack('>B', len(body_bytes) + 5)
	request_bytes += body_bytes
	crc_calc = crcsum(request_bytes)
	request_bytes += struct.pack('>H', crc_calc)
	return response_of_request_bytes(request_bytes)

def response_of_request_bytes(request_bytes):
	request_target = get_target(request_bytes[1])
	request_body = request_bytes[3::][:-2]
	print(request_target, "\t", end = '')
	print_in_hex(request_body)
	print("")
	serial_connection.write(request_bytes)
	response_bytes = get_response()
	#if response_bytes != None and request_target != "----": # request_target should always be valid
	if response_bytes != None:
		request_id = (request_target, request_body)
		state.status[request_id] = response_bytes
	print_full_status()
	return response_bytes

class Serial2TCPRequestHandler(socketserver.BaseRequestHandler):
		timeout = 1
		def handle(self):
				# self.request is the TCP socket connected to the client
				while True:
						self.data = self.request.recv(1024).strip()
#            print("{} wrote:".format(self.client_address[0]))
#            print(self.data)
						# just send back the same data, but upper-cased
						response = response_of_request_bytes(self.data)
						if ret is not None:
#                print("data sending", response)
								self.request.sendall(response)
#            else:
#                print("data empty")
#            self.request.sendall('')



if __name__ == "__main__":
	main()
