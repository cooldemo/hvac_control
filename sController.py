# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import serial, struct, socketserver, crc16, time
import paho.mqtt.client as mqtt
import re, datetime, random, requests, json
MQTT_TOPIC = 'emon/+/#'
MQTT_MYTOPIC = 'emon/controller/'
MQTT_WEATHER = 'weather/#'

pow_con = {}
pow_con['sonoff1'] = {}
pow_con['sonoff2'] = {}
pow_con['bhkw1'] = {}
pow_con['bhkw1']['enabled'] = False
pow_con['bhkw1']['history'] = []
pow_con['bhkw1']['request_power'] = 0
pow_con['bhkw1']['warmup'] = 0
pow_con['bhkw1']['coolant'] = 0
pow_con['bhkw2'] = {}
pow_con['solar'] = {}
pow_con['company'] = {}
pow_con['company']['history'] = []

heating = {}
heating['last_bhkw_run'] = 0
heating['last_hp_run'] = 0
heating['WATER_TEMP1'] = 39.0
heating['WATER_TEMP1a'] = 45.0
heating['WATER_TEMP2'] = 49.0
heating['HEATING_TARGET'] = 27
heating['COMP_RUNTIME_SHOT'] = 3600
heating['BHKW_RESTART_TIME'] = 14400
consumers = [
 'sonoff1', 'sonoff2']

BHKW_MAX_POWER = 3700
BHKW_MIN_POWER = 1700

mqtt = {}

def read_mqtt(topic):
    global mqtt
    if topic not in mqtt:
        return {'value' : 0, 'age' : 0}
    else:
        return {'value' : mqtt[topic]['value'], 'age' : (time.time() - mqtt[topic]['age'])}

heat_pump_command = {}
heat_pump_command['pump'] = 0
heat_pump_command['comp'] = 0
heat_pump_command['valve'] = 0

control_state = 'INIT'
heat_state = 'IDLE'

def on_connect(client, userdata, flags, rc):
    print('Connected with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC)
    client.subscribe(MQTT_WEATHER)


def on_message(client, userdata, msg):
    global control_state
    if re.search('emon/sonoff1/Power', msg.topic):
        data = float(msg.payload.decode('utf-8'))
        pow_con['sonoff1']['power'] = data
        pow_con['sonoff1']['age'] = time.time()
    else:
        if re.search('emon/sonoff2/Power', msg.topic):
            data = float(msg.payload.decode('utf-8'))
            pow_con['sonoff2']['power'] = data
            pow_con['sonoff2']['age'] = time.time()
        elif re.search('emon/bhkw1/el_power', msg.topic):
            data = float(msg.payload.decode('utf-8'))
            pow_con['bhkw1']['power'] = -data
            pow_con['bhkw1']['age'] = time.time()
            pow_con['bhkw1']['history'].append((data, time.time()))
            history_cleanup(pow_con['bhkw1']['history'])
        elif re.search('emon/bhkw1/warmup', msg.topic):
            data = float(msg.payload.decode('utf-8'))
            pow_con['bhkw1']['warmup'] = data
        elif re.search('emon/bhkw1/coolant', msg.topic):
            data = float(msg.payload.decode('utf-8'))
            pow_con['bhkw1']['coolant'] = data
        elif re.search('emon/bhkw1/request_power', msg.topic):
            data = int(msg.payload.decode('utf-8'))
            if data == 0:
                pow_con['bhkw1']['enabled'] = False
            else:
                if data > 1700:
                    pow_con['bhkw1']['enabled'] = True
                pow_con['bhkw1']['request_power'] = data
        elif re.search('emon/emeter1/PTotal', msg.topic):
            data = float(msg.payload.decode('utf-8'))
            pow_con['company']['power'] = data
            pow_con['company']['age'] = time.time()
            pow_con['company']['history'].append((data, time.time()))
            history_cleanup(pow_con['company']['history'])
        else:
            mqtt[msg.topic] = {}
            value = float(msg.payload.decode('utf-8'))
            if value.is_integer():
                data = int(msg.payload.decode('utf-8'))
            else:
                data = value
            mqtt[msg.topic]['age'] = time.time()
            mqtt[msg.topic]['value'] = data


def history_cleanup(hst):
    curr_time = time.time()
    for data in hst[:]:
        if curr_time > data[1] + 3600:
            hst.remove(data)


def history_avg(hst, interval):
    curr_time = time.time()
    avg = 0.0
    avg_c = 0.0
    for data in hst:
        if curr_time < data[1] + interval:
            avg += data[0]
            avg_c += 1

    if avg_c > 0:
        return avg / avg_c
    return 0


def publish_data():
    global client
    global heat_state
    client.publish(MQTT_MYTOPIC + 'bhkw_avg1', history_avg(pow_con['bhkw1']['history'], 60))
    client.publish(MQTT_MYTOPIC + 'bhkw_avg5', history_avg(pow_con['bhkw1']['history'], 300))
    client.publish(MQTT_MYTOPIC + 'bhkw_avg15', history_avg(pow_con['bhkw1']['history'], 1500))
    client.publish(MQTT_MYTOPIC + 'company_avg1', history_avg(pow_con['company']['history'], 60))
    client.publish(MQTT_MYTOPIC + 'company_avg5', history_avg(pow_con['company']['history'], 300))
    client.publish(MQTT_MYTOPIC + 'company_avg15', history_avg(pow_con['company']['history'], 1500))
    client.publish(MQTT_MYTOPIC + 'state', control_state)
    client.publish(MQTT_MYTOPIC + 'heat_state', heat_state)
    client.publish(MQTT_MYTOPIC + 'request_start', 0)
    client.publish(MQTT_MYTOPIC + 'request_stop', 0)



def control_machine():
    global control_state, pow_con, heat_state
    if control_machine.loop_delay > 0:
        control_machine.loop_delay -= 1
        return
    next_state = 'IDLE'
    if control_state == 'INIT':
        if pow_con['bhkw1']['power'] > 200:
            pow_con['bhkw1']['enabled'] = True
            next_state = 'BATT_CHARGING'
        else:
            next_state = 'IDLE'
    elif control_state == 'BATT_CHARGING':
        if history_avg(pow_con['company']['history'],60) + history_avg(pow_con['bhkw1']['history'],60)  > 1800:
            next_state = 'BATT_CHARGING'
        else:
            next_state = 'STOP'
    elif control_state == 'START':
            bhkw1_start()
            next_state = 'STARTING'
    elif control_state == 'STOP':
            bhkw1_stop()
            next_state = 'STOPING'
    elif control_state == 'STOPING':
            if pow_con['bhkw1']['power'] > 0:
                next_state = 'STOPING'
            else:
                next_state = 'IDLE'
                solar1_charging_stop()
    elif control_state == 'STARTING':
            if pow_con['bhkw1']['power'] > 0:
                next_state = 'BATT_CHARGING'
                solar1_charging_start()
            else:
                next_state = 'STARTING'
    elif control_state == 'IDLE':
        next_state = 'IDLE'
    else:
        print("Invalid control machine state:" + control_state)
    control_state = next_state
    return

control_machine.loop_delay = 0

def heat_machine():
    mq = read_mqtt('emon/heatpump1/inHot')
    if mq['age'] > 200:
        control_state
    if heat_machine.loop_delay > 0:
        heat_machine.loop_delay -= 1
        return
    next_state = 'IDLE'
    if control_state == 'IDLE':
        heat_pump_command['comp'] = 0
        heat_pump_command['pump'] = 0
        heat_pump_command['valve'] = 0
        next_state = 'IDLE'
    elif control_state == 'START':
        mq = read_mqtt('emon/heatpump1/inHot')
        if mq['age'] > 200:
            next_state = 'IDLE'
    elif control_state == 'HEATING':
        heat_pump_command['comp'] = 0
        heat_pump_command['pump'] = 1
        heat_pump_command['valve'] = 0
        mq = read_mqtt('emon/heatpump1/inHot')
        if mq['age'] > 200 or mq['value'] > heating['HEATING_TARGET']:
            next_state = 'IDLE'
        else:
            next_state = 'HEATING'

    heat_state = next_state
    return

heat_machine.loop_delay = 0
heat_machine.compressor_runtime = 0.0

def heat_pump_update():
    global control_state
    global heat_pump_command
    try:
        print('heat_pump_update()')
        print(heat_pump_command)
        r = requests.get('http://192.168.192.129/remote.htm', params=heat_pump_command, timeout=5)
        heat_pump_update.retry_count = 0
        print('heat_pump_update() .. ok')
    except requests.exceptions.RequestException as e:
        print('heat_pump_update() .. error')
        heat_pump_update.retry_count += 1
        if heat_pump_update.retry_count > 10:
            control_state = 'STOP'


heat_pump_update.retry_count = 0


def save_runtime():
    with open('runtime.json', 'w') as (fp):
        json.dump(heating, fp)


def restore_runtime():
    with open('runtime.json') as (fp):
        heating = json.load(fp)


# def consumerOn():
#     availConsumers = []
#     print('consumerOn')
#     for consumer in consumers:
#         if 'age' in pow_con[consumer] and 'power' in pow_con[consumer] and time.time() - pow_con[consumer][
#             'age'] < 60 and pow_con[consumer]['power'] < 10:
#             availConsumers.append(consumer)
#
#     print('Available consumers: ')
#     print(availConsumers)
#     if not availConsumers:
#         return False
#     turnOn = random.choice(availConsumers)
#     print('TurnOn: ' + turnOn)
#     if turnOn == 'sonoff1':
#         client.publish('cmnd/sonoff1/power', 'on')
#         return True
#     if turnOn == 'sonoff2':
#         client.publish('cmnd/sonoff2/power', 'on')
#         return True
#     return False
#
#
# def consumerOff():
#     availConsumers = []
#     print('consumerOff')
#     for consumer in consumers:
#         if 'age' in pow_con[consumer] and 'power' in pow_con[consumer] and time.time() - pow_con[consumer][
#             'age'] < 60 and pow_con[consumer]['power'] > 100:
#             availConsumers.append(consumer)
#
#     print('Available consumers: ')
#     print(availConsumers)
#     if not availConsumers:
#         return False
#     turnOff = random.choice(availConsumers)
#     print('TurnOff: ' + turnOff)
#     if turnOff == 'sonoff1':
#         client.publish('cmnd/sonoff1/power', 'off')
#         return True
#     if turnOff == 'sonoff2':
#         client.publish('cmnd/sonoff2/power', 'off')
#         return True
#     return False


def incConsum(diff):
    diffAdjust = diff
    print('incConsum')
    print('diffAdjust: ' + str(diffAdjust))
    if 'age' in pow_con['bhkw1']:
        if 'power' in pow_con['bhkw1']:
            print('BHKW1 present')
            if time.time() - pow_con['bhkw1']['age'] < 60 and -pow_con['bhkw1']['power'] - BHKW_MIN_POWER > 0 and -pow_con['bhkw1']['power'] > 1500 and pow_con['bhkw1']['enabled']:
                print('BHKW1 adjustable')
                bhkwDiff = -pow_con['bhkw1']['power'] - BHKW_MIN_POWER
                if diffAdjust > 100:
                    if diffAdjust > bhkwDiff:
                        print('BHKW1 min power')
                        pow_con['bhkw1']['request_power'] = int(BHKW_MIN_POWER)
                        diffAdjust = diffAdjust - bhkwDiff
                    else:
                        print('BHKW1 dec power ' + str(-pow_con['bhkw1']['power'] - diffAdjust))
                        pow_con['bhkw1']['request_power'] = int(-pow_con['bhkw1']['power'] - diffAdjust)
                        diffAdjust = 0
    print('diffAdjust: ' + str(diffAdjust))
#    if diffAdjust > 200:
#        if consumerOn():
#            return


def decConsum(diff):
    diffAdjust = diff
    print('decConsum')
    print('diffAdjust: ' + str(diffAdjust))
#    if diffAdjust > 200:
#        if consumerOff():
#            return
    if 'age' in pow_con['bhkw1']:
        if 'power' in pow_con['bhkw1']:
            print('BHKW1 present')
            if time.time() - pow_con['bhkw1']['age'] < 60 and BHKW_MAX_POWER + pow_con['bhkw1']['power'] > 0 and -pow_con['bhkw1']['power'] > 1700 and pow_con['bhkw1']['enabled']:
                print('BHKW1 adjustable')
                bhkwDiff = BHKW_MAX_POWER + pow_con['bhkw1']['power']
                if diffAdjust > 100:
                    if diffAdjust > bhkwDiff:
                        print('BHKW1 max power')
                        pow_con['bhkw1']['request_power'] = int(BHKW_MAX_POWER)
                        diffAdjust = diffAdjust - bhkwDiff
                    else:
                        print('BHKW1 inc power ' + str(-pow_con['bhkw1']['power'] + diffAdjust))
                        pow_con['bhkw1']['request_power'] = int(-pow_con['bhkw1']['power'] + diffAdjust)
                        diffAdjust = 0



def do_start():
    print('BHKW1 start')
    client.publish('emon/controller/request_state', 'START')


def bhkw1_start():
    print('BHKW1 start')
    client.publish('emon/bhkw1/request_power', '1800')


def bhkw1_stop():
    print('BHKW1 stop')
    client.publish('emon/bhkw1/request_power', '0')


def solar1_charging_stop():
    print('solar1 charging stop')
    client.publish('emon/solar1/charging_enabled', '0')


def solar1_charging_start():
    print('solar1 charging start')
    client.publish('emon/solar1/charging_enabled', '1')


def charger1_charging_start():
    print('charger1 charging start')
    client.publish('emon/chargerType1/rapi/in/$FE')


def calculate():
    print()


client = mqtt.Client()
client.connect('192.168.0.2', 1883, 60)

if __name__ == '__main__':
    client.on_connect = on_connect
    client.on_message = on_message
    client.loop_start()
    time.sleep(10)
    while True:
        try:
            time.sleep(10)
            total = 0
            for s in pow_con:
                if 'age' in pow_con[s] and 'power' in pow_con[s]:
                    print(s + ' age:' + str(time.time() - pow_con[s]['age']) + ' power: ' + str(pow_con[s]['power']))
                    if time.time() - pow_con[s]['age'] < 60:
                        total += pow_con[s]['power']

            if time.time() - pow_con['company']['age'] < 60:
                total = pow_con['company']['power']
            else:
                total = -10000
            print('total: ' + str(total))
            if total < 0:
                incConsum(100 - total)
            else:
                if total > 100:
                    decConsum(total - 100)
                client.publish('emon/bhkw1/request_power', str(int(pow_con['bhkw1']['request_power'])))
                control_machine()
                heat_machine()
                heat_pump_update()
                publish_data()
                save_runtime()



        except Exception as e:
            try:
                print(str(e))
            finally:
                e = None
                del e

