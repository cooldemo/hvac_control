# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import serial, struct, socketserver, crc16, time
import paho.mqtt.client as mqtt_c
import re, datetime, random, requests, json, math, traceback
import pysolar.solar

MQTT_TOPIC = 'emon/+/#'
MQTT_MYTOPIC = 'emon/controller/'
MQTT_WEATHER = 'weather/#'


latitude = 48.00
longitude = 18.00


pow_con = {}
pow_con['sonoff1'] = {}
pow_con['sonoff2'] = {}
pow_con['bhkw1'] = {}
pow_con['bhkw1']['enabled'] = False
pow_con['bhkw1']['history'] = []
pow_con['bhkw1']['request_power'] = 0
pow_con['bhkw1']['power'] = 0
pow_con['bhkw1']['warmup'] = 0
pow_con['bhkw1']['rpm'] = 0
pow_con['bhkw1']['coolant'] = 0
pow_con['bhkw2'] = {}
pow_con['solar'] = {}
pow_con['company'] = {}
pow_con['company']['history'] = []
pow_con['company']['age'] = 0

heating = {}
heating['last_bhkw_run'] = 0.0
heating['last_hp_run'] = 0.0
heating['WATER_TEMP1'] = 39.0
heating['WATER_TEMP1a'] = 45.0
heating['WATER_TEMP2'] = 49.0
heating['HEATING_TARGET'] = 28
heating['COMP_RUNTIME_SHOT'] = 3600
heating['BHKW_RESTART_TIME'] = 14400
heating['hp_coef'] = -1.0
heating['heatpower_compensation'] = 0.0
heating['INTEMP_TARGET'] = 20.5
heating['disabled'] = 0

consumers = [
    'sonoff1', 'sonoff2']

BHKW_MAX_POWER = 3700
BHKW_MIN_POWER = 1700

mqtt = {}


def read_mqtt(topic):
    global mqtt
    if topic not in mqtt:
        return {'value': 0, 'age': -500000000}
    else:
        return {'value': mqtt[topic]['value'], 'age': (time.time() - mqtt[topic]['age'])}

def read_mqtt_avg(topic, interval):
    global mqtt
    if topic not in mqtt:
        return float('nan')
    else:
        return history_avg(mqtt[topic]['history'], interval)


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
        elif re.search('emon/bhkw1/rpm_actual', msg.topic):
            data = float(msg.payload.decode('utf-8'))
            pow_con['bhkw1']['rpm'] = data
        elif re.search('emon/bhkw1/request_power', msg.topic):
            data = int(msg.payload.decode('utf-8'))
            if data == 0:
                pow_con['bhkw1']['enabled'] = False
                pow_con['bhkw1']['request_power'] = 0
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
        elif re.search(MQTT_MYTOPIC + 'request_state', msg.topic):
            data = msg.payload.decode('utf-8')
            control_state = data
        elif re.search(MQTT_MYTOPIC + 'heating_', msg.topic):
            data = msg.payload.decode('utf-8')
            topic = re.split(MQTT_MYTOPIC + 'heating_', msg.topic)
            if len(topic) == 2:
                heating[topic[1]] = float(msg.payload.decode('utf-8'))
        else:
            if msg.topic not in mqtt:
                mqtt[msg.topic] = {}
            try:
                value = float(msg.payload.decode('utf-8'))
                if value.is_integer():
                    data = int(value)
                else:
                    data = value
            except:
                data = msg.payload.decode('utf-8') # push without conversion
            mqtt[msg.topic]['age'] = time.time()
            mqtt[msg.topic]['value'] = data
            if 'history' not in mqtt[msg.topic]:
                mqtt[msg.topic]['history'] = []
            mqtt[msg.topic]['history'].append((data, time.time()))
            history_cleanup(mqtt[msg.topic]['history'])


def history_cleanup(hst):
    curr_time = time.time()
    for data in hst[:]:
        if curr_time > data[1] + 86400:
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

def history_max(hst, interval):
    curr_time = time.time()
    maxim = -999999
    cnt = 0.0
    for data in hst:
        if curr_time < data[1] + interval:
            if data[0] > maxim:
                maxim = data[0]
            cnt += 1

    if cnt > 0:
        return maxim
    return 0

def history_min(hst, interval):
    curr_time = time.time()
    minim = 999999
    cnt = 0.0
    for data in hst:
        if curr_time < data[1] + interval:
            if data[0] < minim:
                minim = data[0]
            cnt += 1

    if cnt > 0:
        return minim
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
    client.publish(MQTT_MYTOPIC + 'company_max15', history_max(pow_con['company']['history'], 1500))
    client.publish(MQTT_MYTOPIC + 'company_min15', history_min(pow_con['company']['history'], 1500))
    client.publish(MQTT_MYTOPIC + 'state', control_state)
    client.publish(MQTT_MYTOPIC + 'heat_state', heat_state)
    client.publish(MQTT_MYTOPIC + 'request_start', 0)
    client.publish(MQTT_MYTOPIC + 'request_stop', 0)
    for key, value in heating.items():
        client.publish(MQTT_MYTOPIC + 'heating_'+key, value)


def control_machine():
    global control_state, pow_con, heat_state, heating
    if control_machine.loop_delay > 0:
        print("control machine: loop_delay %d" % control_machine.loop_delay)
        control_machine.loop_delay -= 1
        return
    print("control machine: state " + control_state)
    next_state = 'IDLE'
    if control_state == 'INIT':
        if pow_con['bhkw1']['power'] < -1500:
            pow_con['bhkw1']['enabled'] = True
            next_state = 'BATT_CHARGING'
            heat_state = 'START'
        else:
            next_state = 'IDLE'
    elif control_state == 'BATT_NOTCHARGING':
        heating['last_bhkw_run'] = time.time()
#        print("cmp: %f" % history_avg(pow_con['company']['history'], 60))
#        print("bhkw1: %f" % history_avg(pow_con['bhkw1']['history'], 60))
        if history_max(pow_con['company']['history'], 30) > 0:
            next_state = 'BATT_NOTCHARGING'
        else:
            next_state = 'BATT_CHARGING'
            solar1_charging_start()
            control_machine.with_charging = 1
            print("BATT_CHARGING reason: consumption lowered")
    elif control_state == 'BATT_CHARGING':
        heating['last_bhkw_run'] = time.time()
#        print("cmp: %f" % history_avg(pow_con['company']['history'], 60))
#        print("bhkw1: %f" % history_avg(pow_con['bhkw1']['history'], 60))
        if history_avg(pow_con['company']['history'], 60) + history_avg(pow_con['bhkw1']['history'], 60) > 1700:
            next_state = 'BATT_CHARGING'
        else:
            next_state = 'STOP'
            print("STOP reason: battery full")
            if heating['hp_coef'] < 1.0:
                heating['hp_coef'] += 0.1

    elif control_state == 'START':
        bhkw1_start()
        next_state = 'STARTING'
        heat_state = 'START'
    elif control_state == 'START_COMP':
        next_state = 'IDLE'
        heat_state = 'START_COMP'
    elif control_state == 'START_COMP_TIME':
        next_state = 'IDLE'
        heat_state = 'START_COMP_TIME'
    elif control_state == 'STOP':
        bhkw1_stop()
        next_state = 'STOPING'
        heat_state = 'BHKW_COOLDOWN'
    elif control_state == 'STOPING':
        if pow_con['bhkw1']['power'] < 0:
            next_state = 'STOPING'
            heat_state = 'BHKW_COOLDOWN'
        else:
            next_state = 'IDLE'
            if control_machine.with_charging > 0:
                solar1_charging_stop()
                control_machine.with_charging = 0
    elif control_state == 'STARTING':
        if pow_con['bhkw1']['power'] < 0:
            if history_min(pow_con['company']['history'], 900) > 2200:
                next_state = 'STARTING_WAIT_NOTCHARGING'
                control_machine.with_charging = 0
                control_machine.loop_delay = 1
            else:
                next_state = 'STARTING_WAIT'
                control_machine.loop_delay = 1
                solar1_charging_start()
                control_machine.with_charging = 1
        else:
            next_state = 'STARTING'
    elif control_state == 'STARTING_WAIT':
        heating['last_bhkw_run'] = time.time()
        if pow_con['bhkw1']['coolant'] > 55 and (pow_con['bhkw1']['rpm'] < 2030 or pow_con['bhkw1']['rpm'] > 2070):
            next_state = 'BATT_CHARGING'
        else:
            next_state = 'STARTING_WAIT'
    elif control_state == 'STARTING_WAIT_NOTCHARGING':
        heating['last_bhkw_run'] = time.time()
        if pow_con['bhkw1']['coolant'] > 55 and (pow_con['bhkw1']['rpm'] < 2030 or pow_con['bhkw1']['rpm'] > 2070):
            next_state = 'BATT_NOTCHARGING'
        else:
            next_state = 'STARTING_WAIT_NOTCHARGING'
    elif control_state == 'IDLE':
        next_state = 'IDLE'
#        heat_state = 'IDLE'
    else:
        print("Invalid control machine state:" + control_state)
        next_state = 'INIT'
    print("control machine: next_state", next_state)
    control_state = next_state
    return


control_machine.loop_delay = 0
control_machine.with_charging = 0


def heat_machine():
    global control_state, heat_state, heating, heat_pump_command
    mq = read_mqtt('emon/heatpump1/inHot')
    if mq['age'] > 200 and heat_state != 'IDLE':
        print("heat machine: hp data too old ")
        heat_state = 'IDLE'
        if control_state != 'IDLE':
            control_state = 'STOP'
    if heat_machine.loop_delay > 0:
        print("heat machine: loop_delay %d" % heat_machine.loop_delay)
        heat_machine.loop_delay -= 1
        return
    print("heat machine: heat_state " + heat_state)
    print("heat machine: inHot: %f, boilerBottom: %f" % (read_mqtt('emon/heatpump1/inHot')['value'], read_mqtt('emon/heatpump1/boilerBottom')['value']))
    next_state = 'IDLE'
    if heat_state == 'IDLE':
        heat_pump_command['comp'] = 0
        heat_pump_command['pump'] = 0
        heat_pump_command['valve'] = 0
        next_state = 'IDLE'
    elif heat_state == 'START':
        mq = read_mqtt('emon/heatpump1/boilerBottom')
        if mq['age'] > 200:
            next_state = 'IDLE'
            control_state = 'STOP'
        elif mq['value'] < heating['WATER_TEMP1']:
            next_state = 'WATER_HEATING1'
            heat_pump_command['comp'] = 0
            heat_pump_command['pump'] = 1
            heat_pump_command['valve'] = 1
        else:
            next_state = 'HEATING'
            heat_pump_command['comp'] = 0
            heat_pump_command['pump'] = 1
            heat_pump_command['valve'] = 0
            heat_machine.loop_delay = 10
    elif heat_state == 'START_COMP':
        heating['last_hp_run'] = time.time()
        heat_pump_command['comp'] = 1
        heat_pump_command['pump'] = 1
        heat_pump_command['valve'] = 0
        heat_machine.loop_delay = 10
        next_state = 'HEATING_COMP'
    elif heat_state == 'START_COMP_TIME':
        heat_machine.compressor_runtime = time.time() + 2500
        next_state = 'HEATING_COMP_TIME'
    elif heat_state == 'HEATING_COMP':
        heating['last_hp_run'] = time.time()
        heat_pump_command['comp'] = 1
        heat_pump_command['pump'] = 1
        heat_pump_command['valve'] = 0
        mq = read_mqtt('emon/heatpump1/inHot')
        if mq['age'] > 200 :
            next_state = 'IDLE'
        elif mq['value'] > heating['HEATING_TARGET'] + heating['hp_coef']:
            next_state = 'IDLE'
        else:
            next_state = 'HEATING_COMP'
    elif heat_state == 'HEATING_COMP_TIME':
        heating['last_hp_run'] = time.time()
        heat_pump_command['comp'] = 1
        heat_pump_command['pump'] = 1
        heat_pump_command['valve'] = 0

        if pow_con['company']['power'] < -200:
            heat_machine.compressor_runtime = time.time() + 2500
        if heat_machine.compressor_runtime < time.time():
            next_state = 'IDLE'
            heat_machine.compressor_runtime = 0
        else:
            print("heat machine: comp runtime left %.0f " % (heat_machine.compressor_runtime - time.time()))
            next_state = 'HEATING_COMP_TIME'

    elif heat_state == 'HEATING':
        heat_pump_command['comp'] = 0
        heat_pump_command['pump'] = 1
        heat_pump_command['valve'] = 0
        mq = read_mqtt('emon/heatpump1/inHot')
        mq1 = read_mqtt('emon/heatpump1/boilerBottom')
        if mq['age'] > 200 :
            next_state = 'IDLE'
            print("STOP reason: heatpump timeout")
            control_state = 'STOP'
        elif mq['value'] > heating['HEATING_TARGET'] - 1 and mq1['value'] < heating['WATER_TEMP1a']:
            next_state = 'WATER_HEATING2'
        elif mq['value'] > heating['HEATING_TARGET'] + heating['heatpower_compensation']:
            next_state = 'IDLE'
            control_state = 'STOP'
            print("STOP reason: heating finished")
            if heating['hp_coef'] > -2.0:
                heating['hp_coef'] -= 0.2
        else:
            next_state = 'HEATING'
    elif heat_state == 'WATER_HEATING1':
        heat_pump_command['comp'] = 0
        heat_pump_command['pump'] = 1
        heat_pump_command['valve'] = 1
        mq = read_mqtt('emon/heatpump1/boilerBottom')
        if mq['age'] > 200 or mq['value'] > heating['WATER_TEMP1']:
            if int(heating['disabled']) == 1:
                next_state = 'WATER_HEATING2'
                print("heat machine: heating disabled")
            else:
                next_state = 'HEATING'
                heat_pump_command['valve'] = 0
                heat_machine.loop_delay = 40
        else:
            next_state = 'WATER_HEATING1'
    elif heat_state == 'BHKW_COOLDOWN':
        heat_pump_command['comp'] = 0
        heat_pump_command['pump'] = 1
        heat_pump_command['valve'] = 1
        next_state = 'IDLE'
        heat_machine.loop_delay = 60
    elif heat_state == 'WATER_HEATING2':
        heat_pump_command['comp'] = 0
        heat_pump_command['pump'] = 1
        heat_pump_command['valve'] = 1
        mq = read_mqtt('emon/heatpump1/boilerBottom')
        if mq['age'] > 200 or mq['value'] > heating['WATER_TEMP2']:
            if int(heating['disabled']) == 1:
                next_state = 'BHKW_COOLDOWN'
                control_state = 'STOP'
                print("heat machine: heating disabled, STOPPING")
            else:
                next_state = 'HEATING'
                heat_pump_command['valve'] = 0
                heat_machine.loop_delay = 40
        else:
            next_state = 'WATER_HEATING2'

    print("heat machine: next_state", next_state)
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
    global heating
    with open('runtime.json', 'w') as (fp):
        json.dump(heating, fp)


def restore_runtime():
    global heating
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
            if time.time() - pow_con['bhkw1']['age'] < 60 and -pow_con['bhkw1']['power'] - BHKW_MIN_POWER > 0 and - \
            pow_con['bhkw1']['power'] > 1500 and pow_con['bhkw1']['enabled']:
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
            if time.time() - pow_con['bhkw1']['age'] < 60 and BHKW_MAX_POWER + pow_con['bhkw1']['power'] > 0 and - \
            pow_con['bhkw1']['power'] > 1700 and pow_con['bhkw1']['enabled']:
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

def do_comp_start():
    print('HP start')
    client.publish('emon/controller/request_state', 'START_COMP')

def do_comp_start_time():
    print('HP start time')
    client.publish('emon/controller/request_state', 'START_COMP_TIME')


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

def heatpower_compensation():

    heating['heatpower_compensation'] = 2.3 * ((history_avg(pow_con['bhkw1']['history'], 900) / 2200) - 1)
    if heating['heatpower_compensation'] < 0.0:
        heating['heatpower_compensation'] = 0.0

PID_MAX_I = 40.0


def calculate_hourly():
    global control_state, heat_state, heating
    print("Calculate_hourly")
    mq_inTemp = read_mqtt_avg('weather/inTemp_C', 3600)
    print("inTemp avg = %f" % mq_inTemp)
    if not math.isnan(mq_inTemp):
        calculate_hourly.pid_i += heating['INTEMP_TARGET'] - mq_inTemp
        if calculate_hourly.pid_i > PID_MAX_I:
            calculate_hourly.pid_i = PID_MAX_I
        elif calculate_hourly.pid_i < -PID_MAX_I:
            calculate_hourly.pid_i = -PID_MAX_I
        print("pid_i = %f" % calculate_hourly.pid_i)
        heating['HEATING_TARGET'] += calculate_hourly.pid_i * 0.0125 / 24.0
        if mq_inTemp > heating['INTEMP_TARGET'] + 0.2:
            heating['HEATING_TARGET'] -= 0.25 / 24.0
            print("decreasing HEATING_TARGET to = %f" % heating['HEATING_TARGET'])
        elif mq_inTemp < heating['INTEMP_TARGET'] - 0.2:
            heating['HEATING_TARGET'] += 0.25 / 24.0
            print("increasing HEATING_TARGET to = %f" % heating['HEATING_TARGET'])
        if heating['HEATING_TARGET'] > 28.0:
            heating['HEATING_TARGET'] = 28.0
        elif heating['HEATING_TARGET'] < 19.0:
            heating['HEATING_TARGET'] = 19.0

calculate_hourly.pid_i=0

def calculate():
    global control_state, heat_state, heating
    mq = read_mqtt('emon/heatpump1/boilerBottom')
    if mq['age'] > 200: # problem with heatpump comm
        print("problem with heatpump comm")
        return
    if control_state != 'IDLE':
        return

    mq_weather = read_mqtt('weather/outTemp_C')
    mq_tempIn = read_mqtt('weather/inTemp_C')
    mq_battAh = read_mqtt('emon/solar1/AhBatt')
    mq_battCurr = read_mqtt('emon/solar1/BATcurr_realtime')
    if mq_battAh['age'] > 200: # problem with solar comm
        print("problem with solar comm")
        return
    if time.time() - pow_con['company']['age'] > 60:
        print("problem with emeter comm")
        return
    try:
        avg_consumption = history_avg(mqtt['emon/solar1/AOactpo_realtime']['history'], 600)
        print("calculate: avg power usage: %.0f" % avg_consumption)
    except:
        print("calculate: AOactpo unavailable")
        avg_consumption = 0
        pass

    print("calculate: last hp run %d ago, last bhkw run %d ago" % (time.time() - heating['last_hp_run'],time.time() - heating['last_bhkw_run']))
    print("calculate: BattAh = %f" % (mq_battAh['value']))
    if int(heating['disabled']) == 1:
        print("calculate: heating disabled, no autostart")
        return
            
    hp_restart_time = 4*heating['COMP_RUNTIME_SHOT']
    bhkw_restart_time = 4*heating['BHKW_RESTART_TIME']
    if mq_weather['age'] < 600:
        print("calculate: outside temp %f" % mq_weather['value'])
        if mq_weather['value'] < -5:
            hp_restart_time = heating['COMP_RUNTIME_SHOT']/2
            bhkw_restart_time = heating['BHKW_RESTART_TIME']
        elif mq_weather['value'] < 5:
            hp_restart_time = heating['COMP_RUNTIME_SHOT']
            bhkw_restart_time = heating['BHKW_RESTART_TIME']
        elif mq_weather['value'] < 10:
            hp_restart_time = 2*heating['COMP_RUNTIME_SHOT']
            bhkw_restart_time = 2*heating['BHKW_RESTART_TIME']
        elif mq_weather['value'] < 15:
            hp_restart_time = 3*heating['COMP_RUNTIME_SHOT']
            bhkw_restart_time = 3*heating['BHKW_RESTART_TIME']
    else:
        print("calculate: weather unavailable")

    if mq_battAh['value'] >= 0 and pow_con['company']['power'] < -500 and mq_battCurr['value'] > 0 and mq_battCurr['value'] < 10 and heat_state == 'IDLE' and heating['HEATING_TARGET'] > 20.0: # and time.time() - heating['last_bhkw_run'] > bhkw_restart_time/2:
        do_comp_start_time()
        return

    if time.time() - heating['last_bhkw_run'] > bhkw_restart_time and time.time() - heating['last_hp_run'] > 1000 and mq_battAh['value'] < -60:
        if mq_tempIn['age'] > 600:
                if heating['HEATING_TARGET'] > 20.0:
                        print("calculate: inside temp not available")
                        do_start()
        else:
                print("calculate: inside temp %f, heating_target %f" % (mq_tempIn['value'], heating['HEATING_TARGET']))
                if mq_tempIn['value'] < heating['HEATING_TARGET']:
                        do_start()
        return
    if time.time() - heating['last_bhkw_run'] > bhkw_restart_time and mq_battAh['value'] < -240:
        if mq_tempIn['age'] > 600:
                if heating['HEATING_TARGET'] > 20.0:
                        print("calculate: inside temp not available")
                        do_start()
        else:
                print("calculate: inside temp %f, heating_target %f" % (mq_tempIn['value'], heating['HEATING_TARGET']))
                if mq_tempIn['value'] < heating['HEATING_TARGET']:
                        do_start()
        return

    if heat_state != 'IDLE':
        return

    if time.time() - heating['last_hp_run'] > hp_restart_time and time.time() - heating['last_bhkw_run'] > hp_restart_time:
        avg_consumption = history_avg(mqtt['emon/solar1/AOactpo_realtime']['history'], 600)
        if  avg_consumption > 1500:
            print("calculate: power usage higher than limit %.0f. Heatpump start delayed." % avg_consumption)
            return
        if heating['hp_coef'] <= -2.0:
            print("calculate: hp_coef too low to start heatpump")
            return
        do_comp_start()
        return


client = mqtt_c.Client()
#client.connect('192.168.0.2', 1883, 60)
client.connect('localhost', 1883, 60)

if __name__ == '__main__':
    client.on_connect = on_connect
    client.on_message = on_message
    client.loop_start()
    restore_runtime()
    time.sleep(10)
    pow_con['bhkw1']['request_power'] = -pow_con['bhkw1']['power']
    last_run_timer = 0
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
            date = datetime.datetime.now(datetime.timezone.utc)
            solar_elevation = pysolar.solar.get_altitude(latitude, longitude, date)
            print("solar elevation: %f" % solar_elevation)

            power_eqtarget = 100
            if solar_elevation > 10.0:
                power_eqtarget = 10
            print("power_eqtarget: %f" % power_eqtarget)
            

            if total < 0:
                incConsum(power_eqtarget - total)
            else:
                if total > power_eqtarget:
                    decConsum(total - power_eqtarget)
            client.publish('emon/bhkw1/request_power', str(int(pow_con['bhkw1']['request_power'])))
            control_machine()
            heat_machine()
            heat_pump_update()
            publish_data()
            save_runtime()
            heatpower_compensation()
            calculate()
            if time.time() > last_run_timer + 3600:
                last_run_timer = time.time()
                calculate_hourly()
        	
#                print(mqtt)

        except Exception as e:
            try:
#                print(str(e))
                traceback.print_exc() 
            finally:
                e = None
                del e
