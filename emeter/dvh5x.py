#!/usr/bin/python

from __future__ import print_function

import argparse
import serial
import time
import os
import stat


try:
    import meterbus
except ImportError:
    import sys
    sys.path.append('../')
    import meterbus


def ping_address(ser, address, retries=5):
    for i in range(0, retries + 1):
        meterbus.send_ping_frame(ser, address)
        try:
            frame = meterbus.load(meterbus.recv_frame(ser, 1))
            if isinstance(frame, meterbus.TelegramACK):
                return True

        except meterbus.MBusFrameDecodeError:
#            print ("ping timeout..")
            time.sleep(0.1)
            pass

    return False

def do_reg_file(args):
    with open(args.device, 'rb') as f:
        frame = meterbus.load(f.read())
        if frame is not None:
            print(frame.to_JSON())



def send_mask_frame(ser, address=None, req=None):
  if address is not None and meterbus.is_primary_address(address) == False:
    return False

  if req is None:
    frame = meterbus.TelegramLong()
    frame.header.cField.parts = [
#      meterbus.CONTROL_MASK_REQ_UD2 | meterbus.CONTROL_MASK_DIR_M2S | meterbus.CONTROL_MASK_FCV | meterbus.CONTROL_MASK_FCB
	 meterbus.CONTROL_MASK_SND_UD | meterbus.CONTROL_MASK_DIR_M2S
#	0x73
    ]
    frame.header.aField.parts = [address]
  else:
    frame = req
  frame.body.bodyHeaderLength = 2
  frame.body.bodyHeader = [0x51]
#  frame.body.bodyHeader = [0xBD]
#  frame.body.bodyPayload = [0x0D, 0xFD, 0x0B, 0xED, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF ]
  frame.body.bodyPayload = [0x0D, 0xFD, 0x0B, 0xED, 0x03, 0x03, 0x03, 0x03, 0x00, 0x00, 0x00, 0x18, 0x6E, 0x00, 0x80, 0xE3, 0x01 ]
#  frame.body.bodyPayload = [0x01, 0xFF, 0x54, 0x01  ]
#  frame.body.bodyPayload = [0x7F]
  frame.header.crcField = frame.compute_crc()
  
#  print(''.join(format(x, '02x') for x in bytearray(frame)))
  meterbus.serial_send(ser, frame)

  return frame

def send_tarif_frame(ser, tarif, address=None, req=None):
  if address is not None and meterbus.is_primary_address(address) == False:
    return False

  if req is None:
    frame = meterbus.TelegramLong()
    frame.header.cField.parts = [
#      meterbus.CONTROL_MASK_REQ_UD2 | meterbus.CONTROL_MASK_DIR_M2S | meterbus.CONTROL_MASK_FCV | meterbus.CONTROL_MASK_FCB
	 meterbus.CONTROL_MASK_SND_UD | meterbus.CONTROL_MASK_DIR_M2S
#	0x73
    ]
    frame.header.aField.parts = [address]
  else:
    frame = req
  frame.body.bodyHeaderLength = 2
  frame.body.bodyHeader = [0x51]
#  frame.body.bodyHeader = [0xBD]
#  frame.body.bodyPayload = [0x0D, 0xFD, 0x0B, 0xED, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF ]
#  frame.body.bodyPayload = [0x0D, 0xFD, 0x0B, 0xED, 0x03, 0x03, 0x03, 0x03, 0x00, 0x00, 0x00, 0x00, 0x68, 0x00, 0x80, 0xFF, 0x01 ]
  frame.body.bodyPayload = [0x01, 0xFF, 0x54, tarif  ]
#  frame.body.bodyPayload = [0x7F]
  frame.header.crcField = frame.compute_crc()
  
#  print(''.join(format(x, '02x') for x in bytearray(frame)))
  meterbus.serial_send(ser, frame)

  return frame


def dvh5x_tarif(address, device, baudrate, tarif):
    try:
        ibt = meterbus.inter_byte_timeout(baudrate)

        with serial.Serial(device, baudrate, 
                     parity=serial.PARITY_EVEN, rtscts=False, timeout=0.2) as ser:

            frame = None

            ping_address(ser, meterbus.ADDRESS_NETWORK_LAYER, 0)

            if True:
                ret = ping_address(ser, address, 5)
                print ("Ping status: %s " % ret)

                time.sleep(0.1)
                send_tarif_frame(ser, address, tarif)
                frame = meterbus.load(meterbus.recv_frame(ser, 1))
                assert isinstance(frame, meterbus.TelegramACK)
                print ("tarif set ok")
                ser.close()

    except serial.serialutil.SerialException as e:
        ser.close()
        print(e)


def do_reset():
    print ("Reset CP210x ")
    os.system("./usbreset.py CP210x")

def check_reset(status):
    if not hasattr(check_reset, "dvh5x_check_reset"):
        check_reset.dvh5x_check_reset = 0
    print ("Comm failed %d" % check_reset.dvh5x_check_reset)
    if status:
        check_reset.dvh5x_check_reset = 0
    else:
        check_reset.dvh5x_check_reset += 1
        if check_reset.dvh5x_check_reset > 10:
            do_reset()
            check_reset.dvh5x_check_reset = 0
            

def dvh5x_read(address, device, baudrate):

    try:
        ibt = meterbus.inter_byte_timeout(baudrate)

        with serial.Serial(device, baudrate, 
                     parity=serial.PARITY_EVEN, rtscts=False, timeout=0.2) as ser:

            frame = None

            ping_address(ser, meterbus.ADDRESS_NETWORK_LAYER, 0)

            if True:
                ret = ping_address(ser, address, 5)
                print ("Ping status: %s " % ret)

                if ret:
                    check_reset(ret)
                else:
                    ser.close()
                    check_reset(ret)
                    return

                time.sleep(0.1)
                send_mask_frame(ser, address)
                frame = meterbus.load(meterbus.recv_frame(ser, 1))
                assert isinstance(frame, meterbus.TelegramACK)
                print ("custom ok")

                ping_address(ser, meterbus.ADDRESS_NETWORK_LAYER, 0)
                meterbus.send_request_frame(ser, address)
                recv =  meterbus.recv_frame(ser, meterbus.FRAME_DATA_LENGTH)
#	        print("recv", recv)
                frame = meterbus.load(recv)

                ser.close()
                return frame
            if frame is not None:
                print(frame.body.interpreted['records'][0]['value'])
                ser.close()

    except serial.serialutil.SerialException as e:
        print("SerialException")
        print(e)


if __name__ == '__main__':
    try:
        dvh5x_tarif(1, '/dev/ttyUSB1', 9600, 1)
    except meterbus.exceptions.MBusFrameDecodeError:
        print ("Exception")
    while True:
        try:
            print (dvh5x_read(1, '/dev/ttyUSB1', 9600).to_JSON())
        except meterbus.exceptions.MBusFrameDecodeError:
            print ("Exception")
    