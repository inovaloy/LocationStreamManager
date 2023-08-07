import time
import serial
import argparse
import sys


AT_COMMAND  = b'AT'
OK_RESPONSE = b'OK'

#
# HC-06 supports only few AT commands.
# 1. AT              -> Return OK
# 2. AT+VERSION      -> Retrun OK<VERSION>
# 3. AT+NAME<Name>   -> Return OKsetname
# 4. AT+BAUD<n>      -> Return OK<BAUD>
# 5. AT+PIN<XXXX>    -> Return OKsetPIN
#
#
# For BAUD rate set n =>
# 1 set to 1200bps
# 2 set to 2400bps
# 3 set to 4800bps
# 4 set to 9600bps (Default)
# 5 set to 19200bps
# 6 set to 38400bps
# 7 set to 57600bps
# 8 set to 115200bps
#

BAUDRATE = [
    (1200,   "1"),
    (2400,   "2"),
    (4800,   "3"),
    (9600,   "4"),
    (19200,  "5"),
    (38400,  "6"),
    (57900,  "7"),
    (115200, "8"),
]

Serial = serial.Serial(
    port     = '/dev/ttyS0',
    baudrate = 9600,
    parity   = serial.PARITY_NONE,
    stopbits = serial.STOPBITS_ONE,
    bytesize = serial.EIGHTBITS,
    timeout  = 1
)

def sendCommand(Command = AT_COMMAND, timeout = 3):
    print("Command Send : '" + Command.decode() + "'")
    Serial.write(Command)

    returnValue = None
    timeout = time.time() + timeout
    while True:
        data = Serial.readline()
        if data != b'':
            print("Response Get : '" + data.decode() + "'")
            returnValue = data
            break
        if time.time() > timeout:
            print("Data receive timeout!")
            break
        time.sleep(0.1)

    return returnValue


def getVersion():
    return sendCommand(b'AT+VERSION')


def setBluetoothName(Name):
    return sendCommand(b'AT+NAME' + (Name.encode('utf-8')))


def setPIN(pin):
    return sendCommand(b'AT+PIN' + (str(pin).encode('utf-8')))


def setBaudRate(baud):
    for item in BAUDRATE:
        if str(item[0]) == str(baud):
            return sendCommand(b'AT+BAUD' + (item[1].encode('utf-8')))


def getBaudRate():
    BaudRateLen = str(len(BAUDRATE))
    counter = 1
    for item in BAUDRATE:
        print("Checking ["+str(counter)+"/"+BaudRateLen+"]")
        counter += 1
        TempSerial = serial.Serial(
            port     = '/dev/ttyS0',
            baudrate = item[0],
            parity   = serial.PARITY_NONE,
            stopbits = serial.STOPBITS_ONE,
            bytesize = serial.EIGHTBITS,
            timeout  = 1
        )
        TempSerial.write(AT_COMMAND)
        timeout = time.time() + 2
        BaudValue = 0
        while True:
            data = TempSerial.readline()
            if data == OK_RESPONSE:
                BaudValue = item[0]
                break
            if time.time() > timeout:
                break
            time.sleep(0.1)

        if BaudValue != 0:
            print("Baud Rate: ", BaudValue)
            break
    if BaudValue == 0:
        print("Not albe to find proper Baud Rate")



def main():
    parser = argparse.ArgumentParser(description='Bluetooth Configuration Script')

    parser.add_argument(
        "-c",
        "--check",
        action = "store_true",
        help = "Checking Bluetooth module via sending AT command"
    )

    parser.add_argument(
        "-v",
        "--version",
        action = "store_true",
        help = "Checking Bluetooth firmware version"
    )

    parser.add_argument(
        "-n",
        "--name",
        help = "Set Bluetooth device name"
    )

    parser.add_argument(
        "-p",
        "--pin",
        type = int,
        help = "Set Bluetooth device pairing PIN"
    )

    parser.add_argument(
        "-b",
        "--baud",
        type = int,
        help = "Set Bluetooth device Baud rate. Supports "+(", ".join([str(baud[0]) for baud in BAUDRATE]))
    )

    parser.add_argument(
        "-bc",
        "--baudCheck",
        action = "store_true",
        help = "Get Bluetooth device Baud rate"
    )

    args = parser.parse_args(args = None if sys.argv[1:] else ['--help'])

    argsCheck      = args.check
    argsVersion    = args.version
    argsName       = args.name
    argsPin        = args.pin
    argsBaud       = args.baud
    argsBaudCheck  = args.baudCheck

    if argsCheck:
        if sendCommand() == OK_RESPONSE:
            print("Bluetooth Device Wokring Pertectly.")
        else:
            print("Bluetooth Device Checking Failed.")
            print("Check the connection once, and it must not be connected with Host.")
            print("LED should be blinking continiously.")

    if argsVersion:
        if sendCommand() == OK_RESPONSE:
            version = getVersion()
            if version != b'' and OK_RESPONSE in version:
                print("Bluetooth firmware Version:",
                      version.decode().replace(OK_RESPONSE.decode(), ""))
            else:
                print("Not albe to read bluetooth firmware version")

    if argsName != None:
        if sendCommand() == OK_RESPONSE:
            setBluetoothName(argsName)

    if argsPin != None:
        if len(str(argsPin)) == 4:
            if sendCommand() == OK_RESPONSE:
                setPIN(argsPin)
        else:
            print("PIN must be 4 digit; e.g. 1111, 1234 etc")

    if argsBaud != None:
        if str(argsBaud) in [str(baud[0]) for baud in BAUDRATE]:
            if sendCommand() == OK_RESPONSE:
                setBaudRate(argsBaud)
        else:
            print("Baut rate " + str(argsBaud) + " is not supported.")

    if argsBaudCheck:
        getBaudRate()


if __name__ == "__main__":
    main()
