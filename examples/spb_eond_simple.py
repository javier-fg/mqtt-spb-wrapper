#
#    --- Simple Sparkplub B EoN Device example ---
#
#   The application will create an spB EoN Device node that will send data at intervals.
#       - Device will send a counter field that will increment on each published message.
#       - Device can received "ping" commands
#
#   Notes:
#       - To set MQTT broker parameters, use config.yml file
#       - To set spB group and eon ids, use config.yml file.
#
import os
import time
from mqtt_spb_wrapper import *


# APPLICATION default configuration parameters -----------------------------------------------
_DEBUG = True   # Enable debug messages

# Sparkplug B parameters
_config_spb_domain_name = os.environ.get("SPB_GROUP", "TestDomain")
_config_spb_eon_name = os.environ.get("SPB_EON", "Edge-001")
_config_spb_eon_device_name = os.environ.get("SPB_EON_DEVICE", "Device-01s")

# MQTT Configuration
_config_mqtt_topic = "#"    # Topic to listen
_config_mqtt_host = os.environ.get("MQTT_HOST", "localhost")
_config_mqtt_port = int(os.environ.get("MQTT_PORT", 1883))

_config_mqtt_user = os.environ.get("MQTT_USER", "")
_config_mqtt_pass = os.environ.get("MQTT_PASS", "")
_config_mqtt_tls_enabled = os.environ.get("MQTT_TLS_ENABLED", '').lower() in ('true', '1', 't')

_config_mqtt_tls_ca = os.environ.get("MQTT_TLS_CA", "")
_config_mqtt_tls_cert = os.environ.get("MQTT_TLS_CERT", "")
_config_mqtt_tls_key = os.environ.get("MQTT_TLS_KEY", "")

def callback_command(payload):
    """
        Callback function for received commands events.
    """

    print("DEVICE received CMD: %s" % payload)

    # Parse commands
    for cmd in payload['metrics']:

        # Parse fields
        name = cmd["name"]
        value = cmd["value"]

        # Parse commands
        if name == "ping" and value:  # Ping command

            # Send response
            device.data.set_value("ping", True)
            device.publish_data()

            print("  CMD Ping received - Sending response")

def callback_cmd_test(value):
    print("   CMD test received - " + str(value))

def callback_message(topic, payload):
    """
        Callback function for received messages events
    """

    print("Received MESSAGE: %s - %s"%(topic, payload))


print("--- Sparkplug B example - End of Node Device - Simple")

# Create the spB entity object
device = MqttSpbEntityDevice(spb_domain_name=_config_spb_domain_name,
                             spb_eon_name=_config_spb_eon_name,
                             spb_eon_device_name=_config_spb_eon_device_name,
                             debug_enabled=_DEBUG)

# Configure callbacks
device.on_message = callback_message    # Received messages
device.on_command = callback_command    # Callback for received commands

# Default device data values
attributes = {"description": "Simple EoN Device node",
              "type": "Simulated device",
              "version": "0.01"}
commands = {"ping": False}
telemetry = {"value": 0,    # Simple value counter
             }

# Console print the device data and fill device fields
print("--- ATTRIBUTES")
for k in attributes:
    device.attributes.set_value(k, attributes[k])
for k in device.attributes.get_dictionary():
    print("  %s - %s" % (k["name"], k['value']))

print("--- COMMANDS")
device.commands.set_value("test", False, callback_on_change=callback_cmd_test)
for k in commands:
    device.commands.set_value(k, commands[k])
for k in device.commands.get_dictionary():
    print("  %s - %s" % (k["name"], k['value']))

print("--- TELEMETRY")
for k in telemetry:
    device.data.set_value(k, telemetry[k])
for k in device.data.get_dictionary():
    print("  %s - %s" % (k["name"], k['value']))

# Connect to the broker
_connected = device.is_connected()
while not _connected:
    print("Connecting to data broker %s:%d ..." % (_config_mqtt_host, _config_mqtt_port))
    _connected = device.connect(_config_mqtt_host,
                                _config_mqtt_port,
                                _config_mqtt_user,
                                _config_mqtt_pass,
                                _config_mqtt_tls_enabled,
                                _config_mqtt_tls_ca,
                                _config_mqtt_tls_cert,
                                _config_mqtt_tls_key)
    if not _connected:
        print("  Error, could not connect. Trying again in a few seconds ...")
        time.sleep(3)

device.publish_birth()  # Send birth message

# Send some telemetry values
value = 0   # Simple device field value ( simple counter )
for i in range(10):

    # Update the data value
    print("Sending data - value : %d" % value)
    device.data.set_value("value", value)
    value += 1

    # Send data values
    device.publish_data()

    # Sleep some time
    time.sleep(5)

# Disconnect device
print("Disconnecting device")
device.disconnect()

print("Application finished !")