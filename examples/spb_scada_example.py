#
#    --- Simple Sparkplub B SCADA application example ---
#
#   The application will create an spB SCADA application node to receive all group messages and
#   display them in the console. The application will also keep status of current group entities.
#   Also, the SCADA node will send periodically ping messages to active entities.
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
_config_spb_scada_name = os.environ.get("SPB_SCADA", "SCADA-001")

# MQTT Configuration
_config_mqtt_topic = "#"    # Topic to listen
_config_mqtt_host = os.environ.get("MQTT_HOST", "localhost")
_config_mqtt_port = int(os.environ.get("MQTT_PORT", 1883))
_config_mqtt_user = os.environ.get("MQTT_USER", "")
_config_mqtt_pass = os.environ.get("MQTT_PASS", "")

_device_test_eon = os.environ.get("DEV_EON", "Edge-001")
_device_test_eond = os.environ.get("DEV_EOND", "Device-01")

print("--- Sparkplug B example - SCADA Entity Simple")

# Global variables ----------------------------------------

entities = {}   # List of detected spB entities


# Create the SCADA entity to listen to all spB messages
scada = MqttSpbEntityScada(spb_domain_name=_config_spb_domain_name,
                           spb_scada_name=_config_spb_scada_name,
                           debug_info=_DEBUG)

# ATTRIBUTES - Scada application entity
scada.attributes.set_value("description", "SCADA application simple")

# Setting SCADA callbacks on received messages.
# scada.callback_birth = lambda topic, payload: print("SCADA birth msg on - " + str(topic))
# scada.callback_data = lambda topic, payload: print("SCADA data msg on - " + str(topic))
# scada.callback_death = lambda topic, payload: print("SCADA death msg on - " + str(topic))


# Reference to specific EoN entity
# edgetest = scada.get_edge_node("EdgeNode-001")
# edgetest.callback_birth = lambda payload: print("EoN birth msg - " + str(payload)) # Setting callbacks on received messages.
# edgetest.callback_data = lambda payload: print("EoN data msg - " + str(payload))
# edgetest.callback_death = lambda payload: print("EoN death msg - " + str(payload))


# EoND device specific object
print("Getting virtual device %s - %s" %(_device_test_eon, _device_test_eond))
devicetest = scada.get_edge_node_device(_device_test_eon, _device_test_eond)
devicetest.callback_birth = lambda payload: print("EoND birth msg - " + str(payload)) # Setting callbacks on received messages.
# devicetest.callback_data = lambda payload: print("EoND data msg - " + str(payload))
devicetest.callback_death = lambda payload: print("EoND death msg - " + str(payload))

def devicetest_callback(payload):
    print("EoND data msg - " + str(payload))

    # Example - Send a command if a value is equal to a specific number.
    if devicetest.data.get_value("value") == 2:
        devicetest.send_command("test", True, True)   # Note that we set foce=True, since the entity may not implement the test command.


devicetest.callback_data = devicetest_callback

# ------ Connect to the MQTT broker -------
_connected = False
while not _connected:
    print("Connecting to data broker %s:%d ..." % (_config_mqtt_host, _config_mqtt_port))
    _connected = scada.connect(_config_mqtt_host,
                               _config_mqtt_port,
                               _config_mqtt_user,
                               _config_mqtt_pass)
    if not _connected:
        print("  Error, could not connect. Trying again in a few seconds ...")
        time.sleep(3)

# scada.publish_birth()  # Send birth message for the SCADA application

print("Waiting for new device data . . .")

# ------- Loop forever -------
# The application will display the existing discovered entities in the spB domain.
while True:

    # Sleep some time
    time.sleep(10)

    # Print entities status and send some test command
    print("--- SCADA current entities:")

    # Print the discovered EoN node
    for eon_name in scada.entities_eon.keys():
        print("  %s (Active:%s)" % (eon_name, scada.entities_eon[eon_name].is_alive()))
        for eond_name in scada.entities_eon[eon_name].entities_eond.keys():
            device = scada.entities_eon[eon_name].entities_eond[eond_name]
            print("     %s (Active:%s) - %s | %s" % (device.entity_name,
                                              scada.entities_eon[eon_name].entities_eond[eond_name].is_alive(),
                                              device.data.get_dictionary(),
                                              device.attributes.get_dictionary()))

