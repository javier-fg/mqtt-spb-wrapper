# -----------------------------------------------------------------------------
# Copyright (c) Javier FG 2024
#
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
# -----------------------------------------------------------------------------

"""
Filename: spb_app_listener.py

Description:

   --- Simple Sparkplub B APPLICATION entity example ---

  The application will create an spB Application entity to receive all group messages and
  display them in the console. The application will also keep status of current group entities.

"""

import os
import time
from mqtt_spb_wrapper import MqttSpbEntityApp


# APPLICATION default configuration parameters -----------------------------------------------
_DEBUG = True           # Enable debug messages
_GHOST_SPB_APP = True   # If true, the spB App entity will not publish its BIRTH and DEATH messages

# Sparkplug B parameters
_config_spb_domain_name = os.environ.get("SPB_GROUP", "IECON")
_config_spb_app_name = os.environ.get("SPB_APP", "App-001")

# Testing EoN and EoND names for automatic detection
_device_test_eon = os.environ.get("DEV_EON", "Edge-001")
_device_test_eond = os.environ.get("DEV_EOND", "Device-01")

# MQTT Configuration
_config_mqtt_host = os.environ.get("MQTT_HOST", "localhost")
_config_mqtt_port = int(os.environ.get("MQTT_PORT", 1883))
_config_mqtt_user = os.environ.get("MQTT_USER", "")
_config_mqtt_pass = os.environ.get("MQTT_PASS", "")

print("--- Sparkplug B example - spB Application Entity example")

# Global variables ----------------------------------------

#  ---------- Create the spB App entity to listen to all spB messages
application = MqttSpbEntityApp(
    spb_domain_name=_config_spb_domain_name,
    spb_app_name=_config_spb_app_name,
    debug_enabled=_DEBUG
)

# ATTRIBUTES - Application entity
application.attributes.set_value("description", "APP entity example simple")

# Setting APP callbacks on received messages.
# application.callback_birth = lambda topic, payload: print("APP birth msg on - " + str(topic))
# application.callback_data = lambda topic, payload: print("APP data msg on - " + str(topic))
# application.callback_death = lambda topic, payload: print("APP death msg on - " + str(topic))
# application.callback_new_eon = lambda _eon_name: print("APP new EoN entity: " + str(_eon_name))
application.callback_new_eond = lambda _eon_name, _eond_name: print("APP new EoND entity: " + str(_eond_name) + "." + str(_eon_name))

#  ---------- Subscribe to a EoN entity, specific callbacks will be executed when data is being received by the application.
# edgetest = application.get_edge_node("EdgeNode-001")
# edgetest.callback_birth = lambda payload: print("EoN birth msg - " + str(payload)) # Setting callbacks on received messages.
# edgetest.callback_data = lambda payload: print("EoN data msg - " + str(payload))
# edgetest.callback_death = lambda payload: print("EoN death msg - " + str(payload))


# ---------- Subscribe to an EoND device
print("Subscribing to virtual device %s - %s" %(_device_test_eon, _device_test_eond))

devicetest = application.get_edge_device(_device_test_eon, _device_test_eond)


def devicetest_data_callback(payload):

    print("EoND data msg - " + str(payload))

    # Example - Send a command if a value is equal to a specific number.
    if devicetest.data.get_value("value") == 2:
        devicetest.send_command("test", True, True)
        print("EoND command sent!")

# Registering callbacks
devicetest.callback_birth = lambda payload: print("EoND birth msg - " + str(payload))
devicetest.callback_death = lambda payload: print("EoND death msg - " + str(payload))
devicetest.callback_data = devicetest_data_callback
# devicetest.callback_data = lambda payload: print("EoND data msg - " + str(payload))


# ------ Connect to the MQTT broker -------
_connected = False
while not _connected:
    print("Connecting to data broker %s:%d ..." % (_config_mqtt_host, _config_mqtt_port))
    _connected = application.connect(_config_mqtt_host,
                                     _config_mqtt_port,
                                     _config_mqtt_user,
                                     _config_mqtt_pass,
                                     skip_death=_GHOST_SPB_APP)
    if not _connected:
        print("  Error, could not connect. Trying again in a few seconds ...")
        time.sleep(3)

# Send birth message for the spB Application, if GHOST mode activated, no BIRTH message sent
if not _GHOST_SPB_APP:
    application.publish_birth()

print("Waiting for APP initialization...")
while not application.is_initialized():
    time.sleep(0.1)

# ------- Loop forever -------
# The application will display the existing discovered entities in the spB domain.
while True:

    # Print entities status and send some test command
    print("--- APP current entities:")

    # Print the discovered EoN nodes and devices
    for eon_name in application.entities_eon.keys():
        print("  %s (Active:%s)" % (eon_name, application.entities_eon[eon_name].is_alive()))
        for eond_name in application.entities_eon[eon_name].entities_eond.keys():
            device = application.entities_eon[eon_name].entities_eond[eond_name]
            print("     %s (Active:%s) - %s | %s" % (
                device.entity_name,
                application.entities_eon[eon_name].entities_eond[eond_name].is_alive(),
                device.data.as_dict(),
                device.attributes.as_dict())
            )

    # Sleep some time
    time.sleep(10)

