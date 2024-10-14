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
Filename: spb_scada_example.py

Description:

   --- Simple Sparkplub B SCADA entity example ---

  The application will create an spB SCADA application node to receive all group messages and
  display them in the console. The application will also keep status of current group entities.
  Also, the SCADA node will send periodically ping messages to active entities.

"""

import os
import time
from mqtt_spb_wrapper import *

# APPLICATION default configuration parameters -----------------------------------------------
_DEBUG = True               # Enable debug messages
_GHOST_SPB_SCADA = True     # If true, the spB SCADA entity will not publish its BIRTH and DEATH messages

# Sparkplug B parameters
_config_spb_domain_name = os.environ.get("SPB_GROUP", "TestDomain")
_config_spb_scada_name = os.environ.get("SPB_SCADA", "SCADA-001")

# Testing EoN and EoND names for automatic detection and events
_device_test_eon = os.environ.get("DEV_EON", "Edge-001")
_device_test_eond = os.environ.get("DEV_EOND", "Device-01")

# spB MQTT broker configuration
_config_mqtt_host = os.environ.get("MQTT_HOST", "localhost")
_config_mqtt_port = int(os.environ.get("MQTT_PORT", 1883))
_config_mqtt_user = os.environ.get("MQTT_USER", "")
_config_mqtt_pass = os.environ.get("MQTT_PASS", "")

print("--- Sparkplug B example - SCADA Entity Simple")

# Global variables ----------------------------------------

# Create the SCADA entity to listen to all spB messages
scada = MqttSpbEntityScada(spb_domain_name=_config_spb_domain_name,
                           spb_scada_name=_config_spb_scada_name,
                           debug_enabled=_DEBUG)

# ATTRIBUTES - Scada application entity
scada.attributes.set_value("description", "SCADA application simple")

# Setting SCADA callbacks on received messages.
scada.callback_birth = lambda topic, payload: print("SCADA birth msg on - " + str(topic))
scada.callback_data = lambda topic, payload: print("SCADA data msg on - " + str(topic))
scada.callback_death = lambda topic, payload: print("SCADA death msg on - " + str(topic))
scada.callback_new_eon = lambda _eon_name: print("SCADA new EoN entity: " + str(_eon_name))
scada.callback_new_eond = lambda _eon_name, _eond_name: print("SCADA new EoND entity: " + str(_eond_name) + "." + str(_eon_name))


# Reference to specific EoN entity
# edgetest = scada.get_edge_node("EdgeNode-001")
# edgetest.callback_birth = lambda payload: print("EoN birth msg - " + str(payload)) # Setting callbacks on received messages.
# edgetest.callback_data = lambda payload: print("EoN data msg - " + str(payload))
# edgetest.callback_death = lambda payload: print("EoN death msg - " + str(payload))


# EoND device specific object
print("Getting virtual device %s - %s" %(_device_test_eon, _device_test_eond))
devicetest = scada.get_edge_device(_device_test_eon, _device_test_eond)


def devicetest_data_callback(payload):

    print("EoND data msg - " + str(payload))

    # Example - Send a command if a value is equal to a specific number.
    if devicetest.data.get_value("value") == 2:
        devicetest.send_command("test", True, True)
        print("EoND command sent!")


# Register EoND Device Entity callbacks
devicetest.callback_birth = lambda payload: print("EoND birth msg - " + str(payload))
devicetest.callback_death = lambda payload: print("EoND death msg - " + str(payload))
devicetest.callback_data = devicetest_data_callback
# devicetest.callback_data = lambda payload: print("EoND data msg - " + str(payload))


# ------ Connect to the MQTT broker -------
_connected = False
while not _connected:
    print("Connecting to data broker %s:%d ..." % (_config_mqtt_host, _config_mqtt_port))
    _connected = scada.connect(_config_mqtt_host,
                               _config_mqtt_port,
                               _config_mqtt_user,
                               _config_mqtt_pass,
                               skip_death=_GHOST_SPB_SCADA
                               )
    if not _connected:
        print("  Error, could not connect. Trying again in a few seconds ...")
        time.sleep(3)

# Send birth message for the SCADA application
if not _GHOST_SPB_SCADA:
    scada.publish_birth()

print("Waiting for SCADA initialization...")
while not scada.is_initialized():
    time.sleep(0.1)

# ------- Loop forever -------
# The application will display the existing discovered entities in the spB domain.
while True:

    # Print the discovered EoN node
    print("\n--- SCADA current entities:")
    for eon_name in scada.entities_eon.keys():
        print("  %s (Active:%s)" % (eon_name, scada.entities_eon[eon_name].is_alive()))
        for eond_name in scada.entities_eon[eon_name].entities_eond.keys():
            device = scada.entities_eon[eon_name].entities_eond[eond_name]
            print("     %s (Active:%s) - %s | %s" % (
                device.entity_name,
                scada.entities_eon[eon_name].entities_eond[eond_name].is_alive(),
                device.data.as_dict(),
                device.attributes.as_dict())
            )

    # Sleep some time
    time.sleep(20)

