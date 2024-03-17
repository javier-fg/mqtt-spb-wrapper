import time
from mqtt_spb_wrapper import *

_DEBUG = True  # Enable debug messages

def callback_app_message(topic, payload):
    print("APP received MESSAGE: %s - %s" % (topic, payload))


def callback_app_command(payload):
    print("APP received CMD: %s" % payload)


domain_name = "Domain-001"
app_entity_name = "ListenApp01"

app = MqttSpbEntityApplication(domain_name, app_entity_name, debug_info=_DEBUG)

# Set callbacks
app.on_message = callback_app_message
app.on_command = callback_app_command

# Set the device Attributes, Data and Commands that will be sent on the DBIRTH message --------------------------

# Attributes
app.attributes.set_value("description", "Test application")

# Commands
app.commands.set_value("rebirth", False)

# Connect to the broker----------------------------------------------------------------
_connected = False
while not _connected:
    print("Trying to connect to broker...")
    _connected = app.connect("localhost8", 1883)
    if not _connected:
        print("  Error, could not connect. Trying again in a few seconds ...")
        time.sleep(3)

# Send birth message
app.publish_birth()

# Loop forever, messages and commands will be handeled by the callbacks
while True:
    time.sleep(1000)