import time
from mqtt_spb_wrapper import *

_DEBUG = True  # Enable debug messages

print("--- Sparkplug B example - Application Entity Listener")


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
app.attribures.set_value("description", "Test application")

# Commands
app.commands.set_value("rebirth", False)

# Connect to the broker----------------------------------------------------------------
# Upon connection, the BIRTH message will be sent and DEATH messages will be set
print("Connecting to broker...")
app.connect("localhost", 1883)

# Loop forever, messages and commands will be handeled by the callbacks
while True:
    time.sleep(1000)