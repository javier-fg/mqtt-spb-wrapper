import time
from mqtt_spb_wrapper import *

_DEBUG = True  # Enable debug messages

print("--- Sparkplug B example - End of Node Device - Simple")


def callback_command(payload):
    print("DEVICE received CMD: %s" % (payload))


def callback_message(topic, payload):
    print("Received MESSAGE: %s - %s" % (topic, payload))


# Create the spB entity object
group_name = "Group-001"
edge_node_name = "Gateway-001"
device_name = "SimpleEoND-01"

device = MqttSpbEntityDevice(group_name, edge_node_name, device_name, _DEBUG)

device.on_message = callback_message  # Received messages
device.on_command = callback_command  # Callback for received commands

# Set the device Attributes, Data and Commands that will be sent on the DBIRTH message --------------------------

# Attributes
device.attributes.set_value("description", "Simple EoN Device node")
device.attributes.set_value("type", "Simulated-EoND-device")
device.attributes.set_value("version", "0.01")

# Data / Telemetry
device.data.set_value("value", 0)

# Commands
device.commands.set_value("rebirth", False)

# Try to connect to the broker --------------------------------------------
_connected = False
while not _connected:
    print("Trying to connect to broker...")
    _connected = device.connect("localhost8", 1883)
    if not _connected:
        print("  Error, could not connect. Trying again in a few seconds ...")
        time.sleep(3)

# Send birth message
device.publish_birth()

# Send some telemetry values ---------------------------------------
value = 0  # Simple counter
for i in range(5):
    # Update the data value
    device.data.set_value("value", value)

    # Send data values
    print("Sending data - value : %d" % value)
    device.publish_data()

    # Increase counter
    value += 1

    # Sleep some time
    time.sleep(5)

# Disconnect device -------------------------------------------------
# After disconnection the MQTT broker will send the entity DEATH message.
print("Disconnecting device")
device.disconnect()

print("Application finished !")