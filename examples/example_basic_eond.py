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
device.attribures.set_value("description", "Simple EoN Device node")
device.attribures.set_value("type", "Simulated-EoND-device")
device.attribures.set_value("version", "0.01")

# Data / Telemetry
device.data.set_value("value", 0)

# Commands
device.commands.set_value("rebirth", False)

# Connect to the broker --------------------------------------------
# Upon connection, the BIRTH message will be sent and DEATH messages will be set
print("Connecting to broker...")
device.connect("localhost", 1883)

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