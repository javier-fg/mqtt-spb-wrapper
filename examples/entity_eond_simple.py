#
#    --- Simple Sparkplub B EoN Device example ---
#
#   The application will create an spB EoN Device node that will send data at intervals.
#       - Device will send a counter field that will increment on each published message.
#       - Device can received "ping" commands
#
#   Notes:
#       - To set MQTT broker parameters, use config.yml file
#       - To set spB domain and eon ids, use config.yml file.
#
import time
from mqtt_spb_wrapper import *
import yaml

_DEBUG = True   # Enable debug messages

print("--- Sparkplug B example - End of Node Device - Simple")

# Load application configuration file
try:
    with open("config.yml") as fr:
        config = yaml.load(fr, yaml.FullLoader)
    print("Configuration file loaded")
except:
    print("ERROR - Could not load config file")
    exit()

def callback_command(payload):

    print("DEVICE received CMD: %s"%(payload))

    # Parse commands
    for cmd in payload['metrics']:

        # Parse fields
        name = cmd["name"]
        value = cmd["value"]

        #Parse commands
        if name == "ping" and value:  # Ping command

            #Send response
            device.data.update_value("ping", True)
            device.publish_data()

            print("  CMD Ping received - Sending response")

def callback_message(topic, payload):
    print("Received MESSAGE: %s - %s"%(topic, payload))

# Create the spB entity object
domain_id = config['sparkplugb']['domain_id']
edge_node_id = "Gateway-001"
device_id = "SimpleDev-01"

device = MqttSparkPlugB_Entity_Device(domain_id, edge_node_id, device_id, _DEBUG )

device.on_message = callback_message    # Received messages
#device.on_command = callback_command    # Callback for received commands

attributes = { "description" : "Simple EoN Device node",
               "type" : "Simulated device",
               "version" : "0.01"}
commands = { "ping": False}
telemetry = { "value": 0}

# Console print the device data and fill device fields
print("--- ATTRIBUTES")
for k in attributes:
    print("  %s - %s"%(k, str(attributes[k])))
    device.attribures.add_value(k, attributes[k])

print("--- COMMANDS")
for k in commands:
    print("  %s - %s"%(k, str(commands[k])))
    device.commands.add_value(k, commands[k])

print("--- TELEMETRY")
for k in telemetry:
    print("  %s - %s"%(k, str(telemetry[k])))
    device.data.add_value(k, telemetry[k])

# Connect to the broker.
print("Connecting to broker...")
device.connect(config['mqtt']['host'],
            config['mqtt']['port'],
            config['mqtt']['user'],
            config['mqtt']['pass'])

# Send some telemetry values

value = 0   # Simple device field value ( simple counter )

for i in range(6):

    #Update the data value
    print("Sending data - value : %d" % value)
    device.data.update_value("value", value)
    value += 1

    # Send data values
    device.publish_data()

    # Sleep some time
    time.sleep(5)

# Disconnect device
print("Disconnecting device")
device.disconnect()

print("Application finished !")