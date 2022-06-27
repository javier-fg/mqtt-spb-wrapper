#
#    --- Simple Sparkplub B listener pplication example ---
#
#   The application will create an spB application EoN node to receive all domain messages and
#   display them in the console.
#
#   Notes:
#       - To set MQTT broker parameters, use config.yml file
#       - To set spB domain and eon ids, use config.yml file.
#

import time
from mqtt_spb_wrapper import *
import yaml

_DEBUG = True   # Enable debug messages

print("--- Sparkplug B example - Application Entity Listener")

# Load application configuration file
try:
    with open("config.yml") as fr:
        config = yaml.load(fr, yaml.FullLoader)
    print("Configuration file loaded")
except:
    print("ERROR - Could not load config file")
    exit()

def callback_app_message(topic, payload):
    print("APP received MESSAGE: %s - %s"%(topic, payload))

def callback_app_command(payload):
    print("APP received CMD: %s"%(payload))

    # Parse commands
    for cmd in payload['metrics']:

        # Parse fields
        name = cmd["name"]
        value = cmd["value"]

        #Parse commands
        if name == "ping" and value:  # Ping command

            #Send response
            app.data.update_value("ping", True)
            app.publish_data()

            print("  CMD Ping received - Sending response")


app = MqttSparkPlugB_Entity_Application( domain_id=config['sparkplugb']['domain_id'],
                                         app_entity_id="ListenApp01",
                                         debug_info=_DEBUG)
# Set callbacks
app.on_message = callback_app_message
#app.on_command = callback_app_command

# ATTRIBUTES
app.attribures.add_value("Info", "Test application")

# COMMANDS
app.commands.add_value("ping", False)
app.data.add_value("ping", False)

# Connect to the broker.
print("Connecting to broker...")
app.connect(config['mqtt']['host'],
            config['mqtt']['port'],
            config['mqtt']['user'],
            config['mqtt']['pass'])

# Loop forever
while True:
    time.sleep(1000)
