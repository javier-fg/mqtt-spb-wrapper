#
#    --- Simple Sparkplub B listener pplication example ---
#
#   The application will create an spB application EoN node to receive all group messages and
#   display them in the console.
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
_config_spb_group_name = "GroupTest"
_config_spb_app_name = "ListenApp01"

# MQTT Configuration
_config_mqtt_topic = "#"    # Topic to listen
_config_mqtt_host = os.environ.get("MQTT_HOST", "localhost")
_config_mqtt_port = int(os.environ.get("MQTT_PORT", 1883))
_config_mqtt_user = os.environ.get("MQTT_USER", "")
_config_mqtt_pass = os.environ.get("MQTT_PASS", "")


def callback_app_message(topic, payload):
    """
        Callback for received messages events
    """
    print("APP received MESSAGE: %s - %s" % (topic, payload))


def callback_app_command(payload):
    """
        Callback for received commands events
    """
    print("APP received CMD: %s" % payload)

    # Parse commands
    for cmd in payload['metrics']:

        # Parse fields
        name = cmd["name"]
        value = cmd["value"]

        # Parse commands
        if name == "ping" and value:  # Ping command

            # Send response
            app.data.update_value("ping", True)
            app.publish_data()

            print("  CMD Ping received - Sending response")

print("--- Sparkplug B example - Application Entity Listener")

# Create the spB application entity
app = MqttSpbEntityApplication(spb_group_name=_config_spb_group_name,
                               spb_app_entity_name=_config_spb_app_name,
                               debug_info=_DEBUG)
# Set callbacks
app.on_message = callback_app_message
app.on_command = callback_app_command

# ATTRIBUTES
app.attribures.set_value("Info", "Test application")

# COMMANDS
app.commands.set_value("ping", False)
app.data.set_value("ping", False)

# Connect to the broker.
print("Connecting to data broker %s:%d ..." % (_config_mqtt_host, _config_mqtt_port))
app.connect(_config_mqtt_host,
            _config_mqtt_port,
            _config_mqtt_user,
            _config_mqtt_pass)

# Loop forever
while True:
    time.sleep(1000)
