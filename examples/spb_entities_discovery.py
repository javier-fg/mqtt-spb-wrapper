#
#    --- Sparkplub B - Discovery application ---
#
#   The application will display information about all spB entities
#
import os
import time
import json
import paho.mqtt.client as mqtt
from mqtt_spb_wrapper import *

# APPLICATION default configuration parameters -----------------------------------------------
_DEBUG = False   # Enable debug messages

# Sparkplug B parameters
_config_spb_domain_name = os.environ.get("SPB_DOMAIN", "*")

# MQTT Configuration
_config_mqtt_host = os.environ.get("MQTT_HOST", "localhost")
_config_mqtt_port = int(os.environ.get("MQTT_PORT", 1883))
_config_mqtt_user = os.environ.get("MQTT_USER", "")
_config_mqtt_pass = os.environ.get("MQTT_PASS", "")

# Global Variables -------------------------------------------------------
spb_groups = {}    # Variable to store the discord groups and entities

topic = SpbTopic()  # spB Topic parser


def on_connect(client, userdata, flags, rc):
    """
        MQTT Callback function for connect events
    """
    if rc == 0:
        if _config_spb_domain_name == "*":
            topic = "spBv1.0/#"
        else:
            topic = "spBv1.0/%s/#" % _config_spb_domain_name

        # Subscribe to the specific topic
        client.subscribe(topic)
        print("MQTT Client connected and subscribed to topic: " + topic)

    else:
        print("MQTT Client failed to connect with result code " + str(rc))


def on_message(client, userdata, msg):
    """
        MQTT Callback function for received messages events
    """

    global topic, spb_groups

    # print(msg.topic)

    # Parse the topic
    topic.parse_topic(msg.topic)

    group_name = topic.group_name
    eon_name = topic.eon_name
    eon_device_name = topic.eon_device_name
    entity_name = topic.entity_name

    # Check if the group is present in the ones detected
    if group_name not in spb_groups.keys():
        spb_groups[group_name] = {}

    # Check if the eon is present in the group
    if eon_name not in spb_groups[group_name].keys():
        spb_groups[group_name][eon_name] = {}

    # Check if the entity is present in the group and eon
    if entity_name not in spb_groups[group_name][eon_name].keys():
        entity = MqttSpbEntity( spb_domain_name= group_name,
                                spb_eon_name= eon_name,
                                spb_eon_device_name=eon_device_name,
                                debug_info=_DEBUG)
        spb_groups[group_name][eon_name][entity_name] = entity     # Save the reference
    else:
        entity = spb_groups[group_name][eon_name][entity_name]  # Get the reference

    # BIRTH,  parse the data into the entity object, for later display
    if "BIRTH" in topic.message_type:

        # Parse payload
        entity.deserialize_payload_birth(msg.payload)


print("--- Sparkplug B entity discovery application ---")

# Set up the MQTT client connection that will listen to all Sparkplug B messages
_mqtt_client = mqtt.Client()
if _config_mqtt_user != "":
    _mqtt_client.username_pw_set(_config_mqtt_user, _config_mqtt_pass)
_mqtt_client.on_connect = on_connect
_mqtt_client.on_message = on_message
_mqtt_client.connect(_config_mqtt_host, _config_mqtt_port)  # Connect to MQTT
print("Connecting to MQTT broker server - %s:%d" % (_config_mqtt_host, _config_mqtt_port))

# Start the mqtt client thread
_mqtt_client.loop_start()

# Wait some time to receive the persisted messages in the broker.
time.sleep(4)

# Stop the mqtt client
_mqtt_client.loop_stop()

# Let's show the result
PIPE = "│  "
ELBOW = "└──"
TEE = "├──"
SPACE = "   "

out_dict = {}   # output data

for group_name in spb_groups.keys():

    print("\nspBv1.0 / %s" % group_name)

    if group_name not in out_dict.keys():    # Create if it doesn't exist
        out_dict[group_name] = {}

    for eon_name in spb_groups[group_name].keys():

        # Check if last item
        L1_last = (eon_name == list(spb_groups[group_name].keys())[-1])
        if L1_last:
            L1 = ELBOW
        else:
            L1 = TEE

        # ├── EON
        print("  %s %s" % (L1, eon_name))

        # Create if it doesn't exist
        if eon_name not in out_dict[group_name].keys():
            out_dict[group_name][eon_name] = []

        for entity_name in spb_groups[group_name][eon_name].keys():

            # ├── EON
            # │    ├── ENTITY
            L2_last = (entity_name == list(spb_groups[group_name][eon_name].keys())[-1])
            if L1_last:
                L1 = SPACE
            else:
                L1 = PIPE

            if L2_last:  # Check if last item
                L2 = ELBOW
            else:
                L2 = TEE

            entity = spb_groups[group_name][eon_name][entity_name]  # Get entity object

            print("  %s %s %s" % (L1, L2, entity_name))

            # Add the entity information
            out_dict[group_name][eon_name].append(entity.get_dictionary())

            if L2_last:
                L2 = SPACE
            else:
                L2 = PIPE

            MAX_STR_LEN = 40    # Display only a maximum of characters

            # Print entity Attributes, commands, data
            print("  %s %s %s ATTR/" % (L1, L2, TEE))
            attributes = entity.attributes.get_dictionary()
            if len(attributes) != 0:
                for field in attributes:
                    _value = field['value']
                    if isinstance(_value, str):
                        if len(_value) > MAX_STR_LEN:
                            _value = _value[:MAX_STR_LEN].replace("\n", "").replace("\r", "") + " ..."
                        else:
                            _value = _value
                    else:
                        _value = str(_value)
                    print("  %s %s %s - %s : %s (%s)" % (L1, L2, PIPE, field['name'], _value, type(field['value'])))

            print("  %s %s %s CMD/" % (L1, L2, TEE))
            commands = entity.commands.get_dictionary()
            if len(commands) != 0:
                for field in commands:
                    _value = field['value']
                    if isinstance(_value, str):
                        if len(_value) > MAX_STR_LEN:
                            _value = _value[:MAX_STR_LEN].replace("\n", "").replace("\r", "") + " ..."
                        else:
                            _value = _value
                    else:
                        _value = str(_value)
                    print("  %s %s %s - %s : %s (%s)" % (L1, L2, PIPE, field['name'], _value, type(field['value'])))

            print("  %s %s %s DATA/" % (L1, L2, ELBOW))
            data = entity.data.get_dictionary()
            if len(data) != 0:
                for field in data:
                    _value = field['value']
                    if isinstance(_value, str):
                        if len(_value) > MAX_STR_LEN:
                            _value = _value[:MAX_STR_LEN].replace("\n", "").replace("\r", "") + " ..."
                        else:
                            _value = _value
                    else:
                        _value = str(_value)
                    print("  %s %s %s - %s : %s (%s)" % (L1, L2, SPACE, field['name'], _value, type(field['value'])))

# Save discovery results into json file
filename = "spb_discovery_results.json"
print("\nSaving results to file: " + filename)
with open(filename, "w") as fw:
    fw.write( json.dumps(out_dict))
