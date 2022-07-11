#
#    --- Simple Sparkplub B SCADA application example ---
#
#   The application will create an spB SCADA application node to receive all group messages and
#   display them in the console. The application will also keep status of current group entities.
#   Also, the SCADA node will send periodically ping messages to active entities.
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
_config_spb_scada_name = "SCADA01"

# MQTT Configuration
_config_mqtt_topic = "#"    # Topic to listen
_config_mqtt_host = os.environ.get("MQTT_HOST", "localhost")
_config_mqtt_port = int(os.environ.get("MQTT_PORT", 1883))
_config_mqtt_user = os.environ.get("MQTT_USER", "")
_config_mqtt_pass = os.environ.get("MQTT_PASS", "")


print("--- Sparkplug B example - SCADA Entity Simple")

# Global variables ----------------------------------------

entities = {}   # List of detected spB entities


def callback_scada_message(topic : MqttSpbTopic, payload):
    """
        Callback function on spB received messages - Used to discover the spB entities
    """

    global entities

    print("SCADA received DATA: (%s) %s - %s"%( topic.entity_name, topic, payload))

    # Save discovered entities status
    entity_name = topic.entity_name

    # If valid entity and not SCADA
    if entity_name and entity_name != _config_spb_scada_name:

        # Create entity if never discovered before
        if entity_name not in entities.keys():
            entities[entity_name] = MqttSpbEntity(spb_group_name=topic.group_name,
                                                  spb_eon_name=topic.eon_name,
                                                  spb_eon_device_name=topic.eon_device_name,
                                                  debug_info=_DEBUG)

        entity: MqttSpbEntity = entities[entity_name]    # Reference

        # Update entity status based on message _type
        if topic.message_type.endswith("BIRTH") :

            entity.is_alive = True  # Update status

            # Parse data fields
            for field in payload['metrics']:

                # Do not parse sequence fields
                if field['name'] == "bdSeq":
                    continue

                # Get field information
                _type = field['name'].split("/")[0]
                _name = field['name'].split("/")[1]
                _value = field['value']

                # Update entity field value
                if _type == "ATTR":
                    entity.attribures.set_value(_name, _value)
                elif _type == "DATA":
                    entity.data.set_value(_name, _value)
                elif _type == "CMD":
                    entity.commands.set_value(_name, _value)

        elif topic.message_type.endswith("DEATH"):

            entity.is_alive = False  # Update status

        elif topic.message_type.endswith("DATA"):

            # Parse data fields
            for field in payload['metrics']:

                # Get field information
                name = field['name']
                value = field['value']

                # Update entity field value
                entity.data.set_value(name, value)


# Create the SCADA entity to listen to all spB messages
scada = MqttSpbEntityScada(spb_group_name= _config_spb_group_name,
                           spb_scada_name= _config_spb_scada_name,
                           debug_info=_DEBUG)

# Set callbacks
scada.on_message = callback_scada_message

# ATTRIBUTES
scada.attribures.set_value("description", "SCADA application simple")

# Connect to the broker.
print("Connecting to data broker %s:%d ..." % (_config_mqtt_host, _config_mqtt_port))
scada.connect(_config_mqtt_host,
              _config_mqtt_port,
              _config_mqtt_user,
              _config_mqtt_pass)

# Loop forever
while True:

    # Sleep some time
    time.sleep(10)

    # Print entities status and send some test command
    print("--- SCADA current entities:")
    for entity in entities.keys():

        print(" ", entity, entities[entity].is_alive)

        # Send a ping CMD to active entities
        if entities[entity].is_alive:

            print("    Sending ping CMD")

            # If entity EoN Device
            if entities[entity].spb_eon_device_name:
                scada.publish_command_device( entities[entity].spb_eon_name, entities[entity].spb_eon_device_name, {"ping": True})

            elif entities[entity].spb_eon_name:
                scada.publish_command_edge_node( entities[entity].spb_eon_name, {"ping": True})