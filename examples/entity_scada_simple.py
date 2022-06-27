#
#    --- Simple Sparkplub B SCADA application example ---
#
#   The application will create an spB SCADA application node to receive all domain messages and
#   display them in the console. The application will also keep status of current domain entities.
#   Also, the SCADA node will send periodically ping messages to active entities.
#
#   Notes:
#       - To set MQTT broker parameters, use config.yml file
#       - To set spB domain and eon ids, use config.yml file.
#

import time
from mqtt_spb_wrapper import *
import yaml

_DEBUG = True   # Enable debug messages
_SCADA_ID = "SCADA01"  # Current ID for SCADA

print("--- Sparkplug B example - SCADA Entity Simple")

# Load application configuration file
try:
    with open("config.yml") as fr:
        config = yaml.load(fr, yaml.FullLoader)
    print("Configuration file loaded")
except:
    print("ERROR - Could not load config file")
    exit()

entities = {}   # List of detected entities

#Callback function on spB received messages - Used to discover the spB entities
def callback_scada_message(topic : MqttSparkPlugB_Topic, payload):

    global entities

    print("SCADA received DATA: (%s) %s - %s"%( topic.entity_id, topic, payload))

    # Save discovered entities status
    entity_id = topic.entity_id

    #IF valid entity and not SCADA
    if entity_id and entity_id != _SCADA_ID:

        #Create entity if never discovered
        if entity_id not in entities.keys():
            entities[entity_id] = MqttSparkPlugB_Entity( domain_id=topic.domain_id,
                                                         edge_node_id=topic.edge_node_id,
                                                         device_id=topic.device_id,
                                                         debug_info=_DEBUG)

        entity : MqttSparkPlugB_Entity = entities[entity_id]    # Reference

        #Update entity status based on message type
        if topic.message_type.endswith("BIRTH") :

            entity.is_alive = True  # Update status

            # Parse data fields
            for field in payload['metrics']:

                # Do not parse secuence fields
                if field['name'] == "bdSeq":
                    continue

                # Get field information
                type = field['name'].split("/")[0]
                name = field['name'].split("/")[1]
                value = field['value']

                #Update entity field value
                if type == "ATTR":
                    entity.attribures.add_value(name, value)
                elif type == "DATA":
                    entity.data.add_value(name, value)
                elif type == "CMD":
                    entity.commands.add_value(name, value)

        elif topic.message_type.endswith("DEATH"):

            entity.is_alive = False  # Update status

        elif topic.message_type.endswith("DATA"):

            # Parse data fields
            for field in payload['metrics']:

                # Get field information
                name = field['name']
                value = field['value']

                #Update entity field value
                entity.data.add_value(name, value)

scada = MqttSparkPlugB_Entity_SCADA( domain_id=config['sparkplugb']['domain_id'],
                                     scada_id=_SCADA_ID,
                                     debug_info=_DEBUG)
# Set callbacks
scada.on_message = callback_scada_message

# ATTRIBUTES
scada.attribures.add_value("description", "SCADA application simple")

# Connect to the broker.
print("Connecting to broker...")
scada.connect(config['mqtt']['host'],
            config['mqtt']['port'],
            config['mqtt']['user'],
            config['mqtt']['pass'])

# Loop forever
while True:

    #Sleep some time
    time.sleep(10)

    # Print entities status and send some test command
    print("--- SCADA current entities:")
    for entity in entities.keys():

        print(" ", entity, entities[entity].is_alive)

        # Send a ping CMD to active entities
        if( entities[entity].is_alive):

            print("    Sending ping CMD")

            #IF entity EoN Device
            if entities[entity].device_id:
                scada.publish_command_device( entities[entity].edge_node_id, entities[entity].device_id, {"ping": True})

            elif entities[entity].edge_node_id:
                scada.publish_command_edge_node( entities[entity].edge_node_id, {"ping": True})