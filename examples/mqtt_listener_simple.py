import os
import paho.mqtt.client as mqtt
import datetime
from mqtt_spb_wrapper import SpbPayloadParser

# APPLICATION configuration parameters -----------------------------------------------

DECODE_PAYLOAD = True      # Enable / disable payload

_TOPIC = "spBv1.0/#"    # All topics

_config_mqtt_host = os.environ.get("MQTT_HOST", "localhost")
_config_mqtt_port = int(os.environ.get("MQTT_PORT", 1883))
_config_mqtt_user = os.environ.get("MQTT_USER", "")
_config_mqtt_pass = os.environ.get("MQTT_PASS", "")

_config_mqtt_topic = os.environ.get("MQTT_TOPIC", _TOPIC)

_payload_parser = SpbPayloadParser()      # Payload parser


def on_connect(client, userdata, flags, rc):
    """
        MQTT Callback function for connect events
    """
    if rc == 0:
        topic = _config_mqtt_topic
        client.subscribe(topic)
        print("MQTT Client connected and subscribed to topic: " + topic)
    else:
        print("MQTT Client failed to connect with result code " + str(rc))


def on_message(client, userdata, msg):
    """
        MQTT Callback function for received messages events
    """
    global _payload_parser, DECODE_PAYLOAD

    _data = ""

    if DECODE_PAYLOAD:
        try:
            _data = str(_payload_parser.parse_payload(msg.payload))
        except:
            pass

    print(datetime.datetime.utcnow().isoformat() + " " + msg.topic + "\n\t\t" + _data)


# Set up the MQTT client connection that will listen to all Sparkplug B messages
client = mqtt.Client()
if _config_mqtt_user != "":
    client.username_pw_set(_config_mqtt_user, _config_mqtt_pass)
client.on_connect = on_connect
client.on_message = on_message
client.connect(_config_mqtt_host, _config_mqtt_port)  # Connect to MQTT
print("Connecting to MQTT broker server - %s:%d - Username: %s" % (_config_mqtt_host, _config_mqtt_port, _config_mqtt_user))

# Loop forever
client.loop_forever()

