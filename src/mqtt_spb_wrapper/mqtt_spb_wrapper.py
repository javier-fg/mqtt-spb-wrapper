import time
import datetime
import logging
from typing import Dict, Any

import paho.mqtt.client as mqtt
from google.protobuf.json_format import MessageToDict

from spb_core import getDdataPayload, getNodeDeathPayload, getNodeBirthPayload, getDeviceBirthPayload
from spb_core import getSeqNum, getBdSeqNum
from spb_core import addMetric, MetricDataType
from spb_core import Payload

# Application logger
logger = logging.getLogger("MQTTSPB_ENTITY")
logger.setLevel(logging.DEBUG)
_log_handle = logging.StreamHandler()
_log_handle.setFormatter(logging.Formatter('%(asctime)s %(name)s %(levelname)s | %(message)s'))
logger.addHandler(_log_handle)


class MqttSpbTopic:
    """
        Class used to parse MQTT topic string and discover all different Sparkplug b entities
    """

    def __init__(self, topic_str= None):

        self.topic = ""

        self.namespace = None

        self.group_name = None
        self.message_type = None
        self.eon_name = None
        self.eon_device_name = None

        self.entity_name = None

        self.domain = None

        if topic_str is not None:
            self.parse_topic(topic_str)

    def __str__(self):
        return str(self.topic)

    def __repr__(self):
        return str(self.topic)

    def parse_topic(self, topic_str):

        topic_fields = topic_str.split('/')  # Get the topic

        self.topic = topic_str

        self.namespace = topic_fields[0]
        self.group_name = topic_fields[1]
        self.message_type = topic_fields[2]
        self.eon_name = None
        self.eon_device_name = None

        self.entity_name = None

        # If EoN
        if len(topic_fields) > 3:
            self.eon_name = topic_fields[3]
            self.entity_name = self.eon_name

        # If EoN device type
        if len(topic_fields) > 4:
            self.eon_device_name = topic_fields[4]
            self.entity_name = self.eon_device_name

        self.domain = "%s.%s.%s" % ( self.namespace, self.group_name, self.eon_name)
        if self.eon_device_name is not None:
            self.domain += ".%s" % self.eon_device_name

        return str(self)


class MqttSpbPayload:
    """
        Class to parse binary payloads into dictionary
    """

    def __init__(self, payload_data=None):

        self.payload = None

        # If data is passed, then process it
        if payload_data is not None:
            self.parse_payload(payload_data)

    def __str__(self):
        return str(self.payload)

    def __repr__(self):
        return str(self.payload)

    def asDict(self):
        return dict(self.payload)

    def parse_payload(self, payload_data):
        """
           Parse MQTT sparkplug B payload bytes ( protobuff ) into JSON
        :param payload_data: bytes ( protobuff )
        :return:  Dictionary or None if fails
        """
        pb_payload = Payload()

        try:
            pb_payload.ParseFromString(payload_data)
            payload = MessageToDict(pb_payload)  # Convert it to DICT for easy handeling

            # Add the metrics [TYPE_value] field into [value] field for convenience
            if "metrics" in payload.keys():
                for i in range(len(payload['metrics'])):
                    for k in payload['metrics'][i].keys():
                        if "Value" in k:
                            payload['metrics'][i]['value'] = payload['metrics'][i][k]
                            break

        except Exception as e:

            # Check if payload is from SCADA
            try:
                _payload = payload_data.decode()
            except Exception as e1:
                logger.error("Could not parse MQTT CMD payload, message ignored ! (reason: %s)" % (str(e1)))
                return None

            if _payload == "OFFLINE" or _payload == "ONLINE":
                self.payload = _payload
                return self.payload

            logger.error("Could not parse MQTT CMD payload, message ignored ! (reason: %s)" % (str(e)))
            print(payload_data.decode())
            return None

        self.payload = payload  # Save the current payload
        return self.payload


class MqttSpbEntity:

    def __init__(self,
                 spb_domain_name, spb_eon_name,
                 spb_eon_device_name= None,
                 debug_info=False,
                 filter_cmd_msg=True,
                 entity_is_scada=False):

        # Enable / disable the class logger messages
        if debug_info:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.ERROR)

        # Public members -----------

        self.is_birth_published = False

        self.attributes = self._ValuesGroup()
        self.data = self._ValuesGroup()
        self.commands = self._ValuesGroup()

        self.on_command = None  # Callback function when a comand is received
        self.on_connect = None
        self.on_disconnect = None
        self.on_message = None

        # Private members -----------

        self._spb_domain_name = spb_domain_name
        self._spb_eon_name = spb_eon_name
        self._spb_eon_device_name = spb_eon_device_name

        if spb_eon_device_name is None:
            self._entity_domain = "spBv.10.%s.%s" % (self._spb_domain_name, self._spb_eon_name)
        else:
            self._entity_domain = "spBv.10.%s.%s.%s" % (self._spb_domain_name, self._spb_eon_name, self._spb_eon_device_name)

        if spb_eon_device_name is None:
            self._entity_name = self._spb_eon_name
        else:
            self._entity_name = self._spb_eon_device_name

        self._mqtt = None  # Mqtt client object

        self._loopback_topic = ""   # Last publish topic, to avoid ñoopback message reception

        self._filter_cmd_msg = filter_cmd_msg
        self._entity_is_scada = entity_is_scada

        logger.info("%s - New entity created " % self._entity_domain)

    def __str__(self):
        return str(self.get_dictionary())

    def __repr__(self):
        return str(self.get_dictionary())

    def get_dictionary(self):
        temp = {}
        temp['spb_domain_name'] = self._spb_domain_name
        temp['spb_eon_name'] = self._spb_eon_name
        if self._spb_eon_device_name is not None:
            temp['spb_eon_device_name'] = self._spb_eon_device_name
        temp['data'] = self.data.get_dictionary()
        temp['attributes'] = self.attributes.get_dictionary()
        temp['commands'] = self.commands.get_dictionary()
        return temp

    def is_empty(self):
        if self.data.is_empty() and self.attributes.is_empty() and self.commands.is_empty():
            return True
        return False

    @property
    def spb_domain_name(self):
        return self._spb_domain_name

    @property
    def spb_eon_name(self):
        return self._spb_eon_name

    @property
    def spb_eon_device_name(self):
        return self._spb_eon_device_name

    @property
    def entity_name(self):
        return self._entity_name

    @property
    def entity_domain(self):
        return self._entity_domain


    def _spb_data_type(self, data):
        if isinstance(data, str):
            return MetricDataType.Text
        elif isinstance(data, bool):
            return MetricDataType.Boolean
        elif isinstance(data, int):
            return MetricDataType.Double
        elif isinstance(data, float):
            return MetricDataType.Double
        elif isinstance(data, bytes) or isinstance(data, bytearray):
            return MetricDataType.Bytes

        return MetricDataType.Unknown

    def serialize_payload_birth(self):
        """
            Serialize the BIRTH message and get payload bytes
        """

        if self._spb_eon_device_name is None:  # EoN type
            payload = getNodeBirthPayload()
        else:  # Device
            payload = getDeviceBirthPayload()

        # Attributes
        if not self.attributes.is_empty():
            for item in self.attributes.values:
                name = "ATTR/" + item.name
                addMetric(payload, name, None, self._spb_data_type(item.value), item.value, item.timestamp)

        # Data
        if not self.data.is_empty():
            for item in self.data.values:
                name = "DATA/" + item.name
                addMetric(payload, name, None, self._spb_data_type(item.value), item.value, item.timestamp)

        # Commands
        if not self.commands.is_empty():
            for item in self.commands.values:
                name = "CMD/" + item.name
                addMetric(payload, name, None, self._spb_data_type(item.value), item.value, item.timestamp)

        payload_bytes = bytearray(payload.SerializeToString())

        return payload_bytes

    def deserialize_payload_birth(self, data_bytes):

        payload = MqttSpbPayload(data_bytes).payload

        if payload is not None:

            # Iterate over the metrics to update the data fields
            for field in payload.get('metrics', []):

                if field['name'].startswith("ATTR/"):
                    field['name'] = field['name'][5:]
                    self.attributes.set_value(field['name'], field['value'], field['timestamp'])  # update field

                elif field['name'].startswith("CMD/"):
                    field['name'] = field['name'][4:]
                    self.commands.set_value(field['name'], field['value'], field['timestamp'])  # update field

                elif field['name'].startswith("DATA/"):
                    field['name'] = field['name'][5:]
                    self.data.set_value(field['name'], field['value'], field['timestamp'])  # update field

        return payload

    def serialize_payload_data(self, send_all=False):

        # Get a new payload object to add metrics to it.
        payload = getDdataPayload()

        # Iterate for each data field.
        for item in self.data.values:

            # Only send those values that have been updated, or if send_all==True then send all.
            if send_all or item.is_updated:
                addMetric(payload, item.name, None, self._spb_data_type(item.value), item.value, item.timestamp)

        payload_bytes = bytearray(payload.SerializeToString())

        return payload_bytes

    def deserialize_payload_data(self, data_bytes):

        payload = MqttSpbPayload(data_bytes).payload

        if payload is not None:
            #Iterate over the metrics to update the data fields
            for field in payload.get('metrics', []):
                self.data.set_value(field['name'], field['value'], field['timestamp']) # update field

        return payload

    def publish_birth(self, QoS=0):

        if not self.is_connected():  # If not connected
            logger.warning("%s - Could not send publish_birth(), not connected to MQTT server" % self._entity_domain)
            return False

        if self.is_empty():  # If no data (Data, attributes, commands )
            logger.warning(
                "%s - Could not send publish_birth(), entity doesn't have data ( attributes, data, commands )" % self._entity_domain)
            return False

        # If it is a type entity SCADA, change the BIRTH certificate
        if self._entity_is_scada:
            topic = "spBv1.0/" + self.spb_domain_name + "/STATE/" + self._spb_eon_name
            self._loopback_topic = topic
            self._mqtt.publish(topic, "ONLINE".encode("utf-8"), QoS, True)
            logger.info("%s - Published STATE BIRTH message " % (self._entity_domain))
            return

        # Publish BIRTH message
        payload_bytes = self.serialize_payload_birth()
        if self._spb_eon_device_name is None:  # EoN
            topic = "spBv1.0/" + self.spb_domain_name + "/NBIRTH/" + self._spb_eon_name
        else:
            topic = "spBv1.0/" + self.spb_domain_name + "/DBIRTH/" + self._spb_eon_name + "/" + self._spb_eon_device_name
        self._loopback_topic = topic
        self._mqtt.publish(topic, payload_bytes, QoS, True)

        logger.info("%s - Published BIRTH message" % (self._entity_domain))

        self.is_birth_published = True

    def publish_data(self, send_all=False, QoS=0):
        """
            Send the new updated data to the MQTT broker as a Sparkplug B DATA message.

        :param send_all: boolean    True: Send all data fields, False: send only updated field values.
        :return:                    result
        """

        if not self.is_connected():  # If not connected
            logger.warning(
                "%s - Could not send publish_telemetry(), not connected to MQTT server" % self._entity_domain)
            return False

        if self.is_empty():  # If no data (Data, attributes, commands )
            logger.warning(
                "%s - Could not send publish_telemetry(), entity doesn't have data ( attributes, data, commands )" % self._entity_domain)
            return False

        # Send payload if there is new data or we need to send all
        if send_all or self.data.is_updated():

            payload_bytes = self.serialize_payload_data(send_all)   # Get the data payload

            if self._spb_eon_device_name is None:  # EoN
                topic = "spBv1.0/" + self.spb_domain_name + "/NDATA/" + self._spb_eon_name
            else:
                topic = "spBv1.0/" + self.spb_domain_name + "/DDATA/" + self._spb_eon_name + "/" + self._spb_eon_device_name
            self._loopback_topic = topic
            self._mqtt.publish(topic, payload_bytes, QoS, False)

            logger.info("%s - Published DATA message %s" % (self._entity_domain, topic))
            return True

        logger.warning("%s - Could not publish DATA message, may be data no new data values?" % (self._entity_domain))
        return False

    def connect(self,
                host='localhost',
                port=1883,
                user="",
                password="",
				use_tls=False,
                tls_ca_path="",
                tls_cert_path="",
                tls_key_path="",
                tls_insecure=False,
                timeout=5):



        # If we are already connected, then exit
        if self.is_connected():
            return True

        # MQTT Client configuration
        if self._mqtt is None:
            self._mqtt = mqtt.Client(userdata=self)

        self._mqtt.on_connect = self._mqtt_on_connect
        self._mqtt.on_disconnect = self._mqtt_on_disconnect
        self._mqtt.on_message = self._mqtt_on_message

        if user != "":
            self._mqtt.username_pw_set(user, password)

        # If client certificates are provided
        if tls_ca_path and tls_cert_path and tls_key_path:
            logger.debug("Setting CA client certificates")

            if tls_insecure:
                logger.debug("Setting CA client certificates - IMPORTANT CA insecure mode ( use only for testing )")
                import ssl
                self._mqtt.tls_set(ca_certs=tls_ca_path, certfile=tls_cert_path, keyfile=tls_key_path, cert_reqs=ssl.CERT_NONE)
                self._mqtt.tls_insecure_set(True)
            else:
                logger.debug("Setting CA client certificates")
                self._mqtt.tls_set(ca_certs=tls_ca_path, certfile=tls_cert_path, keyfile=tls_key_path)

        #If only CA is proviced.
        elif tls_ca_path:
            logger.debug("Setting CA certificate")
            self._mqtt.tls_set(ca_certs=tls_ca_path)

        # If TLS is enabled
        else:
            if use_tls:
                self._mqtt.tls_set()    # Enable TLS encryption

        # Entity DEATH message - last will message
        if self._entity_is_scada:  # If it is a type entity SCADA, change the DEATH certificate
            topic = "spBv1.0/" + self.spb_domain_name + "/STATE/" + self._spb_eon_name
            self._mqtt.will_set(topic, "OFFLINE".encode("utf-8"), 0, True)  # Set message
        else:  # Normal node
            payload = getNodeDeathPayload()
            payload_bytes = bytearray(payload.SerializeToString())
            if self._spb_eon_device_name is None:  # EoN
                topic = "spBv1.0/" + self.spb_domain_name + "/NDEATH/" + self._spb_eon_name
            else:
                topic = "spBv1.0/" + self.spb_domain_name + "/DDEATH/" + self._spb_eon_name + "/" + self._spb_eon_device_name
            self._mqtt.will_set(topic, payload_bytes, 0, True)  # Set message



        # MQTT Connect
        logger.info("%s - Trying to connect MQTT server %s:%d" % (self._entity_domain, host, port))
        try:
            self._mqtt.connect(host, port)
        except Exception as e:
            logger.warning("%s - Could not connect to MQTT server (%s)" % (self._entity_domain, str(e)))
            return False

        self._mqtt.loop_start()  # Start MQTT background task
        time.sleep(0.1)

        # Wait some time to get connected
        _timeout = time.time() + timeout
        while not self.is_connected() and _timeout > time.time():
            time.sleep(0.1)

        # Return if we connected successfully
        return self.is_connected()

    def disconnect(self, skip_death_publish=False):

        logger.info("%s - Disconnecting from MQTT server" % (self._entity_domain))

        if self._mqtt is not None:

            # Send the DEATH message
            if not skip_death_publish:
                if self._entity_is_scada:  # If it is a type entity SCADA, change the DEATH certificate
                    topic = "spBv1.0/" + self.spb_domain_name + "/STATE/" + self._spb_eon_name
                    self._mqtt.publish(topic, "OFFLINE".encode("utf-8"), 0, False)
                else:  # Normal node
                    payload = getNodeDeathPayload()
                    payload_bytes = bytearray(payload.SerializeToString())
                    if self._spb_eon_device_name is None:  # EoN
                        topic = "spBv1.0/" + self.spb_domain_name + "/NDEATH/" + self._spb_eon_name
                    else:
                        topic = "spBv1.0/" + self.spb_domain_name + "/DDEATH/" + self._spb_eon_name + "/" + self._spb_eon_device_name
                    self._mqtt.publish(topic, payload_bytes, 0, False)  # Set message

            # Disconnect from MQTT broker
            self._mqtt.loop_stop()
            time.sleep(0.1)
            self._mqtt.disconnect()
            time.sleep(0.1)
        self._mqtt = None

    def is_connected(self):
        if self._mqtt is None:
            return False
        else:
            return self._mqtt.is_connected()

    def _mqtt_on_connect(self, client, userdata, flags, rc):

        if rc == 0:
            logger.info("%s - Connected to MQTT server" % self._entity_domain)

            # Subscribing in on_connect() means that if we lose the connection and
            # reconnect then subscriptions will be renewed.
            if self._spb_eon_device_name is None:  # EoN
                topic = "spBv1.0/" + self.spb_domain_name + "/NCMD/" + self._spb_eon_name
            else:
                topic = "spBv1.0/" + self.spb_domain_name + "/DCMD/" + self._spb_eon_name + "/" + self._spb_eon_device_name
            self._mqtt.subscribe(topic)
            logger.info("%s - Subscribed to MQTT topic: %s" % (self._entity_domain, topic))

            # Subscribe to STATE of SCADA application
            topic = "spBv1.0/" + self.spb_domain_name + "/STATE/+"
            self._mqtt.subscribe(topic)
            logger.info("%s - Subscribed to MQTT topic: %s" % (self._entity_domain, topic))

        else:
            logger.error(" %s - Could not connect to MQTT server !" % self._entity_domain)

        # Execute the callback function if it is not None
        if self.on_connect is not None:
            self.on_connect(rc)

    def _mqtt_on_disconnect(self, client, userdata, rc):
        logger.info("%s - Disconnected from MQTT server" % self._entity_domain)

        # Execute the callback function if it is not None
        if self.on_disconnect is not None:
            self.on_disconnect(rc)

    def _mqtt_on_message(self, client, userdata, msg):

        # Check if loopback message
        if self._loopback_topic == msg.topic:
                return

        msg_ts_rx = int(datetime.datetime.utcnow().timestamp() * 1000)  # Save the current timestamp

        logger.info("%s - Message received  %s" % (self._entity_domain, msg.topic))

        # Parse the topic namespace ------------------------------------------------
        topic = MqttSpbTopic(msg.topic)  # Parse and get the topic object

        # Check that the namespace and group are correct
        # NOTE: Should not be because we are subscribed to an specific topic, but good to check.
        if topic.namespace != "spBv1.0" or topic.group_name != self._spb_domain_name:
            logger.error("%s - Incorrect MQTT spBv1.0 namespace and topic. Message ignored !" % self._entity_domain)
            return

        # Check if it is a STATE message from the SCADA application
        if topic.message_type == "STATE":
            # Execute the callback function if it is not None
            if self.on_message is not None:
                self.on_message(topic, msg.payload.decode("utf-8"))
            return

        # Parse the received ProtoBUF data ------------------------------------------------
        payload = MqttSpbPayload().parse_payload(msg.payload)

        # Add the timestamp when the message was received
        payload['timestamp_rx'] = msg_ts_rx

        # Execute the callback function if it is not None
        if self.on_message is not None:
            self.on_message(topic, payload)

        # Actions depending on the MESSAGE TYPE
        if "CMD" in topic.message_type:

            # Check if a list of commands is provided
            if "metrics" not in payload.keys():
                logger.warning(
                    "%s - Incorrect MQTT CMD payload, could not find any metrics. CMD message ignored !" % self._entity_domain)
                return

            # Filter out unknown or incorrect CMDs
            if self._filter_cmd_msg:
                removed = []
                new = []
                list_cmds = self.commands.get_name_list()

                for item in payload['metrics']:

                    # If not in the current list of known commands, removed it
                    if item['name'] not in list_cmds:
                        logger.warning(
                            "%s - Unrecognized CMD: %s - CMD will be ignored" % (self._entity_domain, item['name']))
                        removed.append(item['name'])

                    # Check if the datatypes match, otherwise ignore the command
                    elif not isinstance(item['value'], type(self.commands.get_value(item['name']))):
                        logger.warning("%s - Incorrect CMD datatype: %s - CMD will be ignored" % (
                            self._entity_domain, item['name']))
                        removed.append(item['name'])

                    # Include the right command in the new list
                    else:
                        new.append(item)

                # If there was items to be removed, update the metrics field with the new list
                if removed:
                    payload['metrics'] = new

            if not payload['metrics']:
                logger.error("%s - CMD message IGNORED, incorrect or no CMDs provided" % (self._entity_domain))
            else:
                # Execute the callback function if it is not None
                if self.on_command is not None:
                    self.on_command(payload)

    class _ValueItem:
        def __init__(self, name, value, timestamp=None):
            self.name = name
            self._value = value
            if timestamp is None:
                self._timestamp = int(datetime.datetime.utcnow().timestamp() * 1000)
            else:
                self._timestamp = timestamp
            self.is_updated = True

        def __str__(self):
            return str(self.get_dictionary())

        def __repr__(self):
            return str(self.get_dictionary())

        def get_dictionary(self):
            return {"timestamp": self.timestamp,
                    "name": self.name,
                    "value": self._value,
                    # "updated": self.is_updated
                    }

        @property
        def value(self):
            self.is_updated = False
            return self._value

        @value.setter
        def value(self, value):
            # if value != self._value:
            #     self.is_updated = True
            self.is_updated = True
            self._value = value

        @property
        def timestamp(self):
            return self._timestamp

        @timestamp.setter
        def timestamp(self, value):
            if value is None:
                self.timestamp_update()
            else:
                self._timestamp = int(value)

        def timestamp_update(self):
            self.timestamp = int(datetime.datetime.utcnow().timestamp() * 1000)

    class _ValuesGroup:

        def __init__(self):
            self.values = []
            self.seq_number = None

        def __str__(self):
            return str(self.get_dictionary())

        def __repr__(self):
            return str(self.get_dictionary())

        def get_dictionary(self):
            temp = []
            if len(self.values) > 0:
                for item in self.values:
                    temp.append(item.get_dictionary())
            return temp

        def get_name_list(self):
            temp = []
            if len(self.values) > 0:
                for item in self.values:
                    temp.append(item.name)
            return temp

        def get_value_list(self):
            temp = []
            if len(self.values) > 0:
                for item in self.values:
                    temp.append(item.value)
            return temp

        def keys(self):
            return self.get_name_list()
        def values(self):
            return self.get_value_list()

        def get_value(self, field_name):
            if len(self.values) > 0:
                for item in self.values:
                    if item.name == field_name:
                        return item.value
            return None

        def is_empty(self):
            if not self.values:
                return True
            return False

        def is_updated(self):
            for item in self.values:
                if item.is_updated:
                    return True
            return False

        def clear(self):
            self.values = []

        def set_value(self, name, value, timestamp=None):

            # If value is set to None, ignore the update
            if value is None:
                return False

            # If exist update the value, otherwise add the element.
            for item in self.values:
                if item.name == name:
                    item.value = value
                    item.timestamp = timestamp  # If timestamp is none, the current time will be used.
                    return True

            # item was not found, then add it to the list.
            self.values.append(MqttSpbEntityDevice._ValueItem(name, value, timestamp))
            return True

        def remove_value(self, name):
            # If exist remove the value by its name.
            original_count = len(self.values)
            self.values = [value for value in self.values if value.name != name]
            return len(self.values) < original_count

        def set_dictionary(self, values: dict, timestamp=None):
            """
                Import a list of values based on a dictionary FieldName:FieldValue
            :param values:      Dictionary with fields-values
            :param timestamp:   Timestamp value in ms
            :return:            Result
            """

            # Update the list of values
            for k,v in values.items():
                self.set_value(k, v, timestamp)

            return True


class MqttSpbEntityDevice(MqttSpbEntity):

    def __init__(self, spb_domain_name, spb_eon_name, spb_eon_device_name,
                 debug_info=False,
                 filter_cmd_msg=True):
        # Initialized the object ( parent class ) with Device_id as None - Configuring it as edge node
        super().__init__(spb_domain_name, spb_eon_name, spb_eon_device_name, debug_info, filter_cmd_msg)


class MqttSpbEntityEdgeNode(MqttSpbEntity):

    def __init__(self, spb_domain_name, spb_eon_name, debug_info=False):

        # Initialized the object ( parent class ) with Device_id as None - Configuring it as edge node
        super().__init__(spb_domain_name, spb_eon_name, None, debug_info)

    def publish_command_device(self, spb_eon_device_name, commands):

        if not self.is_connected():  # If not connected
            logger.warning(
                "%s - Could not send publish_command_device(), not connected to MQTT server" % self._entity_domain)
            return False

        if not isinstance(commands, dict):  # If no data commands as dictionary
            logger.warning(
                "%s - Could not send publish_command_device(), commands not provided or not valid. Please provide a dictionary of command:value" % self._entity_domain)
            return False

        # Get a new payload object, to add metrics
        payload = getDdataPayload()

        # Add the list of commands to the payload metrics
        for k in commands:
            addMetric(payload, k, None, self._spb_data_type(commands[k]), commands[k])

        # Send payload if there is new data
        topic = "spBv1.0/" + self.spb_domain_name + "/DCMD/" + self._spb_eon_name + "/" + spb_eon_device_name

        if payload.metrics:
            payload_bytes = bytearray(payload.SerializeToString())
            self._loopback_topic = topic
            self._mqtt.publish(topic, payload_bytes, 0, False)

            logger.info("%s - Published COMMAND message to %s" % (self._entity_domain, topic))

            return True

        logger.warning("%s - Could not publish COMMAND message to %s" % (self._entity_domain, topic))
        return False


class MqttSpbEntityApplication(MqttSpbEntityDevice):

    def __init__(self, spb_domain_name, spb_app_entity_name, debug_info=False):

        # Initialized the object ( parent class ) with Device_id as None - Configuring it as edge node
        super().__init__(spb_domain_name, spb_app_entity_name, None,
                         debug_info=debug_info,
                         filter_cmd_msg=False)

    # Override class method
    def _mqtt_on_connect(self, client, userdata, flags, rc):

        # Call the parent method
        super()._mqtt_on_connect(client, userdata, flags, rc)

        # Subscribe to all group topics
        if rc == 0:
            topic = "spBv1.0/" + self.spb_domain_name + "/#"
            self._mqtt.subscribe(topic)
            logger.info("%s - Subscribed to MQTT topic: %s" % (self._entity_domain, topic))


class MqttSpbEntityScada(MqttSpbEntity):

    class EntityScadaEdgeDevice (MqttSpbEntity):
        '''
            # Entity Scada Edge Device (EoND) class

            This class is used to represent virtually an EoND entity from a SCADA application.
            The scada application will automatically parse the received entity data into its
            representation, allowing you to easly interact with the entity ( via commands ) or
            to get the lates telemetry and attribute values.

            This class allows you to:
              - Subscribe to callback events when data from the entity is received ( BIRTH, DATA, DEATH )
              - Get the latest data values ( entity.data or entity.attributes )
              - Send a Command/s to the physical entity
        '''

        def __init__(self,
                     spb_domain_name, spb_eon_name, spb_eon_device_name,
                     scada_entity,
                     callback_birth=None, callback_data=None, callback_death=None,
                     debug_info=False):

            super().__init__(spb_domain_name=spb_domain_name,
                             spb_eon_name=spb_eon_name,
                             spb_eon_device_name=spb_eon_device_name,
                             debug_info=debug_info)

            self.scada = scada_entity  # Save reference to the scada entity

            self.callback_birth = callback_birth  # Save callback function references
            self.callback_data = callback_data
            self.callback_death = callback_death

            self._is_alive = False  # Device is alive ( BIRTH + DATA ) or not ( DEATH )

            # Console Logger
            self._logger = logging.getLogger("MQTTSPB_SCADA_EoN")
            handler_log = logging.StreamHandler()
            handler_log.setFormatter(logging.Formatter('%(asctime)s %(name)s %(levelname)s | %(message)s'))
            self._logger.addHandler(handler_log)
            if debug_info:
                self._logger.setLevel(logging.DEBUG)
            else:
                self._logger.setLevel(logging.ERROR)

        def is_alive(self):
            return self._is_alive

        def send_command(self, name, value, force=False):
            return self.send_commands({name: value}, force)

        def send_commands(self, commands, force=False):

            # Command Check - If not part of entity commands, through an error.
            if not force:
                for k in commands:
                    if k not in self.commands.get_name_list():
                        self._logger.error("%s - Command %s not sent, unknown device command." % (self._entity_domain, k))
                        return False

            # Send commands via SCADA application
            self.scada.send_commands(commands, self.spb_eon_name, self.spb_eon_device_name)

        def connect(self,
                host='localhost',
                port=1883,
                user="",
                password="",
				use_tls=False,
                tls_ca_path="",
                tls_cert_path="",
                tls_key_path="",
                tls_insecure=False,
                timeout=5):
            raise NotImplementedError("This method has been removed")

        def disconnect(self, skip_death_publish=False):
            raise NotImplementedError("This class method has been removed")

        def is_connected(self):
            raise NotImplementedError("This class method has been removed")

        def publish_birth(self, QoS=0):
            raise NotImplementedError("This class method has been removed")

        def publish_data(self, send_all=False, QoS=0):
            raise NotImplementedError("This class method has been removed")

    class EntityScadaEdgeNode (MqttSpbEntity):
        '''
            Entity Scada Edge Node entity (EoN) class

            This class is used to represent virtually an EoN entity from a SCADA application.
            The scada application will automatically parse the received entity data into its
            representation, allowing you to easily interact with the entity ( via commands ) or
            to get the latest telemetry and attribute values.

            This class allows you to:
              - Subscribe to callback events when data from the entity is received ( BIRTH, DATA, DEATH )
              - Get the latest data values ( entity.data or entity.attributes )
              - Send a Command/s to the physical entity
        '''

        # functions - on_birth, on_death, on_data, send_command,
        def __init__(self,
                     spb_domain_name, spb_eon_name,
                     scada_entity,
                     callback_birth=None, callback_data=None, callback_death=None,
                     debug_info=False):
            super().__init__(spb_domain_name=spb_domain_name,
                             spb_eon_name=spb_eon_name,
                             debug_info=debug_info)

            self.scada = scada_entity   # Save reference to the scada entity

            self.callback_birth = callback_birth    # Save callback function references
            self.callback_data = callback_data
            self.callback_death = callback_death

            self.entities_eond: Dict[str, MqttSpbEntityScada.EntityScadaEdgeDevice] = {}
            '''
                Dictionary of current virtual Edge Devices Entities.
                
                { EoND_Name : MqttSpbEntityScada.EntityScadaEdgeDevice(), ...}
            '''

            self._is_alive = False  # Device is alive ( BIRTH + DATA ) or not ( DEATH )

            # Console Logger
            self._logger = logging.getLogger("MQTTSPB_SCADA_EoND")
            handler_log = logging.StreamHandler()
            handler_log.setFormatter(logging.Formatter('%(asctime)s %(name)s %(levelname)s | %(message)s'))
            self._logger.addHandler(handler_log)
            if debug_info:
                self._logger.setLevel(logging.DEBUG)
            else:
                self._logger.setLevel(logging.ERROR)

        def is_alive(self):
            return self._is_alive

        def connect(self,
                    host='localhost',
                    port=1883,
                    user="",
                    password="",
                    use_tls=False,
                    tls_ca_path="",
                    tls_cert_path="",
                    tls_key_path="",
                    tls_insecure=False,
                    timeout=5):
            raise NotImplementedError("This method has been removed")

        def disconnect(self, skip_death_publish=False):
            raise NotImplementedError("This class method has been removed")

        def is_connected(self):
            raise NotImplementedError("This class method has been removed")

        def publish_birth(self, QoS=0):
            raise NotImplementedError("This class method has been removed")

        def publish_data(self, send_all=False, QoS=0):
            raise NotImplementedError("This class method has been removed")

        def send_command(self, name, value, force=False):
            return self.send_commands({name: value}, force)

        def send_commands(self, commands, force=False):

            # Command Check - If not part of entity commands, through an error.
            if not force:
                for k in commands:
                    if k not in self.commands.get_name_list():
                        self._logger.error("%s - Command %s not sent, unknown device command." % (self._entity_domain, k))
                        return False

            # Send commands via SCADA application
            self.scada.send_commands(commands, self.spb_eon_name)

    def __init__(self,
                 spb_domain_name,  # sparkplug B Domain name
                 spb_scada_name,  # Scada application name
                 callback_birth=None, callback_data=None, callback_death=None,  # callbacks for different messages
                 debug_info=False):
        """

        Initiate the SCADA application class

        Args:
            spb_domain_name:    Sparkplug B domain name
            spb_scada_name:     Scada Application ID ( will be part of the MQTT topic )
            debug_info:         Enable / Dissable debug information.
        """

        # Initialized the object ( parent class ) with Device_id as None - Configuring it as edge node
        super().__init__(spb_domain_name, spb_scada_name, None,
                         debug_info=False,
                         filter_cmd_msg=False,
                         entity_is_scada=True)

        self._debug_info = debug_info  # If debug info should be displayed in the console.

        #
        self.entities_eon: Dict[str, MqttSpbEntityScada.EntityScadaEdgeNode] = {}
        '''
        List of current discovered edge nodes entities (EoN)
        '''

        self.callback_birth = callback_birth  # Save callback function references
        self.callback_data = callback_data
        self.callback_death = callback_death

        self._spb_initialized = False       # Flag to mark the initialization of spb persistent messages(BIRTH, DEATH)
        self._spb_initialized_timeout = 0   # Counter to keep initialization timeout event

        # Console Logger
        self._logger = logging.getLogger("MQTTSPB_SCADA_APP")
        handler_log = logging.StreamHandler()
        handler_log.setFormatter(logging.Formatter('%(asctime)s %(name)s %(levelname)s | %(message)s'))
        self._logger.addHandler(handler_log)
        if debug_info:
            self._logger.setLevel(logging.DEBUG)
        else:
            self._logger.setLevel(logging.ERROR)

        self._logger.info("New SCADA Application object")

    def connect(self,
                host: str = 'localhost',
                port: int = 1883,
                user: str = "",
                password: str = "",
				use_tls: bool = False,
                tls_ca_path: str = "",
                tls_cert_path: str = "",
                tls_key_path: str = "",
                tls_insecure: str = False,
                timeout: int = 5) -> bool:

        # Set the initialization of spb messages ( BIRTH and DEATH )
        self._spb_initialized = False
        self._spb_initialized_timeout = time.time()

        # Call the parent method
        return super().connect(host, port, user, password, use_tls, tls_ca_path, tls_cert_path, tls_key_path, tls_insecure, timeout)

    def disconnect(self, skip_death_publish=False):

        # Clear the initialization of spb messages ( BIRTH and DEATH )
        self._spb_initialized = False

        # Call the parent method
        return super().disconnect(skip_death_publish)


    # Override class method 
    def _mqtt_on_connect(self, client, userdata, flags, rc):

        # Call the parent method
        super()._mqtt_on_connect(client, userdata, flags, rc)

        # Subscribe to all group topics
        if rc == 0:
            topic = "spBv1.0/" + self.spb_domain_name + "/#"
            self._mqtt.subscribe(topic)
            self._logger.info("%s - Subscribed to MQTT topic: %s" % (self._entity_domain, topic))

    def _mqtt_on_message(self, client, userdata, msg):
        """
            Override the mqtt on message
        """

        # self._logger.info("%s - Message received  %s" % (self._entity_domain, msg.topic))

        # Tasks for initialization of spb messages
        if not self._spb_initialized:

            # If timeout expired, then mark the finalization of the initialization period
            if time.time() > (self._spb_initialized_timeout + 0.5):

                self._spb_initialized = True    # spb presistent messages are being initialized

                # Reset entity state for all discovered entities
                for eon in self.entities_eon.keys():
                    self.entities_eon[eon]._is_alive = False

                    for eond in self.entities_eon[eon].entities_eond.keys():
                        self.entities_eon[eon].entities_eond[eond]._is_alive = False

                self._logger.debug("spB initialization period expired, all entity states are cleared.")

        # Process the message for edge nodes ( EoN and EoND )
        topic = MqttSpbTopic(msg.topic)
        eon_name = topic.eon_name
        eond_name = topic.eon_device_name

        # Ignore data from SCADA application
        if eon_name == self.entity_name:
            return  # Exit and ignore the data

        # EDGE NODE - Let's check if the EdgeNode is already discovered, if not create it
        if eon_name not in self.entities_eon.keys():
            self._logger.debug("Unknown EoN entity, registering edge node: " + topic.eon_name)
            self.entities_eon[eon_name] = self.EntityScadaEdgeNode(spb_domain_name=self.spb_domain_name,
                                                                   spb_eon_name=topic.eon_name,
                                                                   scada_entity=self,
                                                                   debug_info=self._debug_info)

        # DEVICE NODE - Lets check if device is discovered, otherwise create it.
        if eond_name not in self.entities_eon[eon_name].entities_eond.keys():
            self.entities_eon[eon_name].entities_eond[eond_name] = self.EntityScadaEdgeDevice(spb_domain_name=self.spb_domain_name,
                                                                                              spb_eon_name=eon_name,
                                                                                              spb_eon_device_name=eond_name,
                                                                                              scada_entity=self,
                                                                                              debug_info=self._debug_info)
            self._logger.debug("New device <%s> for edge node <%s>" % (eond_name, eon_name))

        # PARSE PAYLOAD - De-serialize payload and update device data

        # Get entity reference, EoN or EoND
        if eond_name is None:
            entity = self.entities_eon[eon_name]
        else:
            entity = self.entities_eon[eon_name].entities_eond[eond_name]

        # Parse the payload, to be sent to the callbacks
        payload = MqttSpbPayload(msg.payload)

        # Parse the message from its type
        if topic.message_type.endswith("BIRTH"):
            if self._spb_initialized:
                entity._is_alive = True  # Update status
            entity.deserialize_payload_birth(msg.payload)  # Send the payload to the entity to deserialize it.
            if entity.callback_birth is not None:
                entity.callback_birth(payload.asDict())
            if self.callback_birth is not None:
                self.callback_birth(topic, payload.asDict())

        elif topic.message_type.endswith("DATA"):
            if self._spb_initialized:
                entity._is_alive = True  # Update status
            entity.deserialize_payload_data(msg.payload)  # Send the payload to the entity to deserialize it.
            if entity.callback_data is not None:
                entity.callback_data(payload.asDict())
            if self.callback_data is not None:
                self.callback_data(topic, payload.asDict())

        elif topic.message_type.endswith("DEATH"):
            if self._spb_initialized:
                entity._is_alive = False    # Update status
            # TODO decode the Death message and update the entity status
            # entity.deserialize_payload_death(msg.payload)  # Send the payload to the entity to deserialize it.
            if entity.callback_death is not None:
                entity.callback_death(payload.asDict())
            if self.callback_death is not None:
                self.callback_death(topic, payload.asDict())

        elif topic.message_type.endswith("CMD"):
            pass    # do not parse entity commands

        else:
            self._logger.warning("%s - Unknown message type %s, could not parse MQTT message, message ignored" % (self._entity_domain, topic.message_type))

        # Send message to Entity Scada
        super()._mqtt_on_message(client, userdata, msg)

    def send_command(self, cmd_name: str, cmd_value, eon_name: str, eond_name: str=None) -> bool:
        '''
        Send a command to an EoN or EoND entity.


        Args:
            cmd_name:       Command name
            cmd_value:      Command value
            eon_name:       EoN entity name
            eond_name:      EoND entity name. If set to None the command will be sent to an EoN entity.

        Returns:    Boolean representing the result
        '''
        return self.send_commands(eon_name, eond_name, {cmd_name: cmd_value})

    def send_commands(self, commands: Dict[str, Any], eon_name: str, eond_name: str = None) -> bool:
        """
        Send a list of commands to the Edge node ( EoN_name ) or Edge Device Node ( EoND_Name )

        Args:
            commands:   Dictionary of commands
            eon_name:   Name of Edge node
            eond_name:  Name of Edge Device node. If empty, commands will be only sent to Edge entity (EoN)

        Returns:    True if commands sent successfully

        """
        if not self.is_connected():  # If not connected
            self._logger.warning("%s - Could not send publish_command(), not connected to MQTT server" % self._entity_domain)
            return False

        if not isinstance(commands, dict):  # If no data commands as dictionary
            self._logger.warning(
                "%s - Command not sent. Please provide a dictionary with <command_name:value>" % self._entity_domain)
            return False

        # PAYLOAD
        payload = getDdataPayload()

        # Add the list of commands to the payload metrics
        for k in commands:
            addMetric(payload, k, None, self._spb_data_type(commands[k]), commands[k])

        # Send payload if there is new data
        if eond_name is not None:
            topic = "spBv1.0/" + self.spb_domain_name + "/DCMD/" + eon_name + "/" + eond_name
        else:
            topic = "spBv1.0/" + self.spb_domain_name + "/NCMD/" + eon_name

        if payload.metrics:
            payload_bytes = bytearray(payload.SerializeToString())
            self._mqtt.publish(topic, payload_bytes, 0, False)
            self._logger.debug("%s - Published COMMAND message to %s" % (self._entity_domain, topic))
            return True
        else:
            self._logger.warning(
              "%s - Could not publish COMMAND message to %s, no payload metrics?" % (self._entity_domain, topic))
            return False

    def get_edge_node(self, eon_name: str) -> EntityScadaEdgeNode:
        """
            Subscribe and get a reference to the virtual EoN entity.
        Args:
            eon_name:   EoN Entity Name

        Returns:    EntityScadaEdgeNode reference for the EoN Entity.
        """
        return self.get_edge_node_device(eon_name, None)   # Get reference for the

    def get_edge_node_device(self, eon_name: str, eond_name: str) -> EntityScadaEdgeDevice:
        """
        Get reference for the virtual Edge Node Device (EoND) entity.

        Args:
            eon_name:   EoN name
            eond_name:  EoN Device name
        """

        # EDGE NODE - Let's check if the EdgeNode is already discovered, if not create it
        if eon_name not in self.entities_eon.keys():
            self._logger.debug("Unknown EoN entity, registering edge node: " + eon_name)
            self.entities_eon[eon_name] = self.EntityScadaEdgeNode(spb_domain_name=self.spb_domain_name,
                                                                   spb_eon_name=eon_name,
                                                                   scada_entity=self,
                                                                   debug_info=self._debug_info)

        # DEVICE NODE - Lets check if device is discovered, otherwise create it.
        if eond_name is not None:
            if eond_name not in self.entities_eon[eon_name].entities_eond.keys():
                self.entities_eon[eon_name].entities_eond[eond_name] = self.EntityScadaEdgeDevice(spb_domain_name=self.spb_domain_name,
                                                                                                  spb_eon_name=eon_name,
                                                                                                  spb_eon_device_name=eond_name,
                                                                                                  scada_entity=self,
                                                                                                  debug_info=self._debug_info)
                self._logger.debug("New device <%s> for edge node <%s>" % (eond_name, eon_name))

            return self.entities_eon[eon_name].entities_eond[eond_name]    # Return EoND
        else:
            return self.entities_eon[eon_name]   # Return EoN

    # def get_device_by_attribute(self, attribute_name, attribute_value, eon_name=None):
    #     #TODO implement get_entity_by_attribute
    #
    #     # Search for entities that match attribute name and value
    #     # If eon_name is None it will search in all eon
    #     pass
