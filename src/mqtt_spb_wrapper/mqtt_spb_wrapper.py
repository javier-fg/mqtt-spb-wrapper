import time
import logging
import paho.mqtt.client as mqtt
from google.protobuf.json_format import MessageToDict

from .spb_core import getDdataPayload, getNodeDeathPayload, getNodeBirthPayload, getDeviceBirthPayload
from .spb_core import getSeqNum, getBdSeqNum
from .spb_core import addMetric, MetricDataType
from .spb_core import Payload

# Application logger
logger = logging.getLogger("MQTTSPB_ENTITY")
logger.setLevel(logging.DEBUG)
_log_handle = logging.StreamHandler()
_log_handle.setFormatter(logging.Formatter('%(asctime)s %(name)s %(levelname)s | %(message)s'))
logger.addHandler(_log_handle)

class MqttSparkPlugB_Topic():

    def __init__(self, topic_str):
        self.parse_topic(topic_str)

    def __str__(self):
        return str(self.topic)

    def __repr__(self):
        return str(self.topic)

    def parse_topic(self, topic_str):

        topic_fields = topic_str.split('/')  # Get the topic

        self.topic = topic_str
        self.namespace = topic_fields[0]
        self.domain_id = topic_fields[1]
        self.message_type = topic_fields[2]
        self.edge_node_id = None
        self.device_id = None
        self.entity_id = None

        #If EoN
        if len(topic_fields) > 3:
            self.edge_node_id = topic_fields[3]
            self.entity_id = self.edge_node_id

        # If EoN device type
        if len(topic_fields) > 4:
            self.device_id = topic_fields[4]
            self.entity_id = self.device_id


class MqttSparkPlugB_Entity():

    def __init__(self, domain_id, edge_node_id, device_id,
                 debug_info=False,
                 filter_cmd_msg=True,
                 entity_is_scada=False):

        # Enable / dissable the class logger messages
        if debug_info:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.ERROR)

        self._domain_id = domain_id
        self._edge_node_id = edge_node_id
        self._device_id = device_id

        self.is_alive = True

        if device_id is None:
            self._entity_domain = "%s.%s" % (self._domain_id, self._edge_node_id)
        else:
            self._entity_domain = "%s.%s.%s" % (self._domain_id, self._edge_node_id, self._device_id)

        self.on_command = None  # Callback function when a comand is received
        self.on_connect = None
        self.on_message = None

        self._mqtt = None  # Mqtt client object

        self._loopback_topic = ""   # Last publish topic, to avoid ñoopback message reception

        self.attribures = self._ValuesGroup()
        self.data = self._ValuesGroup()
        self.commands = self._ValuesGroup()

        self._filter_cmd_msg = filter_cmd_msg
        self._entity_is_scada = entity_is_scada

        logger.info("%s - New entity created " % self._entity_domain)

    def __str__(self):
        return str(self.get_dictionary())

    def __repr__(self):
        return str(self.get_dictionary())

    def get_dictionary(self):
        temp = {}
        temp['domain_id'] = self._domain_id
        temp['edge_node_id'] = self._edge_node_id
        if self._device_id is not None:
            temp['device_id'] = self._device_id
        temp['data'] = self.data.get_dictionary()
        temp['attributes'] = self.attribures.get_dictionary()
        temp['commands'] = self.commands.get_dictionary()
        return temp

    def is_empty(self):
        if self.data.is_empty() and self.attribures.is_empty() and self.commands.is_empty():
            return True
        return False

    @property
    def domain_id(self):
        return self._domain_id

    @property
    def edge_node_id(self):
        return self._edge_node_id

    @property
    def device_id(self):
        return self._device_id

    def _spb_data_type(self, data):
        if isinstance(data, str):
            return MetricDataType.Text
        elif isinstance(data, bool):
            return MetricDataType.Boolean
        elif isinstance(data, int):
            return MetricDataType.Int64
        elif isinstance(data, float):
            return MetricDataType.Double
        elif isinstance(data, bytes) or isinstance(data, bytearray):
            return MetricDataType.Bytes

        return MetricDataType.Unknown

    def publish_birth(self):

        if not self.is_connected():  # If not connected
            logger.warning("%s - Could not send publish_birth(), not connected to MQTT server" % self._entity_domain)
            return False

        if self.is_empty():  # If no data (Telemetry, attributes, commands )
            logger.warning(
                "%s - Could not send publish_birth(), entity doesn't have data ( attributes, data, commands )" % self._entity_domain)
            return False

        # If it is a type entity SCADA, change the BIRTH certificate
        if self._entity_is_scada:
            topic = "spBv1.0/" + self.domain_id + "/STATE/" + self._edge_node_id
            self._loopback_topic = topic
            self._mqtt.publish(topic, "ONLINE".encode("utf-8"), 0, True)
            logger.info("%s - Published STATE BIRTH message " % (self._entity_domain))
            return

        # PAYLOAD
        if self._device_id == None:  # EoN type
            payload = getNodeBirthPayload()
        else:  # Device
            payload = getDeviceBirthPayload()

        # Attributes
        if not self.attribures.is_empty():
            for item in self.attribures.values:
                name = "ATTR/" + item.name
                addMetric(payload, name, None, self._spb_data_type(item.value), item.value)

        # Telemetry
        if not self.data.is_empty():
            for item in self.data.values:
                name = "DATA/" + item.name
                addMetric(payload, name, None, self._spb_data_type(item.value), item.value)

        # Commands
        if not self.commands.is_empty():
            for item in self.commands.values:
                name = "CMD/" + item.name
                addMetric(payload, name, None, self._spb_data_type(item.value), item.value)

        # Publish BIRTH message
        payload_bytes = bytearray(payload.SerializeToString())
        if self._device_id == None:  # EoN
            topic = "spBv1.0/" + self.domain_id + "/NBIRTH/" + self._edge_node_id
        else:
            topic = "spBv1.0/" + self.domain_id + "/DBIRTH/" + self._edge_node_id + "/" + self._device_id
        self._loopback_topic = topic
        self._mqtt.publish(topic, payload_bytes, 0, True)

        logger.info("%s - Published BIRTH message" % (self._entity_domain))

    def publish_data(self, send_all=False):

        if not self.is_connected():  # If not connected
            logger.warning(
                "%s - Could not send publish_telemetry(), not connected to MQTT server" % self._entity_domain)
            return False

        if self.is_empty():  # If no data (Telemetry, attributes, commands )
            logger.warning(
                "%s - Could not send publish_telemetry(), entity doesn't have data ( attributes, data, commands )" % self._entity_domain)
            return False

        # PAYLOAD - Telemetry Only
        payload = getDdataPayload()

        for item in self.data.values:
            # Only send those values that have been updated, or if send_all==True, then send all.
            if send_all or item.is_updated:
                addMetric(payload, item.name, None, self._spb_data_type(item.value), item.value)

        # Send payload if there is new data
        if payload.metrics:
            payload_bytes = bytearray(payload.SerializeToString())
            if self._device_id == None:  # EoN
                topic = "spBv1.0/" + self.domain_id + "/NDATA/" + self._edge_node_id
            else:
                topic = "spBv1.0/" + self.domain_id + "/DDATA/" + self._edge_node_id + "/" + self._device_id
            self._loopback_topic = topic
            self._mqtt.publish(topic, payload_bytes, 0, False)

            logger.info("%s - Published DATA message %s" % (self._entity_domain, topic))
            return True

        logger.warning("%s - Could not publish DATA message" % (self._entity_domain))
        return False

    def connect(self, host='localhost', port=1883, user = "", password = ""):

        # If we are already connected, then exit
        if self.is_connected():
            return

        # MQTT Client configuration
        if self._mqtt is None:
            self._mqtt = mqtt.Client(userdata=self)

        self._mqtt.on_connect = self._mqtt_on_connect
        self._mqtt.on_disconnect = self._mqtt_on_disconnect
        self._mqtt.on_message = self._mqtt_on_message

        if user != "":
            self._mqtt.username_pw_set(user, password)

        # Entity DEATH message - last will message
        if self._entity_is_scada:  # If it is a type entity SCADA, change the DEATH certificate
            topic = "spBv1.0/" + self.domain_id + "/STATE/" + self._edge_node_id
            self._mqtt.will_set(topic, "OFFLINE".encode("utf-8"), 0, True)  # Set message
        else:  # Normal node
            payload = getNodeDeathPayload()
            payload_bytes = bytearray(payload.SerializeToString())
            if self._device_id == None:  # EoN
                topic = "spBv1.0/" + self.domain_id + "/NDEATH/" + self._edge_node_id
            else:
                topic = "spBv1.0/" + self.domain_id + "/DDEATH/" + self._edge_node_id + "/" + self._device_id
            self._mqtt.will_set(topic, payload_bytes, 0, True)  # Set message

        # MQTT Connect
        self._mqtt.connect(host, port)
        time.sleep(0.1)
        self._mqtt.loop_start()  # Start MQTT background task

        logger.info("%s - Trying to connect MQTT server %s:%d" % (self._entity_domain, host, port))

        return True

    def disconnect(self):

        logger.info("%s - Disconnecting from MQTT server" % (self._entity_domain))

        if self._mqtt is not None:
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
            if self._device_id is None:  # EoN
                topic = "spBv1.0/" + self.domain_id + "/NCMD/" + self._edge_node_id
            else:
                topic = "spBv1.0/" + self.domain_id + "/DCMD/" + self._edge_node_id + "/" + self._device_id
            self._mqtt.subscribe(topic)
            logger.info("%s - Subscribed to MQTT topic: %s" % (self._entity_domain, topic))

            #Subscribe to STATE of SCADA application
            topic = "spBv1.0/" + self.domain_id + "/STATE/+"
            self._mqtt.subscribe(topic)
            logger.info("%s - Subscribed to MQTT topic: %s" % (self._entity_domain, topic))


            # Publish Birth data
            self.publish_birth()

        else:
            logger.error(" %s - Could not connect to MQTT server !" % self._entity_domain)

        # Execute the callback function if it is not None
        if self.on_connect is not None:
            self.on_connect(rc)

    def _mqtt_on_disconnect(self, client, userdata, rc):
        logger.info("%s - Disconnected from MQTT server" % self._entity_domain)

    def _mqtt_on_message(self, client, userdata, msg):

        #Check if loopback message
        if self._loopback_topic == msg.topic:
                return

        msg_ts_rx = int(round(time.time() * 1000))  # Save the current timestamp

        logger.info("%s - Message received  %s" % (self._entity_domain, msg.topic))

        # Parse the topic namespace ------------------------------------------------
        topic = MqttSparkPlugB_Topic(msg.topic)  # Parse and get the topic object

        # Check that the namespace and domain are correct
        # NOTE: Should not be because we are subscribed to an specific topic, but good to check.
        if topic.namespace != "spBv1.0" or topic.domain_id != self._domain_id:
            logger.error("%s - Incorrect MQTT spBv1.0 namespace and topic. Message ignored !" % self._entity_domain)
            return

        # Check if it is a STATE message from the SCADA application
        if topic.message_type == "STATE":
            # Execute the callback function if it is not None
            if self.on_message is not None:
                self.on_message(topic, msg.payload.decode("utf-8"))
            return

        # Parse the received ProtoBUF data ------------------------------------------------
        pb_payload = Payload()

        try:
            pb_payload.ParseFromString(msg.payload)
            payload = MessageToDict(pb_payload)  # Convert it to DICT for easy handeling

            # Add the metrics [TYPE_value] field into [value] field for convenience
            if "metrics" in payload.keys():
                for i in range(len(payload['metrics'])):
                    for k in payload['metrics'][i].keys():
                        if "Value" in k:
                            payload['metrics'][i]['value'] = payload['metrics'][i][k]
                            break

            # Add the timestamp when the message was received
            payload['timestamp_rx'] = msg_ts_rx

        except Exception as e:
            logger.error(
                "%s - Could not parse MQTT CMD payload, messge ignored ! (reason: %s)" % (self._entity_domain, str(e)))
            return

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


    class _ValueItem():
        def __init__(self, name, value, timestamp=None):
            self.name = name
            self._value = value
            if timestamp is None:
                self._timestamp = int(time.time() * 1000)
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
                    #"updated": self.is_updated
                    }

        @property
        def value(self):
            self.is_updated = False
            return self._value

        @value.setter
        def value(self, value):
            self.is_updated = True
            self._value = value

        @property
        def timestamp(self):
            return self._timestamp

        @timestamp.setter
        def timestamp(self, value):
            if value == None:
                self.timestamp_update()
            else:
                self._timestamp = int(value)

        def timestamp_update(self):
            self.timestamp = int(time.time() * 1000)

    class _ValuesGroup():

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

        def add_value(self, field_name, field_value, field_timestamp=None):
            # If exist, update the value, otherwise create it
            if not self.update_value(field_name, field_value, field_timestamp):
                self.values.append(MqttSparkPlugB_Entity_Device._ValueItem(field_name, field_value, field_timestamp))
            return

        def update_value(self, field_name, field_value, timestamp=None):
            for item in self.values:
                if item.name == field_name:
                    item.value = field_value
                    item.timestamp = timestamp
                    item.is_updated = True
                    return True
            return False


class MqttSparkPlugB_Entity_Device(MqttSparkPlugB_Entity):

    def __init__(self, domain_id, edge_node_id, device_id,
                 debug_info=False,
                 filter_cmd_msg=True):
        # Initialized the object ( parent class ) with Device_id as None - Configuring it as edge node
        super().__init__(domain_id, edge_node_id, device_id, debug_info, filter_cmd_msg)


class MqttSparkPlugB_Entity_EdgeNode(MqttSparkPlugB_Entity):

    def __init__(self, domain_id, edge_node_id, debug_info=False):

        # Initialized the object ( parent class ) with Device_id as None - Configuring it as edge node
        super().__init__(domain_id, edge_node_id, None, debug_info)

    def publish_command_device(self, device_id, commands):

        if not self.is_connected():  # If not connected
            logger.warning(
                "%s - Could not send publish_command_device(), not connected to MQTT server" % self._entity_domain)
            return False

        if not isinstance(commands, dict):  # If no data commands as dictionary
            logger.warning(
                "%s - Could not send publish_command_device(), commands not provided or not valid. Please provide a dictionary of command:value" % self._entity_domain)
            return False

        # PAYLOAD
        payload = getDdataPayload()

        # Add the list of commands to the payload metrics
        for k in commands:
            addMetric(payload, k, None, self._spb_data_type(commands[k]), commands[k])

        # Send payload if there is new data
        topic = "spBv1.0/" + self.domain_id + "/DCMD/" + self._edge_node_id + "/" + device_id

        if payload.metrics:
            payload_bytes = bytearray(payload.SerializeToString())
            self._loopback_topic = topic
            self._mqtt.publish(topic, payload_bytes, 0, False)

            logger.info("%s - Published COMMAND message to %s" % (self._entity_domain, topic))

            return True

        logger.warning("%s - Could not publish COMMAND message to %s" % (self._entity_domain, topic))
        return False


class MqttSparkPlugB_Entity_Application(MqttSparkPlugB_Entity_Device):

    def __init__(self, domain_id, app_entity_id, debug_info=False):

        # Initialized the object ( parent class ) with Device_id as None - Configuring it as edge node
        super().__init__(domain_id, app_entity_id, None,
                         debug_info=debug_info,
                         filter_cmd_msg=False)

    # Override class method
    def _mqtt_on_connect(self, client, userdata, flags, rc):

        # Call the parent method
        super()._mqtt_on_connect(client, userdata, flags, rc)

        # Subscribe to all domain topics
        if rc == 0:
            topic = "spBv1.0/" + self.domain_id + "/#"
            self._mqtt.subscribe(topic)
            logger.info("%s - Subscribed to MQTT topic: %s" % (self._entity_domain, topic))


class MqttSparkPlugB_Entity_SCADA(MqttSparkPlugB_Entity):

    def __init__(self, domain_id, scada_id, debug_info=False):

        # Initialized the object ( parent class ) with Device_id as None - Configuring it as edge node
        super().__init__(domain_id, scada_id, None,
                         debug_info=debug_info,
                         filter_cmd_msg=False,
                         entity_is_scada=True)

    # Override class method
    def _mqtt_on_connect(self, client, userdata, flags, rc):

        # Call the parent method
        super()._mqtt_on_connect(client, userdata, flags, rc)

        # Subscribe to all domain topics
        if rc == 0:
            topic = "spBv1.0/" + self.domain_id + "/#"
            self._mqtt.subscribe(topic)
            logger.info("%s - Subscribed to MQTT topic: %s" % (self._entity_domain, topic))

    def publish_command_edge_node(self, edge_node_id, commands):

        if not self.is_connected():  # If not connected
            logger.warning(
                "%s - Could not send publish_command_edge_node(), not connected to MQTT server" % self._entity_domain)
            return False

        if not isinstance(commands, dict):  # If no data commands as dictionary
            logger.warning(
                "%s - Could not send publish_command_edge_node(), commands not provided or not valid. Please provide a dictionary of command:value" % self._entity_domain)
            return False

        # PAYLOAD
        payload = getDdataPayload()

        # Add the list of commands to the payload metrics
        for k in commands:
            addMetric(payload, k, None, self._spb_data_type(commands[k]), commands[k])

        # Send payload if there is new data
        topic = "spBv1.0/" + self.domain_id + "/NCMD/" + edge_node_id

        if payload.metrics:
            payload_bytes = bytearray(payload.SerializeToString())
            self._loopback_topic = topic
            self._mqtt.publish(topic, payload_bytes, 0, False)

            logger.info("%s - Published COMMAND message to %s" % (self._entity_domain, topic))

            return True

        logger.warning("%s - Could not publish COMMAND message to %s" % (self._entity_domain, topic))
        return False

    def publish_command_device(self, edge_node_id, device_id, commands):

        if not self.is_connected():  # If not connected
            logger.warning(
                "%s - Could not send publish_command_device(), not connected to MQTT server" % self._entity_domain)
            return False

        if not isinstance(commands, dict):  # If no data commands as dictionary
            logger.warning(
                "%s - Could not send publish_command_device(), commands not provided or not valid. Please provide a dictionary of command:value" % self._entity_domain)
            return False

        # PAYLOAD
        payload = getDdataPayload()

        # Add the list of commands to the payload metrics
        for k in commands:
            addMetric(payload, k, None, self._spb_data_type(commands[k]), commands[k])

        # Send payload if there is new data
        topic = "spBv1.0/" + self.domain_id + "/DCMD/" + edge_node_id + "/" + device_id

        if payload.metrics:
            payload_bytes = bytearray(payload.SerializeToString())
            self._loopback_topic = topic
            self._mqtt.publish(topic, payload_bytes, 0, False)

            logger.info("%s - Published COMMAND message to %s" % (self._entity_domain, topic))

            return True

        logger.warning("%s - Could not publish COMMAND message to %s" % (self._entity_domain, topic))
        return False


