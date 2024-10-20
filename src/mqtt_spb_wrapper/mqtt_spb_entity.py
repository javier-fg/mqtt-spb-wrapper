import time
import paho.mqtt.client as mqtt

from .spb_base import SpbEntity, SpbTopic, SpbPayloadParser
from .spb_protobuf import getNodeDeathPayload


class MqttSpbEntity(SpbEntity):

    def __init__(self,
                 spb_domain_name,
                 spb_eon_name,
                 spb_eon_device_name=None,
                 retain_birth=False,
                 debug_enabled=False,
                 debug_id="MQTT_SPB_ENTITY",
                 entity_is_scada=False
                 ):

        super().__init__(spb_domain_name=spb_domain_name,
                         spb_eon_name=spb_eon_name,
                         spb_eon_device_name=spb_eon_device_name,
                         debug_enabled=debug_enabled, debug_id=debug_id)

        # Public members -----------
        self.on_command = None  # Callback function when a command is received
        self.on_connect = None
        self.on_disconnect = None
        self.on_message = None

        # Private members -----------
        self._retain_birth = retain_birth
        self._entity_is_scada = entity_is_scada
        self._mqtt = None  # Mqtt client object
        self._loopback_topic = ""  # Last publish topic, to avoid loopback message reception

    def publish_birth(self, qos=0):

        if not self.is_connected():  # If not connected
            self._logger.warning("%s - Could not send publish_birth(), not connected to MQTT server"
                                 % self._entity_domain)
            return False

        # If it is a type entity SCADA, change the BIRTH certificate
        if self._entity_is_scada:
            topic = "spBv1.0/" + self.spb_domain_name + "/STATE/" + self._spb_eon_name
            self._loopback_topic = topic
            self._mqtt.publish(topic, "ONLINE".encode("utf-8"), qos, True)
            self._logger.info("%s - Published STATE BIRTH message " % self._entity_domain)
            self.is_birth_published = True
            return

        if self.is_empty():  # If no data (Data, attributes, commands )
            self._logger.warning(
                "%s - Could not send publish_birth(), entity doesn't have data ( attributes, data, commands )"
                % self._entity_domain)
            return False

        # Publish BIRTH message
        payload_bytes = self.serialize_payload_birth()

        if self._spb_eon_device_name is None:  # EoN
            topic = "spBv1.0/" + self.spb_domain_name + "/NBIRTH/" + self._spb_eon_name
        else:
            topic = "spBv1.0/" + self.spb_domain_name + "/DBIRTH/" + self._spb_eon_name \
                    + "/" + self._spb_eon_device_name

        self._loopback_topic = topic
        self._mqtt.publish(topic, payload_bytes, qos, self._retain_birth)

        self._logger.info("%s - Published BIRTH message" % self._entity_domain)

        self.is_birth_published = True

    def publish_data(self, send_all=False, qos=0):
        """
            Send the new updated data to the MQTT broker as a Sparkplug B DATA message.

        :param send_all: boolean    True: Send all data fields, False: send only updated field values
        :param qos                  QoS level
        :return:                    result
        """

        if not self.is_connected():  # If not connected
            self._logger.warning(
                "%s - Could not send publish_telemetry(), not connected to MQTT server" % self._entity_domain)
            return False

        if self.is_empty():  # If no data (Data, attributes, commands )
            self._logger.warning(
                "%s - Could not send publish_telemetry(), entity doesn't have data ( attributes, data, commands )"
                % self._entity_domain)
            return False

        # Send payload if there is new data, or we need to send all
        if send_all or self.data.is_updated():

            payload_bytes = self.serialize_payload_data(send_all)  # Get the data payload

            if self._spb_eon_device_name is None:  # EoN
                topic = "spBv1.0/" + self.spb_domain_name + "/NDATA/" + self._spb_eon_name
            else:
                topic = "spBv1.0/" + self.spb_domain_name + "/DDATA/" + self._spb_eon_name + "/" \
                        + self._spb_eon_device_name

            self._loopback_topic = topic
            self._mqtt.publish(topic, payload_bytes, qos, False)

            self._logger.debug("%s - Published DATA message %s" % (self._entity_domain, topic))
            return True

        self._logger.warning("%s - Could not publish DATA message, may be data no new data values?"
                             % self._entity_domain)
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
                timeout=5,
                skip_death=False,
                client_id="",
                ):
        """
            Connect to the spB MQTT server
        Args:
            host:
            port:
            user:
            password:
            use_tls:
            tls_ca_path:
            tls_cert_path:
            tls_key_path:
            tls_insecure:
            timeout:
            skip_death:         If true, DEATH meassage will not be sent

        Returns:

        """
        # If we are already connected, then exit
        if self.is_connected():
            return True

        # MQTT Client configuration
        if self._mqtt is None:
            self._mqtt = mqtt.Client(userdata=self, client_id=client_id)

        self._mqtt.on_connect = self._mqtt_on_connect
        self._mqtt.on_disconnect = self._mqtt_on_disconnect
        self._mqtt.on_message = self._mqtt_on_message

        if user != "":
            self._mqtt.username_pw_set(user, password)

        # If client certificates are provided
        if tls_ca_path and tls_cert_path and tls_key_path:
            self._logger.debug("Setting CA client certificates")

            if tls_insecure:
                self._logger.debug(
                    "Setting CA client certificates - IMPORTANT CA insecure mode ( use only for testing )")
                import ssl
                self._mqtt.tls_set(ca_certs=tls_ca_path, certfile=tls_cert_path, keyfile=tls_key_path,
                                   cert_reqs=ssl.CERT_NONE)
                self._mqtt.tls_insecure_set(True)
            else:
                self._logger.debug("Setting CA client certificates")
                self._mqtt.tls_set(ca_certs=tls_ca_path, certfile=tls_cert_path, keyfile=tls_key_path)

        # If only CA is provided.
        elif tls_ca_path:
            self._logger.debug("Setting CA certificate")
            self._mqtt.tls_set(ca_certs=tls_ca_path)

        # If TLS is enabled
        else:
            if use_tls:
                self._mqtt.tls_set()  # Enable TLS encryption

        # Entity DEATH message - last will message
        if not skip_death:
            if self._entity_is_scada:  # If it is a type entity SCADA, change the DEATH certificate
                topic = "spBv1.0/" + self.spb_domain_name + "/STATE/" + self._spb_eon_name
                self._mqtt.will_set(topic, "OFFLINE".encode("utf-8"), 0, True)  # Set message
            else:  # Normal node
                payload = getNodeDeathPayload()
                payload_bytes = bytearray(payload.SerializeToString())
                if self._spb_eon_device_name is None:  # EoN
                    topic = "spBv1.0/" + self.spb_domain_name + "/NDEATH/" + self._spb_eon_name
                else:
                    topic = "spBv1.0/" + self.spb_domain_name + "/DDEATH/" + self._spb_eon_name + "/" \
                            + self._spb_eon_device_name

                self._mqtt.will_set(topic, payload_bytes, 0, False)  # Set message

        # MQTT Connect
        self._logger.info("%s - Trying to connect MQTT server %s:%d" % (self._entity_domain, host, port))
        try:
            self._mqtt.connect(host, port)
        except Exception as e:
            self._logger.warning("%s - Could not connect to MQTT server (%s)" % (self._entity_domain, str(e)))
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

        self._logger.info("%s - Disconnecting from MQTT server" % self._entity_domain)

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
                        topic = "spBv1.0/" + self.spb_domain_name + "/DDEATH/" + self._spb_eon_name \
                                + "/" + self._spb_eon_device_name

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
            self._logger.info("%s - Connected to MQTT server" % self._entity_domain)

            # Subscribing in on_connect() means that if we lose the connection and
            # reconnect then subscriptions will be renewed.
            if self._spb_eon_device_name is None:  # EoN
                topic = "spBv1.0/" + self.spb_domain_name + "/NCMD/" + self._spb_eon_name
            else:
                topic = "spBv1.0/" + self.spb_domain_name + "/DCMD/" + self._spb_eon_name + "/" \
                        + self._spb_eon_device_name
            client.subscribe(topic)
            self._logger.info("%s - Subscribed to MQTT topic: %s" % (self._entity_domain, topic))

            # Subscribe to STATE of SCADA application
            topic = "spBv1.0/" + self.spb_domain_name + "/STATE/+"
            client.subscribe(topic)
            self._logger.info("%s - Subscribed to MQTT topic: %s" % (self._entity_domain, topic))

        else:
            self._logger.error(" %s - Could not connect to MQTT server !" % self._entity_domain)

        # Execute the callback function if it is not None
        if self.on_connect is not None:
            self.on_connect(rc)

    def _mqtt_on_disconnect(self, client, userdata, rc):
        self._logger.info("%s - Disconnected from MQTT server" % self._entity_domain)

        # Execute the callback function if it is not None
        if self.on_disconnect is not None:
            self.on_disconnect(rc)

    def _mqtt_on_message(self, client, userdata, msg):

        # Check if loopback message
        if self._loopback_topic == msg.topic:
            return

        msg_ts_rx = int(time.time() * 1000)  # Save the current timestamp

        # self._logger.info("%s - Message received  %s" % (self._entity_domain, msg.topic))

        # Parse the topic namespace ------------------------------------------------
        topic = SpbTopic(msg.topic)  # Parse and get the topic object

        # Check that the namespace and group are correct
        # NOTE: Should not be because we are subscribed to a specific topic, but good to check.
        if topic.namespace != "spBv1.0" or topic.domain_name != self._spb_domain_name:
            self._logger.error(
                "%s - Incorrect MQTT spBv1.0 namespace and topic. Message ignored !" % self._entity_domain)
            return

        # Check if it is a STATE message from the SCADA application
        if topic.message_type == "STATE":
            # Execute the callback function if it is not None
            if self.on_message is not None:
                self.on_message(topic, msg.payload.decode("utf-8"))
            return

        # Parse the received ProtoBUF data ------------------------------------------------
        payload = SpbPayloadParser().parse_payload(msg.payload)

        # Add the timestamp when the message was received
        payload['timestamp_rx'] = msg_ts_rx

        # Execute the callback function if it is not None
        if self.on_message is not None:
            self.on_message(topic, payload)

        # Actions depending on the MESSAGE TYPE
        if "CMD" in topic.message_type:

            # Check if a list of commands is provided
            if "metrics" not in payload.keys():
                self._logger.error(
                    "%s - Incorrect MQTT CMD payload, could not find any metrics. CMD message ignored !"
                    % self._entity_domain)
                return

            # Process CMDs - Unknown commands will be ignored
            for item in payload['metrics']:

                # If not in the current list of known commands, removed it
                if item['name'] not in self.commands.get_names():
                    self._logger.warning(
                        "%s - Unrecognized CMD: %s - CMD will be ignored" % (self._entity_domain, item['name']))
                    continue  # Process next command

                # Check if the datatypes match, otherwise ignore the command
                elif not isinstance(item['value'], type(self.commands.get_value(item['name']))):
                    self._logger.warning("%s - Incorrect CMD datatype: %s - CMD will be ignored" % (
                        self._entity_domain, item['name']))
                    continue  # Process next command

                # Update the command value. If it has a callback configured, it will be executed
                self.commands.set_value(name=item["name"], value=item["value"], timestamp=item.get("timestamp", None))

            # Execute the callback function if it is not None, with all commands received
            if self.on_command is not None:
                self.on_command(payload)
