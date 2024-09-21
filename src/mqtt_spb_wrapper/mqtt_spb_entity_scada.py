import time
from typing import Dict, Any

from .spb_protobuf import getDdataPayload
from .spb_protobuf import addMetric

from .spb_base import SpbTopic, SpbPayload
from .mqtt_spb_entity import SpbEntity
from .mqtt_spb_entity import MqttSpbEntity


class MqttSpbEntityScada(MqttSpbEntity):

    class EntityScadaEdgeDevice(SpbEntity):
        '''
            Entity Scada Edge Device (EoND) class

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
                             debug_info=debug_info, debug_id="MQTT_SPB_SCADA_DEVICE")

            self.callback_birth = callback_birth  # Save callback function references
            self.callback_data = callback_data
            self.callback_death = callback_death

            self._is_alive = False  # Device is alive ( BIRTH + DATA ) or not ( DEATH )
            self._scada = scada_entity  # Save reference to the scada entity

        def is_alive(self):
            return self._is_alive

        def send_command(self, name, value, force=False):
            return self.send_commands({name: value}, force)

        def send_commands(self, commands, force=False):

            # Command Check - If not part of entity commands, through an error.
            if not force:
                for k in commands:
                    if k not in self.commands.get_name_list():
                        self._logger.error \
                            ("%s - Command %s not sent, unknown device command." % (self._entity_domain, k))
                        return False

            # Send commands via SCADA application
            self._scada.send_commands(commands, self.spb_eon_name, self.spb_eon_device_name)

    class EntityScadaEdgeNode(SpbEntity):
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
                             debug_info=debug_info, debug_id="MQTT_SPB_SCADA_EDGENODE")

            self.callback_birth = callback_birth  # Save callback function references
            self.callback_data = callback_data
            self.callback_death = callback_death

            self.entities_eond: Dict[str, MqttSpbEntityScada.EntityScadaEdgeDevice] = {}

            self._is_alive = False  # Device is alive ( BIRTH + DATA ) or not ( DEATH )
            self._scada = scada_entity  # Save reference to the scada entity

        def is_alive(self):
            return self._is_alive

        def send_command(self, name, value, force=False):
            return self.send_commands({name: value}, force)

        def send_commands(self, commands, force=False):

            # Command Check - If not part of entity commands, through an error.
            if not force:
                for k in commands:
                    if k not in self.commands.get_name_list():
                        self._logger.error \
                            ("%s - Command %s not sent, unknown device command." % (self._entity_domain, k))
                        return False

            # Send commands via SCADA application
            self._scada.send_commands(commands, self.spb_eon_name)

    def __init__(self,
                 spb_domain_name,  # sparkplug B Domain name
                 spb_scada_name,  # Scada application name
                 callback_birth=None, callback_data=None, callback_death=None,  # callbacks for different messages
                 retain_birth=False,
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
                         retain_birth=retain_birth,
                         entity_is_scada=True,
                         debug_info=debug_info, debug_id="MQTT_SPB_SCADA"
                         )

        self.entities_eon: Dict[str, MqttSpbEntityScada.EntityScadaEdgeNode] = {}
        '''
        List of current discovered edge nodes entities (EoN)
        '''

        self.callback_birth = callback_birth  # Save callback function references
        self.callback_data = callback_data
        self.callback_death = callback_death

        self._spb_initialized = False  # Flag to mark the initialization of spb persistent messages(BIRTH, DEATH)
        self._spb_initialized_timeout = 0  # Counter to keep initialization timeout event

        self._debug_info = debug_info

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
        return super().connect(host, port, user, password, use_tls, tls_ca_path, tls_cert_path, tls_key_path,
                               tls_insecure, timeout)

    def disconnect(self, skip_death_publish=False):

        # Clear the initialization of spb messages ( BIRTH and DEATH )
        self._spb_initialized = False

        # Call the parent method
        return super().disconnect(skip_death_publish)

    def _mqtt_on_connect(self, client, userdata, flags, rc):

        # Call the parent method
        super()._mqtt_on_connect(client, userdata, flags, rc)

        # Subscribe to all group topics
        if rc == 0:
            topic = "spBv1.0/" + self.spb_domain_name + "/#"
            self._mqtt.subscribe(topic)
            self._logger.info("%s - Subscribed to MQTT topic: %s" % (self._entity_domain, topic))

    def _mqtt_on_message(self, client, userdata, msg):

        # self._logger.info("%s - Message received  %s" % (self._entity_domain, msg.topic))

        # Tasks for initialization of spb messages
        if not self._spb_initialized:

            # If timeout expired, then mark the finalization of the initialization period
            if time.time() > (self._spb_initialized_timeout + 0.5):

                self._spb_initialized = True  # spb presistent messages are being initialized

                # Reset entity state for all discovered entities
                for eon in self.entities_eon.keys():
                    self.entities_eon[eon]._is_alive = False

                    for eond in self.entities_eon[eon].entities_eond.keys():
                        self.entities_eon[eon].entities_eond[eond]._is_alive = False

                self._logger.debug("spB initialization period expired, all entity states are cleared.")

        # Process the message for edge nodes ( EoN and EoND )
        topic = SpbTopic(msg.topic)
        eon_name = topic.eon_name
        eond_name = topic.eon_device_name

        # Ignore data from SCADA application
        if eon_name == self.entity_name:
            return  # Exit and ignore the data

        # EDGE NODE - Let's check if the EdgeNode is already discovered, if not create it
        if eon_name not in self.entities_eon.keys():
            self.entities_eon[eon_name] = self.EntityScadaEdgeNode(spb_domain_name=self.spb_domain_name,
                                                                   spb_eon_name=topic.eon_name,
                                                                   scada_entity=self,
                                                                   # debug_info=self._debug_info
                                                                   )

            self._logger.debug("NEW edge node discovered - <%s>" % (eon_name))

        # DEVICE NODE - Lets check if device is discovered, otherwise create it.
        if eond_name is not None:
            if eond_name not in self.entities_eon[eon_name].entities_eond.keys():
                new_device = self.EntityScadaEdgeDevice(spb_domain_name=self.spb_domain_name,
                                                        spb_eon_name=eon_name,
                                                        spb_eon_device_name=eond_name,
                                                        scada_entity=self,
                                                        # debug_info=self._debug_info,
                                                        )

                self.entities_eon[eon_name].entities_eond[eond_name] = new_device

                self._logger.debug("NEW device discovered    - <%s>.<%s>" % (eon_name, eond_name))

        # Select the entity to process the data and inject the data into the entity
        if eond_name is None:
            entity = self.entities_eon[eon_name]
        else:
            entity = self.entities_eon[eon_name].entities_eond[eond_name]

        # PARSE PAYLOAD - De-serialize payload and update device dataFix
        payload = SpbPayload(msg.payload)

        # Parse the message from its type
        if topic.message_type.endswith("BIRTH"):
            if self._spb_initialized:
                entity._is_alive = True  # Update status
            entity.deserialize_payload_birth(msg.payload)  # Send the payload to the entity to deserialize it.
            if entity.callback_birth is not None:
                entity.callback_birth(payload.as_dict())
            if self.callback_birth is not None:
                self.callback_birth(topic, payload.as_dict())

        elif topic.message_type.endswith("DATA"):
            if self._spb_initialized:
                entity._is_alive = True  # Update status
            entity.deserialize_payload_data(msg.payload)  # Send the payload to the entity to deserialize it.
            if entity.callback_data is not None:
                entity.callback_data(payload.as_dict())
            if self.callback_data is not None:
                self.callback_data(topic, payload.as_dict())

        elif topic.message_type.endswith("DEATH"):
            if self._spb_initialized:
                entity._is_alive = False  # Update status
            # TODO decode the Death message and update the entity status
            # entity.deserialize_payload_death(msg.payload)  # Send the payload to the entity to deserialize it.
            if entity.callback_death is not None:
                entity.callback_death(payload.as_dict())
            if self.callback_death is not None:
                self.callback_death(topic, payload.as_dict())

        elif topic.message_type.endswith("CMD"):
            pass  # do not parse entity commands

        elif topic.message_type.endswith("STATE"):
            pass  # do not parse entity commands

        else:
            self._logger.warning("%s - Unknown message type %s, could not parse MQTT message, message ignored" %
                                 (self._entity_domain, topic.message_type))

        # Send message to Entity Scada
        super()._mqtt_on_message(client, userdata, msg)

    def send_command(self, cmd_name: str, cmd_value, eon_name: str, eond_name: str = None) -> bool:
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
            self._logger.warning(
                "%s - Could not send publish_command(), not connected to MQTT server" % self._entity_domain)
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
        return self.get_edge_node_device(eon_name, None)  # Get reference for the

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

                self.entities_eon[eon_name].entities_eond[eond_name] = self.EntityScadaEdgeDevice(
                    spb_domain_name=self.spb_domain_name,
                    spb_eon_name=eon_name,
                    spb_eon_device_name=eond_name,
                    scada_entity=self,
                    debug_info=self._debug_info)

                if eond_name is not None:
                    self._logger.debug("New device <%s> for edge node <%s>" % (eond_name, eon_name))
                else:
                    self._logger.debug("New edge node <%s>" % (eon_name))

            return self.entities_eon[eon_name].entities_eond[eond_name]  # Return EoND
        else:
            return self.entities_eon[eon_name]  # Return EoN

    # def get_device_by_attribute(self, attribute_name, attribute_value, eon_name=None):
    #     #TODO implement get_entity_by_attribute
    #
    #     # Search for entities that match attribute name and value
    #     # If eon_name is None it will search in all eon
    #     pass
