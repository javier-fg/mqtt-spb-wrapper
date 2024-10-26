import time
from typing import Dict, Any

from .spb_protobuf import getDdataPayload
from .spb_protobuf import addMetric
from .spb_protobuf import getValueDataType

from .spb_base import SpbTopic, SpbPayloadParser
from .mqtt_spb_entity import SpbEntity
from .mqtt_spb_entity import MqttSpbEntity
from .mqtt_spb_entity_app import MqttSpbEntityApp


class MqttSpbEntityScada(MqttSpbEntityApp):

    class DeviceEntity(SpbEntity):
        """
            Entity Scada Edge Device (EoND) class

            This class is used to represent virtually an EoND entity from a SCADA application.
            The scada application will automatically parse the received entity data into its
            representation, allowing you to easly interact with the entity ( via commands ) or
            to get the lates telemetry and attribute values.

            This class allows you to:
              - Subscribe to callback events when data from the entity is received ( BIRTH, DATA, DEATH )
              - Get the latest data values ( entity.data or entity.attributes )
              - Send a Command/s to the physical entity
        """

        def __init__(self,
                     spb_domain_name, spb_eon_name, spb_eon_device_name,
                     scada_entity,
                     callback_birth=None, callback_data=None, callback_death=None,
                     debug_enabled=False):

            super().__init__(
                spb_domain_name=spb_domain_name,
                spb_eon_name=spb_eon_name,
                spb_eon_device_name=spb_eon_device_name,
                debug_enabled=debug_enabled,
                debug_id="MQTT_SPB_SCADA_DEVICE"
            )

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
                    if k not in self.commands.get_names():
                        self._logger.error \
                            ("%s - Command %s not sent, unknown device command." % (self._entity_domain, k))
                        return False

            # Send commands via SCADA application
            self._scada.send_commands(commands, self.spb_eon_name, self.spb_eon_device_name)

    class EdgeEntity(SpbEntity):
        """
            Entity Scada Edge Node entity (EoN) class

            This class is used to represent virtually an EoN entity from a SCADA application.
            The scada application will automatically parse the received entity data into its
            representation, allowing you to easily interact with the entity ( via commands ) or
            to get the latest telemetry and attribute values.

            This class allows you to:
              - Subscribe to callback events when data from the entity is received ( BIRTH, DATA, DEATH )
              - Get the latest data values ( entity.data or entity.attributes )
              - Send a Command/s to the physical entity
        """

        # functions - on_birth, on_death, on_data, send_command,
        def __init__(self,
                     spb_domain_name, spb_eon_name,
                     scada_entity,
                     callback_birth=None, callback_data=None, callback_death=None,
                     debug_enabled=False):

            super().__init__(
                spb_domain_name=spb_domain_name,
                spb_eon_name=spb_eon_name,
                debug_enabled=debug_enabled,
                debug_id="MQTT_SPB_SCADA_EDGENODE",
            )

            self.callback_birth = callback_birth  # Save callback function references
            self.callback_data = callback_data
            self.callback_death = callback_death

            self.entities_eond: Dict[str, MqttSpbEntityScada.DeviceEntity] = {}

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
                    if k not in self.commands.get_names():
                        self._logger.error \
                            ("%s - Command %s not sent, unknown device command." % (self._entity_domain, k))
                        return False

            # Send commands via SCADA application
            self._scada.send_commands(commands, self.spb_eon_name)

        def search_device_by_attribute(self, attributes: dict) -> list:

            res = []    # List of devices found

            # Iterate over the devices
            for eond, device in self.entities_eond.items():

                is_found = True     # Flag to mark a detection

                # Iterate over the attributes to be found
                for k, v in attributes.items():

                    # not found on previous iteration/attribute, then exit
                    if not is_found:
                        break

                    # Search for attribute name
                    if k not in device.attributes.get_names():
                        is_found = False
                        continue    # Not found

                    # Compare the attribute value
                    if not str(device.attributes.get_value(k)) == str(v):
                        is_found = False
                        continue

                # If found a match, add the device
                if is_found:
                    res.append(eond)

            return res

    def __init__(self,
                 spb_domain_name,  # sparkplug B Domain name
                 spb_scada_name,  # Scada application name
                 callback_birth=None, callback_data=None, callback_death=None,  # callbacks for different messages
                 callback_new_eon=None, callback_new_eond=None,
                 retain_birth=False,
                 debug_enabled=False):
        """

        Initiate the SCADA application class

        Args:
            spb_domain_name:    Sparkplug B domain name
            spb_scada_name:     Scada Application ID ( will be part of the MQTT topic )
            debug_info:         Enable / Disable debug information.
        """

        # Initialized base class
        MqttSpbEntity.__init__(
            self,
            spb_domain_name=spb_domain_name,
            spb_eon_name=spb_scada_name,
            spb_eon_device_name=None,
            retain_birth=retain_birth,
            entity_is_scada=True,
            debug_enabled=debug_enabled,
            debug_id="MQTT_SPB_SCADA"
        )

        self.entities_eon: Dict[str, MqttSpbEntityScada.EdgeEntity] = {}

        """
        List of current discovered edge nodes entities (EoN)
        """

        self.callback_birth = callback_birth  # Save callback function references
        self.callback_data = callback_data
        self.callback_death = callback_death

        self.callback_new_eon = callback_new_eon
        self.callback_new_eond = callback_new_eond

        self._spb_initialized = False  # Flag to mark the initialization of spb persistent messages(BIRTH, DEATH)
        self._spb_initialized_timeout = 0  # Counter to keep initialization timeout event

        self._logger.info("New SCADA Application object")

    def send_command(self, cmd_name: str, cmd_value, eon_name: str, eond_name: str = None) -> bool:
        """
        Send a command to an EoN or EoND entity.


        Args:
            cmd_name:       Command name
            cmd_value:      Command value
            eon_name:       EoN entity name
            eond_name:      EoND entity name. If set to None the command will be sent to an EoN entity.

        Returns:    Boolean representing the result
        """
        return self.send_commands(
            eon_name=eon_name,
            eond_name=eond_name,
            commands={cmd_name: cmd_value}
        )

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
            addMetric(payload, k, None, getValueDataType(commands[k]), commands[k])

        # Send payload if there is new data
        if eond_name is not None:
            topic = "%s/%s/DCMD/%s/%s" % (self._spb_namespace,
                                          self._spb_domain_name,
                                          eon_name,
                                          eond_name)
        else:
            topic = "%s/%s/NCMD/%s" % (self._spb_namespace,
                                       self._spb_domain_name,
                                       eon_name)

        if payload.metrics:
            payload_bytes = bytearray(payload.SerializeToString())
            self._mqtt_payload_publish(topic, payload_bytes)
            self._logger.debug("%s - Published COMMAND message to %s" % (self._entity_domain, topic))
            return True
        else:
            self._logger.warning(
                "%s - Could not publish COMMAND message to %s, no payload metrics?" % (self._entity_domain, topic))
            return False


    def _register_edge_node(self, eon_name) -> EdgeEntity:
        """
        If not discovered, it will create the entity and return a reference

        Args:
            eon_name:       EoN name

        Returns:    reference to the entity
        """

        # EDGE NODE - Let's check if the EdgeNode is already discovered, if not create it
        if eon_name not in self.entities_eon.keys():
            self._logger.debug("Unknown EoN entity, registering edge node: " + eon_name)
            self.entities_eon[eon_name] = self.EdgeEntity(
                spb_domain_name=self.spb_domain_name,
                spb_eon_name=eon_name,
                scada_entity=self,
                debug_enabled=self._debug_enabled
            )

            # If callback is configured
            if self.callback_new_eon is not None:
                self.callback_new_eon(eon_name)

            self._logger.debug("New edge node <%s>" % eon_name)

        return self.entities_eon[eon_name]  # Return EoN

    def _register_edge_device(self, eon_name, eond_name) -> DeviceEntity:
        """
            If not discovered, it will create the entity and return a reference

        Args:
            eon_name:       EoN name
            eond_name:      EoND name

        Returns:    reference to the entity

        """

        # DEVICE ENTITY - Let's check if the Device entity is already discovered, if not create it
        if eond_name not in self.entities_eon[eon_name].entities_eond.keys():

            self.entities_eon[eon_name].entities_eond[eond_name] = self.DeviceEntity(
                spb_domain_name=self.spb_domain_name,
                spb_eon_name=eon_name,
                spb_eon_device_name=eond_name,
                scada_entity=self,
                debug_enabled=self._debug_enabled
            )

            # If callback is configured
            if self.callback_new_eond is not None:
                self.callback_new_eond(eon_name, eond_name)

            self._logger.debug("New device <%s> for edge node <%s>" % (eond_name, eon_name))

        return self.entities_eon[eon_name].entities_eond[eond_name]  # Return EoND

    def get_edge_node(self, eon_name: str) -> EdgeEntity:
        """
            Subscribe and get a reference to the virtual EoN entity.
        Args:
            eon_name:   EoN Entity Name

        Returns:    Reference for the EoN Entity.
        """
        return self._register_edge_node(eon_name)  # Get reference for the

    def get_edge_device(self, eon_name: str, eond_name: str) -> DeviceEntity:
        """
        Get reference for the virtual Edge Node Device (EoND) entity.

        Args:
            eon_name:   EoN name
            eond_name:  EoN Device name
        """
        # If not created EoN, create it
        self._register_edge_node(eon_name=eon_name)

        # Create EoND and return reference
        return self._register_edge_device(eon_name=eon_name,eond_name=eond_name)
