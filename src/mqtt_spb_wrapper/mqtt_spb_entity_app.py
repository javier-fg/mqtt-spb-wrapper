import time
from typing import Dict

from .spb_base import SpbTopic, SpbPayloadParser
from .mqtt_spb_entity import SpbEntity
from .mqtt_spb_entity import MqttSpbEntity


class MqttSpbEntityApp(MqttSpbEntity):

    class DeviceEntity(SpbEntity):
        """
            Entity Edge Device (EoND) class

            This class is used to represent virtually an EoND entity from an application.
            The application entity will automatically parse the received entity data into its
            representation, allowing you to easly interact with the entity ( via commands ) or
            to get the lates telemetry and attribute values.

            This class allows you to:
              - Subscribe to callback events when data from the entity is received ( BIRTH, DATA, DEATH )
              - Get the latest data values ( entity.data or entity.attributes )
              - Send a Command/s to the physical entity
        """

        def __init__(self,
                     spb_domain_name, spb_eon_name, spb_eon_device_name,
                     callback_birth=None, callback_data=None, callback_death=None,
                     debug_enabled=False):

            super().__init__(
                spb_domain_name=spb_domain_name,
                spb_eon_name=spb_eon_name,
                spb_eon_device_name=spb_eon_device_name,
                debug_enabled=debug_enabled,
                debug_id="MQTT_SPB_APP_DEVICE"
            )

            self.callback_birth = callback_birth  # Save callback function references
            self.callback_data = callback_data
            self.callback_death = callback_death

            self._is_alive = False  # Device is alive ( BIRTH + DATA ) or not ( DEATH )

        def is_alive(self):
            return self._is_alive

    class EdgeEntity(SpbEntity):
        """
            Entity Edge Node entity (EoN) class

            This class is used to represent virtually an EoN entity from an application entity.
            The application entity will automatically parse the received entity data into its
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
                     callback_birth=None, callback_data=None, callback_death=None,
                     debug_enabled=False):

            super().__init__(spb_domain_name=spb_domain_name,
                             spb_eon_name=spb_eon_name,
                             debug_enabled=debug_enabled, debug_id="MQTT_SPB_APP_EDGENODE")

            self.callback_birth = callback_birth  # Save callback function references
            self.callback_data = callback_data
            self.callback_death = callback_death

            self.entities_eond: Dict[str, MqttSpbEntityApp.DeviceEntity] = {}

            self._is_alive = False  # Device is alive ( BIRTH + DATA ) or not ( DEATH )

        def is_alive(self):
            return self._is_alive

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
                 spb_app_name,     # Application name
                 callback_birth=None, callback_data=None, callback_death=None,  # callbacks for different spb messages
                 callback_new_eon=None, callback_new_eond=None,
                 retain_birth=False,
                 debug_enabled=False):
        """

        Initiate the spb application entity

        Args:
            spb_domain_name:  Sparkplug B domain name
            spb_app_name:     Application entity ID ( will be part of the MQTT topic )
            debug_enabled:       Enable / Disable debug information.
        """

        # Initialized the object ( parent class ) with Device_id as None - Configuring it as edge node
        super().__init__(
            spb_domain_name=spb_domain_name,
            spb_eon_name=spb_app_name,
            spb_eon_device_name=None,
            retain_birth=retain_birth,
            entity_is_scada=False,
            debug_enabled=debug_enabled,
            debug_id="MQTT_SPB_APP",
         )

        self.entities_eon: Dict[str, MqttSpbEntityApp.EdgeEntity] = {}
        '''
        List of current discovered edge nodes entities (EoN)
        '''

        self.callback_birth = callback_birth  # Save callback function references
        self.callback_data = callback_data
        self.callback_death = callback_death

        self.callback_new_eon = callback_new_eon    # Callbacks for new entities
        self.callback_new_eond = callback_new_eond

        self._spb_initialized = False  # Flag to mark the initialization of spb persistent messages(BIRTH, DEATH)
        self._spb_initialized_timeout = 0  # Counter to keep initialization timeout event

        self._debug_enabled = debug_enabled

        self._logger.info("New spb APP object")

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
                timeout: int = 5,
                skip_death=False,
                ) -> bool:

        # Set the initialization of spb messages ( BIRTH and DEATH )
        self._spb_initialized = False
        self._spb_initialized_timeout = time.time()

        # Call the parent method
        return super().connect(
            host=host, port=port,
            user=user, password=password,
            use_tls=use_tls,
            tls_ca_path=tls_ca_path, tls_cert_path=tls_cert_path, tls_key_path=tls_key_path,
            tls_insecure=tls_insecure,
            timeout=timeout,
            skip_death=skip_death
        )

    def disconnect(self, skip_death_publish=False):

        # Clear the initialization of spb messages ( BIRTH and DEATH )
        self._spb_initialized = False

        # Call the parent method
        return super().disconnect(skip_death_publish)

    def is_initialized(self):
        """
        Returns True if application is initialized.
        This initialization time is used to retrieve all BIRTH messages from the broker during an specific timeout.

            Returns: True when initialized

        """

        # Check if the timeout has expired
        if not self._spb_initialized:

            # If timeout expired, then mark the finalization of the initialization period
            if time.time() > (self._spb_initialized_timeout + 0.5):

                # spb persistent messages are being initialized
                self._spb_initialized = True

                # Reset entity state for all discovered entities
                for eon in self.entities_eon.keys():
                    self.entities_eon[eon]._is_alive = False

                    for eond in self.entities_eon[eon].entities_eond.keys():
                        self.entities_eon[eon].entities_eond[eond]._is_alive = False

                self._logger.debug("spB initialization period expired, all entity states are cleared.")

        return self._spb_initialized

    def _mqtt_on_connect(self, client, userdata, flags, rc):

        # Subscribe to all group topics
        if rc == 0:
            topic = "%s/%s/#" % (self._spb_namespace,
                                 self._spb_domain_name)
            self._mqtt.subscribe(topic)
            self._logger.info("%s - Subscribed to MQTT topic: %s" % (self._entity_domain, topic))

        else:
            self._logger.error(" %s - Could not connect to MQTT server !" % self._entity_domain)

        # Execute the callback function if it is not None
        if self.on_connect is not None:
            self.on_connect(rc)

    def _mqtt_on_message(self, client, userdata, msg):

        # self._logger.info("%s - Message received  %s" % (self._entity_domain, msg.topic))

        # Check if it is initialized
        if not self._spb_initialized:
            self.is_initialized()   # Ignore return value, just run function to initialize devices

        # Process the message for edge nodes ( EoN and EoND )
        topic = SpbTopic(msg.topic)
        eon_name = topic.eon_name
        eond_name = topic.eon_device_name

        # Ignore data from self application
        if eon_name == self.entity_name:
            return  # Exit and ignore the data

        # EDGE NODE - Let's check if the EdgeNode is already discovered, if not create it
        self._register_edge_node(eon_name=eon_name)

        # DEVICE NODE - Lets check if device is discovered, otherwise create it.
        if eond_name is not None:
            self._register_edge_device(eon_name=eon_name, eond_name=eond_name)

        # Select the entity (EoN / EoND) to process the data and inject the data into the entity
        if eond_name is None:
            entity = self.entities_eon[eon_name]
        else:
            entity = self.entities_eon[eon_name].entities_eond[eond_name]

        # PARSE PAYLOAD - De-serialize payload and update device data

        payload = SpbPayloadParser(msg.payload)

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

        # Send message to Entity
        super()._mqtt_on_message(client, userdata, msg)


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
            self.entities_eon[eon_name] = MqttSpbEntityApp.EdgeEntity(
                spb_domain_name=self.spb_domain_name,
                spb_eon_name=eon_name,
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

            self.entities_eon[eon_name].entities_eond[eond_name] = MqttSpbEntityApp.DeviceEntity(
                spb_domain_name=self.spb_domain_name,
                spb_eon_name=eon_name,
                spb_eon_device_name=eond_name,
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
