from .mqtt_spb_entity import MqttSpbEntity

from .spb_protobuf import getDdataPayload, getValueDataType
from .spb_protobuf import addMetric


class MqttSpbEntityEdgeNode(MqttSpbEntity):

    def __init__(self, spb_domain_name, spb_eon_name,
                 retain_birth=False,
                 debug_enabled=False, debug_id="MQTT_SPB_EDGENODE",
                 include_spb_rebirth=True):

        # Initialized the object ( parent class ) with Device_id as None - Configuring it as edge node
        super().__init__(spb_domain_name=spb_domain_name, spb_eon_name=spb_eon_name,
                         retain_birth=retain_birth,
                         debug_enabled=debug_enabled, debug_id=debug_id)

        # Add spB Birth command as per Specifications
        if include_spb_rebirth:
            self.commands.set_value(name="Node Control/Rebirth",
                                    value=False,
                                    callback_on_change=self.publish_birth())

    def publish_command_device(self, spb_eon_device_name, commands):

        if not self.is_connected():  # If not connected
            self._logger.warning(
                "%s - Could not send publish_command_device(), not connected to MQTT server" % self._entity_domain)
            return False

        if not isinstance(commands, dict):  # If no data commands as dictionary
            self._logger.warning(
                "%s - Could not send publish_command_device(), commands not provided or not valid. Please provide a dictionary of command:value" % self._entity_domain)
            return False

        # Get a new payload object, to add metrics
        payload = getDdataPayload()

        # Add the list of commands to the payload metrics
        for k in commands:
            addMetric(payload, k, None, getValueDataType(commands[k]), commands[k])

        # Send payload if there is new data
        topic = "%s/%s/DCMD/%s/%s" % (self._spb_namespace,
                                      self._spb_domain_name,
                                      self._spb_eon_name,
                                      spb_eon_device_name)

        if payload.metrics:
            payload_bytes = bytearray(payload.SerializeToString())
            self._loopback_topic = topic
            self._mqtt_payload_publish(topic, payload_bytes)

            self._logger.info("%s - Published COMMAND message to %s" % (self._entity_domain, topic))

            return True

        self._logger.warning("%s - Could not publish COMMAND message to %s" % (self._entity_domain, topic))
        return False

