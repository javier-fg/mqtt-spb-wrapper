from .mqtt_spb_entity_edgenode import MqttSpbEntityEdgeNode


class MqttSpbEntityApplication(MqttSpbEntityEdgeNode):

    def __init__(self,
                 spb_domain_name,
                 spb_app_entity_name,
                 retain_birth=False,
                 debug_info=False):

        # Initialized the object ( parent class ) with Device_id as None - Configuring it as edge node
        super().__init__(spb_domain_name=spb_domain_name, spb_eon_name=spb_app_entity_name,
                         retain_birth=retain_birth,
                         debug_info=debug_info, debug_id="MQTT_SPB_APP")

    # Override class method
    def _mqtt_on_connect(self, client, userdata, flags, rc):

        # Call the parent method
        super()._mqtt_on_connect(client, userdata, flags, rc)

        # Subscribe to all group topics
        if rc == 0:
            topic = "spBv1.0/" + self.spb_domain_name + "/#"
            self._mqtt.subscribe(topic)
            self._logger.info("%s - Subscribed to MQTT topic: %s" % (self._entity_domain, topic))

