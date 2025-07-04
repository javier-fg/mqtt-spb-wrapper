from .mqtt_spb_entity import MqttSpbEntity


class MqttSpbEntityDevice(MqttSpbEntity):

    def __init__(self, spb_group_name, spb_eon_name, spb_eon_device_name,
                 retain_birth=False,
                 debug=False
                 ):

        # Initialized the object ( parent class ) with Device_id as None - Configuring it as edge node
        super().__init__(spb_group_name=spb_group_name, spb_eon_name=spb_eon_name, spb_eon_device_name=spb_eon_device_name,
                         retain_birth=retain_birth,
                         debug=debug, debug_id="MQTT_SPB_DEVICE"
                         )

