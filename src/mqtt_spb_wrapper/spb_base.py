import datetime
import logging
from typing import Callable, Any

from .spb_protobuf import Payload

from google.protobuf.json_format import MessageToDict
from .spb_protobuf import getDdataPayload, getNodeBirthPayload, getDeviceBirthPayload
from .spb_protobuf import addMetric, MetricDataType


class SpbEntity:

    def __init__(self,
                 spb_domain_name,
                 spb_eon_name,
                 spb_eon_device_name=None,
                 debug_info=False,
                 debug_id="SPB_ENTITY"):

        # Public members -----------
        self.is_birth_published = False

        self.attributes = self.MetricGroup()
        self.data = self.MetricGroup()
        self.commands = self.MetricGroup()

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

        # Console Logger
        self._logger = logging.getLogger(debug_id)
        handler_log = logging.StreamHandler()
        handler_log.setFormatter(logging.Formatter('%(asctime)s %(name)s %(levelname)s | %(message)s'))
        self._logger.addHandler(handler_log)
        if debug_info:
            self._logger.setLevel(logging.DEBUG)
        else:
            self._logger.setLevel(logging.ERROR)

        self._logger.info("%s - New %s created " % (self._entity_domain, debug_id))

    def __str__(self):
        return str(self.get_dictionary())

    def __repr__(self):
        return str(self.get_dictionary())

    def get_dictionary(self):

        temp = {'spb_domain_name': self._spb_domain_name,
                'spb_eon_name': self._spb_eon_name}

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

        payload = SpbPayload(data_bytes).payload

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

        payload = SpbPayload(data_bytes).payload

        if payload is not None:

            # Iterate over the metrics to update the data fields
            for field in payload.get('metrics', []):
                self.data.set_value(field['name'], field['value'], field['timestamp'])  # update field

        return payload

    class MetricValue:

        def __init__(self, name, value, timestamp=None, callback: Callable[[Any], None] = None):

            self.name = name
            self._value = value
            if timestamp is None:
                self._timestamp = int(datetime.datetime.utcnow().timestamp() * 1000)
            else:
                self._timestamp = timestamp
            self.is_updated = True
            self._callback = callback

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

            # If a callback is configured, execute it and pass the value
            if self._callback is not None:
                self._callback(self._value)

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

        def set_callback(self, callback: Callable[[Any], None]):
            self._callback = callback

        def has_callback(self):
            """
            Check if current value has a callback function configured.

            Returns:  True if a callback function is set
            """
            return self._callback is not None

    class MetricGroup:

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

        def set_value(self, name, value, timestamp=None, callback=None):

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
            self.values.append(SpbEntity.MetricValue(name=name, value=value, timestamp=timestamp, callback=callback))

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
            for k, v in values.items():
                self.set_value(k, v, timestamp)

            return True


class SpbTopic:
    """
        Class used to parse MQTT topic string and discover all different Sparkplug b entities
    """

    def __init__(self, topic_str=None):

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

        self.domain = "%s.%s.%s" % (self.namespace, self.group_name, self.eon_name)
        if self.eon_device_name is not None:
            self.domain += ".%s" % self.eon_device_name

        return str(self)


class SpbPayload:
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

    def as_dict(self):
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
                return None

            if _payload == "OFFLINE" or _payload == "ONLINE":
                self.payload = _payload
                return self.payload

            return None

        self.payload = payload  # Save the current payload
        return self.payload
