import logging
import time
from typing import Callable, Any

from .spb_protobuf import Payload

from google.protobuf.json_format import MessageToDict
from .spb_protobuf import getDdataPayload, getNodeBirthPayload, getDeviceBirthPayload
from .spb_protobuf import addMetric, MetricDataType
from .spb_protobuf.sparkplug_b import addMetricDataset_from_dict


class MetricValue:
    """
    Metric Value Class

    Class to encapsulate the basic sparkplug B metric value properties and functions

    Args:
        name: Metric value name
        value: Metric values
        timestamp: Metric timestamp in milliseconds
        callback_on_change: callback reference for on value change events.

    Returns:
        object: Initialized class object.
    """

    def __init__(
            self,
            name: str,
            value,
            timestamp=None,
            callback_on_change: Callable[[Any], None] = None
    ):

        self.name = name
        self.is_updated = True
        self._callback = callback_on_change

        if isinstance(value, list) and isinstance(timestamp, list) and len(value) == len(timestamp):
            self._value = value
            self._timestamp = timestamp
        else:
            if isinstance(value, list):
                self._value = value
            else:
                self._value = [value]

            if isinstance(timestamp, list):
                self._timestamp = timestamp
            else:
                if timestamp is None:
                    self._timestamp = [int(time.time() * 1000)]
                else:
                    self._timestamp = [timestamp]

    def __str__(self):
        return str(self.as_dict())

    def __repr__(self):
        return str(self.as_dict())

    def is_single_value(self):
        """ Returns True if there is only one value and timestamp """
        return (len(self._value) == 1) or (len(self._timestamp) != len(self._value))

    def as_dict(self) -> dict:
        """
        Get a dictionary contain the current class property values.

        Returns: dictionary

        """
        is_updated = self.is_updated
        data = {
            "name": self.name,
            "value": self.value,
            "timestamp": self.timestamp,
            "is_updated": is_updated,
            "is_single_value": self.is_single_value(),
        }
        self.is_updated = is_updated
        return data

    def set(self, value, timestamp):
        self.value = value
        self.timestamp = timestamp

    @property
    def value(self):
        """
        Metric value.

        Returns:

        """
        self.is_updated = False

        if self.is_single_value():
            return self._value[0]
        else:
            return self._value

    @value.setter
    def value(self, value):

        if isinstance(value, list):
            self._value = value
        else:
            self._value = [value]

        # If a callback is configured, execute it and pass the value
        if self._callback is not None:
            self._callback(self.value)

        # Set updated flag
        self.is_updated = True

    @property
    def timestamp(self):
        """
        metric timestamp in milliseconds

        Returns:

        """
        if self.is_single_value():
            return self._timestamp[0]
        else:
            return self._timestamp

    @timestamp.setter
    def timestamp(self, timestamp):

        if timestamp is None:
            self.timestamp_update()
        else:
            if isinstance(timestamp, list):
                self._timestamp = timestamp
            else:
                self._timestamp = [timestamp]

    def timestamp_update(self):
        """
        Set metric current stamp based in current system time clock.

        Returns: Nothing

        """
        self.timestamp = int(time.time() * 1000)

    @property
    def callback_on_change(self) -> Callable[[Any], None]:
        """
            Callback function reference for on value change events

        Returns:

        """
        return self._callback

    @callback_on_change.setter
    def callback_on_change(self, callback: Callable[[Any], None]):
        self._callback = callback

    def has_callback(self):
        """
        Check if current value has a callback function configured.

        Returns:  True if a callback function is set
        """
        return self._callback is not None


class MetricGroup:
    """
    Metric Group class

    This class is used to group a set of MetricValues, representing multiple metric fields in the group.

    This is used to group the metrics into DATA, ATTRIBUTES or COMMANDS.
    """

    def __init__(self):

        self._items = {}
        self.seq_number = None

    def __str__(self):
        return str(self.get_dictionary())

    def __repr__(self):
        return str(self.get_dictionary())

    def get_dictionary(self):
        """
        Get a dictionary contain the current class property values.

        Returns: dictionary
        """
        temp = []
        if len(self._items) > 0:
            for item in self._items.values():
                temp.append(item.as_dict())
        return temp

    def as_dict(self):
        return self.get_dictionary()

    def get_names(self):
        return self._items.keys()

    def get_values(self):
        return self._items.values()

    def keys(self):
        return self.get_names()

    def values(self):
        return self.get_values()

    def get_value(self, field_name):
        if field_name in self._items.keys():
            return self._items[field_name].value
        return None

    def is_empty(self) -> bool:
        """
        True if there is not metrics for the current group
        Returns:

        """
        if not self._items:
            return True
        return False

    def is_single_value(self, name) -> bool:
        """
        True if some metric value has been updated

        Returns:

        """
        if name in self._items.keys():
            return self._items[name].is_single_value()
        return True

    def is_updated(self) -> bool:
        """
        True if some metric value has been updated

        Returns:

        """
        for item in self._items.values():
            if item.is_updated:
                return True
        return False

    def clear(self):
        """
        Reset and initialize the Metric Group object.

        Returns:

        """
        self._items = {}

    def count(self) -> int:
        """
        Return the number of metric values
        Returns:

        """
        return len(self._items)

    def set_value(self, name: str, value, timestamp=None, callback_on_change=None):
        """
        Initialize/set a metric value

        Args:
            name: Metric name
            value: Metric value
            timestamp: Epoc timestamp in milliseconds. If set to None timestamp set to current UTC timestamp.
            callback_on_change: function reference for on change events.

        Returns:

        """
        # If value is set to None, ignore the update
        if value is None:
            return False

        # If exist update the value, otherwise add the element.
        if name in self._items.keys():
            self._items[name].value = value
            self._items[name].timestamp = timestamp
            return True

        # item was not found, then add it to the list.
        new_item = MetricValue(
            name=name,
            value=value,
            timestamp=timestamp,
            callback_on_change=callback_on_change
        )

        self._items[name] = new_item
        return True

    def remove_value(self, name: str) -> bool:
        """
        Remove a metric value from the group

        Args:
            name: metric name

        Returns:  True if value was removed.

        """
        if name in self._items.keys():
            self._items.pop(name)
            return True
        else:
            return False

    def set_dictionary(self, values: dict, timestamp: int = None) -> bool:
        """

        Args:
            values: Dictionary with fields-values
            timestamp:  Epoc timestamp in milliseconds

        Returns:

        """
        # Update the list of values
        for k, v in values.items():
            self.set_value(k, v, timestamp)

        return True

    # Implementation of Dictionary Like operations -------------------------------------------------

    # Get an item like a dictionary
    def __getitem__(self, name) -> MetricValue:
        return self._items[name]

    # Set an item like a dictionary
    def __setitem__(self, name, value: MetricValue):
        self._items[name] = value

    # Delete an item like a dictionary
    def __delitem__(self, name):
        del self._items[name]

    # Optionally, allow iteration (e.g., for looping through keys)
    def __iter__(self):
        return iter(self._items)

    # Optionally, provide the length (e.g., for len() function)
    def __len__(self):
        return len(self._items)


class SpbEntity:
    """
    Sparkplug B Entity Class

    Initialize the class and set some basic configuration parameters.

    Args:
        spb_domain_name (str): spB Domain name
        spb_eon_name (str): spB Edge of Network (EoN) node name
        spb_eon_device_name (str, optional): spB Edge of Network Devide (EoND) node name. If set to None, the entity is of an EoN type.
        spb_host_app_name (str, optional): spB Primary Host(AKA SCADA) Application name. If set to None, will listen to any STATE messages.
        debug_enabled ( bool, optional): Enable console debug messages
        debug_id ( str, optional ): Console debug identification for the class messages.
    """

    def __init__(
            self,
            spb_domain_name: str,
            spb_eon_name: str,
            spb_eon_device_name: str = None,
            spb_host_app_name: str = None,
            debug_enabled: bool = False,
            debug_id: str = "SPB_ENTITY",
    ):

        # Public members -----------
        self.is_birth_published = False

        self.attributes = MetricGroup()
        self.data = MetricGroup()
        self.commands = MetricGroup()

        # Private members -----------
        self._spb_domain_name = spb_domain_name
        self._spb_eon_name = spb_eon_name
        self._spb_eon_device_name = spb_eon_device_name
        self._spb_host_app_name = spb_host_app_name

        if spb_eon_device_name is None:
            self._entity_domain = "spBv1.%s.%s" % (self._spb_domain_name, self._spb_eon_name)
        else:
            self._entity_domain = "spBv1.%s.%s.%s" % (self._spb_domain_name, self._spb_eon_name, self._spb_eon_device_name)

        if spb_eon_device_name is None:
            self._entity_name = self._spb_eon_name
        else:
            self._entity_name = self._spb_eon_device_name

        self._debug_enabled = debug_enabled     # Debug parameters
        self._debug_id = debug_id
        self._update_debug_id()

        self._logger.info("%s - New %s created " % (self._entity_domain, debug_id))

    def __str__(self):
        return str(self.get_dictionary())

    def __repr__(self):
        return str(self.get_dictionary())

    def get_dictionary(self) -> dict:
        """
            Get the spB entity properties and main values as Dictionary.

        Returns:    Dictionary containing the main class properties s

        """


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

    def _update_debug_id(self):
        """
            Update the console debug logger

        Returns:    Nothing

        """

        # Console Logger
        self._logger = logging.getLogger(self._debug_id)
        handler_log = logging.StreamHandler()
        handler_log.setFormatter(logging.Formatter('%(asctime)s %(name)s %(levelname)s | %(message)s'))
        self._logger.addHandler(handler_log)

        # Set level
        if self._debug_enabled:
            self._logger.setLevel(logging.DEBUG)
        else:
            self._logger.setLevel(logging.ERROR)

    @property
    def debug_enabled(self):
        return self._debug_enabled

    @debug_enabled.setter
    def debug_enabled(self, debug_enabled):

        self._debug_enabled = debug_enabled

        if self._debug_enabled:
            self._logger.setLevel(logging.DEBUG)
        else:
            self._logger.setLevel(logging.ERROR)

    @property
    def debug_id(self) -> str:
        return self._debug_id

    @debug_id.setter
    def debug_id(self, debug_id_name):
        self._debug_id = debug_id_name
        self._update_debug_id()

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

        if self._spb_eon_device_name is None:  # If EoN type
            payload = getNodeBirthPayload()
        else:  # Device
            payload = getDeviceBirthPayload()

        # Attributes
        if not self.attributes.is_empty():
            for item in self.attributes.values():
                name = "ATTR/" + item.name
                # If multiple values send it as DataSet
                if not item.is_single_value():
                    addMetricDataset_from_dict(payload, name=name, alias=None,
                                               data={"timestamps": item.timestamp, "values": item.value})
                else:
                    addMetric(payload, name, None, self._spb_data_type(item.value), item.value, item.timestamp)

        # Data
        if not self.data.is_empty():
            for item in self.data.values():
                name = "DATA/" + item.name
                # If multiple values send it as DataSet
                if not item.is_single_value():
                    addMetricDataset_from_dict(payload, name=name, alias=None,
                                               data={"timestamps": item.timestamp, "values": item.value})
                else:
                    addMetric(payload, name, None, self._spb_data_type(item.value), item.value, item.timestamp)

        # Commands
        if not self.commands.is_empty():
            for item in self.commands.values():
                name = "CMD/" + item.name
                # If multiple values send it as DataSet
                if not item.is_single_value():
                    addMetricDataset_from_dict(payload, name=name, alias=None,
                                               data={"timestamps": item.timestamp, "values": item.value})
                else:
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
                    if field.get("value"):
                        # Check if multiple values are being send as DataSet or Metric
                        if "datasetValue" in field.keys():
                            # Get they dataSet values
                            columns_data = {column: [] for column in field['datasetValue']['columns']}
                            for row in field['datasetValue']['rows']:
                                for idx, element in enumerate(row['elements']):
                                    column_name = field['datasetValue']['columns'][idx]
                                    # Append the intValue to the respective column list
                                    if 'intValue' in element:
                                        columns_data[column_name].append(element['intValue'])

                            # They should contain the "timestamps" and "values" items, otherwise ignore
                            if "timestamps" in columns_data.keys() and "values" in columns_data.keys():
                                if len(columns_data['timestamps']) == len(columns_data['values']):
                                    self.attributes.set_value(
                                        name=field['name'],
                                        value=columns_data["values"],
                                        timestamp=[int(k) for k in columns_data['timestamps']]  # Force as integer
                                    )
                        else:
                            self.attributes.set_value(field['name'], field['value'], field['timestamp'])  # update field

                elif field['name'].startswith("CMD/"):
                    field['name'] = field['name'][4:]
                    if field.get("value"):
                        # Check if multiple values are being send as DataSet or Metric
                        if "datasetValue" in field.keys():
                            # Get they dataSet values
                            columns_data = {column: [] for column in field['datasetValue']['columns']}
                            for row in field['datasetValue']['rows']:
                                for idx, element in enumerate(row['elements']):
                                    column_name = field['datasetValue']['columns'][idx]
                                    # Append the intValue to the respective column list
                                    if 'intValue' in element:
                                        columns_data[column_name].append(element['intValue'])

                            # They should contain the "timestamps" and "values" items, otherwise ignore
                            if "timestamps" in columns_data.keys() and "values" in columns_data.keys():
                                if len(columns_data['timestamps']) == len(columns_data['values']):
                                    self.commands.set_value(
                                        name=field['name'],
                                        value=columns_data["values"],
                                        timestamp=[int(k) for k in columns_data['timestamps']]  # Force as integer
                                    )
                        else:
                            self.commands.set_value(field['name'], field['value'], field['timestamp'])  # update field

                elif field['name'].startswith("DATA/"):
                    field['name'] = field['name'][5:]
                    if field.get("value"):
                        # Check if multiple values are being send as DataSet or Metric
                        if "datasetValue" in field.keys():
                            # Get they dataSet values
                            columns_data = {column: [] for column in field['datasetValue']['columns']}
                            for row in field['datasetValue']['rows']:
                                for idx, element in enumerate(row['elements']):
                                    column_name = field['datasetValue']['columns'][idx]
                                    # Append the intValue to the respective column list
                                    if 'intValue' in element:
                                        columns_data[column_name].append(element['intValue'])

                            # They should contain the "timestamps" and "values" items, otherwise ignore
                            if "timestamps" in columns_data.keys() and "values" in columns_data.keys():
                                if len(columns_data['timestamps']) == len(columns_data['values']):
                                    self.data.set_value(
                                        name=field['name'],
                                        value=columns_data["values"],
                                        timestamp=[int(k) for k in columns_data['timestamps']]  # Force as integer
                                    )
                        else:
                            self.data.set_value(field['name'], field['value'], field['timestamp'])  # update field

        return payload

    def serialize_payload_data(self, send_all=False):

        # Get a new payload object to add metrics to it.
        payload = getDdataPayload()

        # Iterate for each data field.
        for item in self.data.values():
            # Only send those values that have been updated, or if send_all==True then send all.
            if send_all or item.is_updated:
                # If multiple values send it as DataSet
                if not item.is_single_value():
                    addMetricDataset_from_dict(payload, name=item.name, alias=None, data={"timestamps": item.timestamp,"values": item.value} )
                else:
                    addMetric(payload, item.name, None, self._spb_data_type(item.value), item.value, item.timestamp)

        payload_bytes = bytearray(payload.SerializeToString())

        return payload_bytes

    def deserialize_payload_data(self, data_bytes):

        payload = SpbPayload(data_bytes).payload

        if payload is not None:

            # Iterate over the metrics to update the data fields
            for field in payload.get('metrics', []):

                # Check if multiple values are being send as DataSet or Metric
                if "datasetValue" in field.keys():
                    # Get they dataSet values
                    columns_data = {column: [] for column in field['datasetValue']['columns']}
                    for row in field['datasetValue']['rows']:
                        for idx, element in enumerate(row['elements']):
                            column_name = field['datasetValue']['columns'][idx]
                            # Append the intValue to the respective column list
                            if 'intValue' in element:
                                columns_data[column_name].append(element['intValue'])

                    # They should contain the "timestamps" and "values" items, otherwise ignore
                    if "timestamps" in columns_data.keys() and "values" in columns_data.keys():
                        if len(columns_data['timestamps']) == len(columns_data['values']):
                            self.data.set_value(
                                name=field['name'],
                                value=columns_data["values"],
                                timestamp=[ int(k) for k in columns_data['timestamps']] # Force as integer
                            )
                else:
                    self.data.set_value(field['name'], field['value'], field['timestamp'])  # update field

        return payload

class SpbTopic:
    """
    Sparkplug B Topic class

    Class used to parse MQTT topic string and discover all different Sparkplug b properties from the topic.

    Args:
            topic_str: MQTT topic string value to parse
    """

    def __init__(self, topic_str: str = None):

        self.topic: str = ""
        """
        Current MQTT topic string
        """

        self.namespace = None
        """
        Name space representation ( default - spBv1.0 )
        """

        self.domain_name = None
        """
        spB domain name
        """

        self.message_type = None
        """
        spB message name ( as per spB specification DBIRTH, DDATA, NDEATH, ... )
        """

        self.eon_name = None
        """
        spb EoN name
        """

        self.eon_device_name = None
        """
        spb EoND name
        """

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
        if "spBv1.0/STATE" in topic_str:
            self.domain_name = None
            self.message_type = "STATE"

            self.eon_name = None
            self.eon_device_name = None
            self.entity_name = None

            # If EoN
            if len(topic_fields) > 2:
                self.eon_name = topic_fields[2]
                self.entity_name = self.eon_name

            # If EoN device type
            if len(topic_fields) > 3:
                self.eon_device_name = topic_fields[3]
                self.entity_name = self.eon_device_name

            self.domain = "%s.%s.%s" % (self.namespace, self.eon_name)
            if self.eon_device_name is not None:
                self.domain += ".%s" % self.eon_device_name

        else:
            self.domain_name = topic_fields[1]
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

            self.domain = "%s.%s.%s" % (self.namespace, self.domain_name, self.eon_name)
            if self.eon_device_name is not None:
                self.domain += ".%s" % self.eon_device_name

        return str(self)

    def generate_topic(self) -> str:
        """
        Generate the MQTT topic based on entity values

        Returns: string representing the MQTT topic

        """

        self.namespace = "spBv1.0"

        if self.message_type == "STATE":
            out = "%s/%s/%s" % (
                self.namespace,
                self.message_type,
                self.eon_name,
            )
        else:
            out = "%s/%s/%s/%s" % (
                self.namespace,
                self.domain_name,
                self.message_type,
                self.eon_name,
            )

        if self.eon_device_name:
            out += "/" + self.eon_device_name

        return out

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
