import logging
import time
from io import TextIOWrapper, BufferedReader
from typing import Callable, Any
from datetime import datetime
import uuid
import base64

from google.protobuf.json_format import MessageToDict
from .spb_protobuf import getDdataPayload, getNodeBirthPayload, getDeviceBirthPayload, Payload, getValueDataType
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
            timestamp:int = None,
            callback_on_change: Callable[[Any], None] = None,
            spb_data_type: MetricDataType = None,
            spb_alias_num: int = None,
    ):
        self.name = name
        self.is_updated = True
        self.spb_alias_num = spb_alias_num
        self._callback = callback_on_change

        # If data provided as list of values ( values + timestamps )
        if isinstance(value, list) and isinstance(timestamp, list) and len(value) == len(timestamp):
            self._value = value
            self._timestamp = timestamp

            # Now we force value to be of a single type, for data type detection in following function steps.
            value = value[0]

        # Data is single point
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
                    self._timestamp = [int(timestamp)]

        # Data type detection
        if spb_data_type is not None:
            self._spb_data_type = spb_data_type
        else:
            self._spb_data_type = getValueDataType(value)

            # If unknown type, trigger an exception
            if self._spb_data_type is MetricDataType.Unknown:
                raise ValueError(f"Unsupported value type for metric '{name}': {type(value)}")

    def is_list_values(self):
        """ Returns True if there is only one value and timestamp """
        result = (len(self._value) == 1) or (len(self._timestamp) != len(self._value))
        return not result

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
            "spb_data_type": self.spb_data_type,
            "is_updated": is_updated,
            "is_list_values": self.is_list_values(),
            "has_callback": self.has_callback(),
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

        if not self.is_list_values():
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
        if self.is_list_values():
            return self._timestamp
        else:
            return self._timestamp[0]

    @timestamp.setter
    def timestamp(self, timestamp):

        if timestamp is None:
            self.timestamp_update()
        else:
            if isinstance(timestamp, list):
                self._timestamp = timestamp
            else:
                self._timestamp = [int(timestamp)]

    def timestamp_update(self):
        """
        Set metric current stamp based in current system time clock.

        Returns: Nothing

        """
        self.timestamp = int(time.time() * 1000)

    @property
    def callback(self) -> Callable[[Any], None]:
        """
            Callback function reference for on value change events

        Returns:

        """
        return self._callback

    @callback.setter
    def callback(self, callback: Callable[[Any], None]):
        self._callback = callback

    def has_callback(self):
        """
        Check if current value has a callback function configured.

        Returns:  True if a callback function is set
        """
        return self._callback is not None

    @property
    def spb_data_type(self):
        """
         Get the metric data type of the SPB entity

        Returns: MetricDataType
        """
        return self._spb_data_type

    @spb_data_type.setter
    def spb_data_type(self, data_type : MetricDataType):
        """
        Set the metric data type of the SPB entity.

        Note that you can enforce specific data types

        Args:
            data_type: MetricDataType

        Returns: Nothing

        """
        self._spb_data_type = data_type


    def __str__(self):
        return str(self.as_dict())

    def __repr__(self):
        return str(self.as_dict())

class MetricGroup:
    """
    Metric Group class

    This class is used to group a set of MetricValues, representing multiple metric fields in the group.

    This is used to group the metrics into DATA, ATTRIBUTES or COMMANDS.
    """

    def __init__(self, birth_prefix=""):

        self._items = {}
        self.seq_number = None
        self.birth_prefix = birth_prefix

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

    def get_value_timestamp(self, field_name):
        if field_name in self._items.keys():
            return self._items[field_name].timestamp
        return None

    def is_empty(self) -> bool:
        """
        True if there is not metrics for the current group
        Returns:

        """
        if not self._items:
            return True
        return False

    def is_list_values(self, name) -> bool:
        """
        True if some metric value has been updated

        Returns:

        """
        if name in self._items.keys():
            return self._items[name].is_list_values()
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

    def set_callback(self,
                     name: str,
                     callback: Callable[[Any], None]
                     ) -> bool:

        # If exist update the value, otherwise add the element.
        if name in self._items.keys():
            self._items[name].callback = callback
            return True
        else:
            return False

    def set_value(self,
                  name: str, value,
                  timestamp=None,
                  callback_on_change=None,
                  spb_alias_num=None,
                  spb_data_type=None,
                  skip_callback:bool = False,
                  ):
        """
        Initialize/set a metric value

        Args:
            name: Metric name
            value: Metric value
            timestamp: Epoc timestamp in milliseconds. If set to None timestamp set to current UTC timestamp.
            callback_on_change: function reference for on change events.
            spb_data_type: MetricDataType - If None, the type will be automatically assigned.
            skip_callback: If true, the execution of callback will be skipped ( typically used on Birth messages )

        Returns: boolean - operation successful

        """

        # If value is set to None, ignore the update
        if value is None:
            return False

        # If exist update the value, otherwise add the element.
        if name in self._items.keys():
            self._items[name].timestamp = timestamp

            # Setting the MetricValue.value will trigger the callback ( if callback is set ).
            # We can enforce to skip the callback on value change ( typically on birth messages ).
            if not skip_callback:
                self._items[name].value = value
            else:
                # Save callback reference, and set it to None
                temp_callback = self._items[name].callback
                self._items[name].callback = None
                # Update value
                self._items[name].value = value
                # Restore callback
                self._items[name].callback = temp_callback

            return True

        # item was not found, then add it to the list.
        new_item = MetricValue(
            name=name,
            value=value,
            timestamp=timestamp,
            callback_on_change=callback_on_change,
            spb_data_type=spb_data_type,
            spb_alias_num=spb_alias_num,
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
        debug_enabled ( bool, optional): Enable console debug messages
        debug_id ( str, optional ): Console debug identification for the class messages.
    """

    def __init__(
            self,
            spb_domain_name: str,
            spb_eon_name: str,
            spb_eon_device_name: str = None,
            debug_enabled: bool = False,
            debug_id: str = "SPB_ENTITY",
    ):

        # Public members -----------
        self.is_birth_published = False

        # Group of Metrics
        self.attributes = MetricGroup(birth_prefix="ATTR")
        self.data = MetricGroup(birth_prefix="DATA")
        self.commands = MetricGroup(birth_prefix="CMD")

        # Private members -----------
        self._spb_domain_name = spb_domain_name
        self._spb_eon_name = spb_eon_name
        self._spb_eon_device_name = spb_eon_device_name

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

    def _serialize_payload_metric(self, payload, name, metric_value: MetricValue):
        """
            Add spB Metric to payload based on an MetricValue.
        Args:
            payload: payload
            name: metric name
            metric_value: MetricValue

        Returns: Nothing

        """

        # If multiple values as list send it as spB DataSet
        if metric_value.is_list_values():
            addMetricDataset_from_dict(
                payload,
                name=name,
                alias=metric_value.spb_alias_num,
                data={"timestamps": metric_value.timestamp, "values": metric_value.value}
            )
            return

        # DATASET - DICT of values
        # data = {
        #     "Temperature": [23.5, 22.0, 21.8],
        #     "Humidity": [60.2, 58.9, 59.5],
        #     "Status": ["Normal", "Warning", "Alert"]
        # }
        if metric_value.spb_data_type == MetricDataType.DataSet:

            # Check if all values are lists
            if not all(isinstance(v, list) for v in metric_value.value.values()):
                raise ValueError("Not all metric values in the dictionary are lists. DatasetMetric:" + name)
            else:
                # Get the length of the first list
                first_list_length = len(next(iter(metric_value.value.values())))

                # Check if all lists have the same length
                all_lists_same_length = all(len(v) == first_list_length for v in metric_value.value.values())

                if not all_lists_same_length:
                    raise ValueError("Not all lists are of the same size. DatasetMetric:" + name)
                else:
                    addMetricDataset_from_dict(
                        payload,
                        name=name,
                        alias=metric_value.spb_alias_num,
                        data=metric_value.value
                    )
                    return

        # Convert certain metric types to correct value types--------------------------------
        # DateTime
        if metric_value.spb_data_type == MetricDataType.DateTime:
            if isinstance(metric_value.value, datetime):
                metric_value.value = int(metric_value.value.timestamp()*1000)
            else:
                metric_value.value = int(metric_value.value)
        # UUID
        elif metric_value.spb_data_type == MetricDataType.UUID:
            if isinstance(metric_value.value, uuid.UUID):
                metric_value.value = str(metric_value.value)
            else:
                metric_value.value = str(metric_value.value)
        #BYTES
        elif metric_value.spb_data_type == MetricDataType.Bytes:
            metric_value.value = bytes(metric_value.value)

        #FILE
        elif metric_value.spb_data_type == MetricDataType.File:
            if isinstance(metric_value.value, TextIOWrapper):
                metric_value.value.seek(0)
                metric_value.value = bytes(metric_value.value.read().encode('utf-8'))
            elif isinstance(metric_value.value, BufferedReader):
                metric_value.value.seek(0)
                metric_value.value = bytes(metric_value.value.read())
            else:
                metric_value.value = bytes(metric_value.value)

        # Add metric
        addMetric(
            payload,
            name=name,
            alias=metric_value.spb_alias_num,
            type=metric_value.spb_data_type,
            value=metric_value.value,
            timestamp=metric_value.timestamp
        )

    def _deserialize_payload_metric(self, value_group: MetricGroup, metric_value: dict, skip_callback: bool = False):
        """
            Parse a metric value from a spB payload and insert it into the Metric Group list of values

        Args:
            value_group:  Metric Group to store the new value
            metric_value: spB Metric data as dict, from spB payload
            skip_callback: if True, upon updating the value, if a value callback on change exits, it will not be executed

        Returns: Nothing

        """

        # If no valid value, exit
        if metric_value.get("value", None) is None:
            return

        # DATASET - DICT / VALUE LIST - Check if multiple values are being send as DataSet or Metric
        if metric_value.get("datatype") == MetricDataType.DataSet:

            # Get they dataSet values
            columns_data = {column: [] for column in metric_value['datasetValue']['columns']}

            for row in metric_value['datasetValue']['rows']:
                for idx, element in enumerate(row['elements']):
                    column_name = metric_value['datasetValue']['columns'][idx]
                    value = next(iter(element.values())) # get first element value
                    values_data_type = metric_value['datasetValue']['types'][
                        metric_value['datasetValue']['columns'].index(column_name)]

                    # If value is numeric type, convert it
                    if values_data_type == MetricDataType.Double or values_data_type == MetricDataType.Float:
                        value = float(value)
                    elif values_data_type >= MetricDataType.Int8 and values_data_type <= MetricDataType.UInt64:
                        value = int(value)
                    elif values_data_type == MetricDataType.Boolean:
                        value = bool(value)

                    # Append the Value to the respective column list
                    columns_data[column_name].append(value)

            # LIST VALUES - They should contain the "timestamps" and "values" items, otherwise it is a dictionary
            if "timestamps" in columns_data.keys() and "values" in columns_data.keys():
                if len(columns_data['timestamps']) == len(columns_data['values']):
                    # Get the values data type
                    values_data_type = metric_value['datasetValue']['types'][
                        metric_value['datasetValue']['columns'].index('values')]
                    # Add the values
                    value_group.set_value(
                        name=metric_value['name'],
                        value=columns_data["values"],
                        timestamp=[int(k) for k in columns_data['timestamps']],  # Force as integer
                        spb_data_type=values_data_type,
                        skip_callback=skip_callback,  # Dont trigger value update callback ( typically on birth data)
                    )

            # DICT DataSet - The values are a dictionary/dataset
            else:
                # Add value to the group
                value_group.set_value(
                    name=metric_value['name'],
                    value=columns_data,
                    timestamp=metric_value['timestamp'],
                    spb_data_type=metric_value['datatype'],
                    skip_callback=skip_callback,  # Dont trigger value update callback ( typically on birth data)
                )  # update field

        # DEFAULT - Add it and keep the original data type
        else:

            # Add value to the group
            value_group.set_value(
                name=metric_value['name'],
                value=metric_value['value'],
                timestamp=metric_value['timestamp'],
                spb_data_type=metric_value['datatype'],
                skip_callback=skip_callback,  # Dont trigger value update callback ( typically on birth data)
            )  # update field

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
                # Add metric to payload
                self._serialize_payload_metric(
                    payload=payload,
                    name=self.attributes.birth_prefix + "/" + item.name,
                    metric_value=item
                )

        # Data
        if not self.data.is_empty():
            for item in self.data.values():
                # Add metric to payload
                self._serialize_payload_metric(
                    payload=payload,
                    name=self.data.birth_prefix + "/" + item.name,
                    metric_value=item
                )

        # Commands
        if not self.commands.is_empty():
            for item in self.commands.values():
                # Add metric to payload
                self._serialize_payload_metric(
                    payload=payload,
                    name=self.commands.birth_prefix + "/" + item.name,
                    metric_value=item
                )

        payload_bytes = bytearray(payload.SerializeToString())

        return payload_bytes

    def deserialize_payload_birth(self, data_bytes):

        payload = SpbPayloadParser(data_bytes).payload

        if payload is not None:

            # Iterate over the metrics to update the data fields
            for field in payload.get('metrics', []):

                if field['name'].startswith(self.attributes.birth_prefix):

                    # remove the prefix
                    field['name'] = field['name'].replace(self.attributes.birth_prefix + "/", '')

                    # Insert the element in the metric group
                    self._deserialize_payload_metric(
                        value_group=self.attributes,
                        metric_value=field,
                        skip_callback=True,     # Dont trigger value update callback on birth data.
                    )

                elif field['name'].startswith(self.commands.birth_prefix):

                    # remove the prefix
                    field['name'] = field['name'].replace(self.commands.birth_prefix + "/", '')

                    # Insert the element in the metric group
                    self._deserialize_payload_metric(
                        value_group=self.commands,
                        metric_value=field,
                        skip_callback=True,  # Dont trigger value update callback on birth data.
                    )

                elif field['name'].startswith(self.data.birth_prefix):

                    # remove the prefix
                    field['name'] = field['name'].replace(self.data.birth_prefix + "/", '')

                    # Insert the element in the metric group
                    self._deserialize_payload_metric(
                        value_group=self.data,
                        metric_value=field,
                        skip_callback=True,  # Dont trigger value update callback on birth data.
                    )

        return payload

    def serialize_payload_data(self, send_all=False):

        # Get a new payload object to add metrics to it.
        payload = getDdataPayload()

        # Iterate for each data field.
        for item in self.data.values():
            # Only send those values that have been updated, or if send_all==True then send all.
            if send_all or item.is_updated:
                # Add metric to payload
                self._serialize_payload_metric(
                    payload=payload,
                    name=item.name,
                    metric_value=item
                )

        payload_bytes = bytearray(payload.SerializeToString())

        return payload_bytes

    def deserialize_payload_data(self, data_bytes):

        payload = SpbPayloadParser(data_bytes).payload

        if payload:

            # Iterate over the metrics to update the data fields
            for field in payload.get('metrics', []):

                # Insert the element in the metric group
                self._deserialize_payload_metric(
                    value_group=self.data,
                    metric_value=field
                )

            return payload
        else:
            return None

    def serialize_payload_cmd(self, send_all=False):

        # Get a new payload object to add metrics to it.
        payload = getDdataPayload()

        # Iterate for each data field.
        for item in self.commands.values():
            # Only send those values that have been updated, or if send_all==True then send all.
            if send_all or item.is_updated:
                # Add metric to payload
                self._serialize_payload_metric(
                    payload=payload,
                    name=item.name,
                    metric_value=item
                )

        payload_bytes = bytearray(payload.SerializeToString())

        return payload_bytes

    def deserialize_payload_cmd(self, data_bytes):

        payload = SpbPayloadParser(data_bytes).payload

        if payload is not None:

            # Iterate over the metrics to update the data fields
            for field in payload.get('metrics', []):

                # Insert the element in the metric group
                self._deserialize_payload_metric(
                    value_group=self.commands,
                    metric_value=field
                )

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

    def to_string(self):
        return str(self.topic)

    def __repr__(self):
        return str(self.topic)

    def parse_topic(self, topic_str):

        topic_fields = topic_str.split('/')  # Get the topic

        # Validate the number of fields
        if len(topic_fields) < 3:  # Ensure at least "namespace", "group_name", and "message_type" are present
            raise ValueError(f"Invalid topic string: {topic_str}")

        # Check name space
        if topic_fields[0] != "spBv1.0" and topic_fields[0] != "sspBv1.0":
            raise ValueError(f"Invalid topic string: {topic_str}")

        self.topic = topic_str

        self.namespace = topic_fields[0]
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

        out = "%s/%s/%s/%s" % (
            self.namespace,
            self.domain_name,
            self.message_type,
            self.eon_name,
        )

        if not self.eon_device_name:
            out += "/" + self.eon_device_name

        return out

class SpbPayloadParser:
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

        payload = None      # Temp buffer for parsing data

        try:
            pb_payload.ParseFromString(payload_data)

            # If empty conversion
            if str(pb_payload) == "":
                raise ValueError("Invalid payload")

            payload = MessageToDict(pb_payload)  # Convert it to DICT for easy handeling

            if "metrics" in payload.keys():
                for i in range(len(payload['metrics'])):

                    # value item - Add the metrics [TYPE_value] field into [value] field for convenience
                    for k in payload['metrics'][i].keys():
                        if "Value" in k:
                            payload['metrics'][i]['value'] = payload['metrics'][i][k]
                            break

                    # PARSE values - If value is numeric type, convert it
                    if payload['metrics'][i]['datatype'] == MetricDataType.Double or payload['metrics'][i]['datatype'] == MetricDataType.Float:
                        payload['metrics'][i]['value'] = float(payload['metrics'][i]['value'])
                    elif payload['metrics'][i]['datatype'] >= MetricDataType.Int8 and payload['metrics'][i]['datatype'] <= MetricDataType.UInt64:
                        payload['metrics'][i]['value'] = int(payload['metrics'][i]['value'])
                    elif payload['metrics'][i]['datatype'] == MetricDataType.Boolean:
                        payload['metrics'][i]['value'] = bool(payload['metrics'][i]['value'])
                    elif payload['metrics'][i]['datatype'] == MetricDataType.DateTime:
                        try:
                            payload['metrics'][i]['value'] = datetime.fromtimestamp(int(payload['metrics'][i]['value']) / 1000)
                        except:
                            pass
                    elif payload['metrics'][i]['datatype'] == MetricDataType.Bytes or payload['metrics'][i]['datatype'] == MetricDataType.File:
                        try:
                            payload['metrics'][i]['value'] = base64.b64decode(payload['metrics'][i]['value'])
                        except:
                            pass

                    # BUG FIX - Decoder makes alias as string, force alias to be int
                    try:
                        payload['metrics'][i]['alias'] = int(payload['metrics'][i]['alias'])
                    except:
                        pass

        except Exception as e:

            # Check if payload is from SCADA ( String type )
            try:
                _payload = payload_data.decode()

                if _payload == "OFFLINE" or _payload == "ONLINE":
                    self.payload = _payload
                    return self.payload

            except Exception as e1:
                pass

        self.payload = payload  # Save the current payload

        return self.payload
