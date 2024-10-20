import unittest
from datetime import datetime
import uuid

from mqtt_spb_wrapper.spb_base import SpbEntity, MetricDataType, SpbPayloadParser


class TestSpbEntity(unittest.TestCase):

    def test_initialization_eon_node(self):
        """Test initialization of SpbEntity as an EoN node."""
        entity = SpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        self.assertEqual(entity.spb_domain_name, "Group1")
        self.assertEqual(entity.spb_eon_name, "EoN1")
        self.assertIsNone(entity.spb_eon_device_name)
        self.assertEqual(entity.entity_name, "EoN1")
        self.assertEqual(entity.entity_domain, "spBv1.Group1.EoN1")
        self.assertFalse(entity.is_birth_published)
        self.assertTrue(entity.attributes.is_empty())
        self.assertTrue(entity.data.is_empty())
        self.assertTrue(entity.commands.is_empty())

    def test_initialization_eon_device(self):
        """Test initialization of SpbEntity as an EoN device."""
        entity = SpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1", spb_eon_device_name="Device1")
        self.assertEqual(entity.spb_domain_name, "Group1")
        self.assertEqual(entity.spb_eon_name, "EoN1")
        self.assertEqual(entity.spb_eon_device_name, "Device1")
        self.assertEqual(entity.entity_name, "Device1")
        self.assertEqual(entity.entity_domain, "spBv1.Group1.EoN1.Device1")
        self.assertFalse(entity.is_birth_published)
        self.assertTrue(entity.attributes.is_empty())
        self.assertTrue(entity.data.is_empty())
        self.assertTrue(entity.commands.is_empty())

    def test_set_metrics(self):
        """Test setting metrics in attributes, data, and commands."""
        entity = SpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        # Set attribute
        entity.attributes.set_value(name="attr1", value="value1")
        self.assertFalse(entity.attributes.is_empty())
        self.assertEqual(entity.attributes.get_value("attr1"), "value1")
        # Set data metric
        entity.data.set_value(name="data1", value=123)
        self.assertFalse(entity.data.is_empty())
        self.assertEqual(entity.data.get_value("data1"), 123)
        # Set command
        entity.commands.set_value(name="cmd1", value=True)
        self.assertFalse(entity.commands.is_empty())
        self.assertEqual(entity.commands.get_value("cmd1"), True)

    def test_serialize_birth_payload_eon_node(self):
        """Test serialization of birth payload for EoN node."""
        entity = SpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        # Set some attributes and data
        entity.attributes.set_value(name="attr1", value="value1")
        entity.data.set_value(name="data1", value=123)
        entity.commands.set_value(name="cmd1", value=True)
        # Serialize birth payload
        payload_bytes = entity.serialize_payload_birth()
        self.assertIsInstance(payload_bytes, bytearray)
        # Parse the payload and check contents
        parser = SpbPayloadParser(payload_bytes)
        self.assertIsNotNone(parser.payload)
        metrics = parser.payload.get('metrics', [])
        # We expect 3 metrics: attr1, data1, cmd1, each with their prefixes
        metric_names = [metric['name'] for metric in metrics]
        self.assertIn('ATTR/attr1', metric_names)
        self.assertIn('DATA/data1', metric_names)
        self.assertIn('CMD/cmd1', metric_names)

    def test_serialize_birth_payload_eon_device(self):
        """Test serialization of birth payload for EoN device."""
        entity = SpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1", spb_eon_device_name="Device1")
        # Set some attributes and data
        entity.attributes.set_value(name="attr1", value="value1")
        entity.data.set_value(name="data1", value=123)
        entity.commands.set_value(name="cmd1", value=True)
        # Serialize birth payload
        payload_bytes = entity.serialize_payload_birth()
        self.assertIsInstance(payload_bytes, bytearray)
        # Parse the payload and check contents
        parser = SpbPayloadParser(payload_bytes)
        self.assertIsNotNone(parser.payload)
        metrics = parser.payload.get('metrics', [])
        # We expect 3 metrics: attr1, data1, cmd1, each with their prefixes
        metric_names = [metric['name'] for metric in metrics]
        self.assertIn('ATTR/attr1', metric_names)
        self.assertIn('DATA/data1', metric_names)
        self.assertIn('CMD/cmd1', metric_names)

    def test_deserialize_birth_payload(self):
        """Test deserialization of a birth payload."""
        entity = SpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        # Set some attributes and data
        entity.attributes.set_value(name="attr1", value="value1")
        entity.data.set_value(name="data1", value=123)
        entity.commands.set_value(name="cmd1", value=True)
        payload_bytes = entity.serialize_payload_birth()
        # Create a new entity and deserialize the payload
        new_entity = SpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        new_entity.deserialize_payload_birth(payload_bytes)
        # Verify that metrics are correctly deserialized
        self.assertEqual(new_entity.attributes.get_value("attr1"), "value1")
        self.assertEqual(new_entity.data.get_value("data1"), 123)
        self.assertEqual(new_entity.commands.get_value("cmd1"), True)

    def test_serialize_data_payload(self):
        """Test serialization of data payload."""
        entity = SpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        # Set data metrics
        entity.data.set_value(name="data1", value=123)
        entity.data.set_value(name="data2", value=456)
        # Serialize data payload
        payload_bytes = entity.serialize_payload_data()
        self.assertIsInstance(payload_bytes, bytearray)
        # Parse the payload and check contents
        parser = SpbPayloadParser(payload_bytes)
        self.assertIsNotNone(parser.payload)
        metrics = parser.payload.get('metrics', [])
        self.assertEqual(len(metrics), 2)
        metric_names = [metric['name'] for metric in metrics]
        self.assertIn('data1', metric_names)
        self.assertIn('data2', metric_names)

    def test_deserialize_data_payload(self):
        """Test deserialization of a data payload."""
        entity = SpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        # Set data metrics
        entity.data.set_value(name="data1", value=123)
        entity.data.set_value(name="data2", value=456)
        payload_bytes = entity.serialize_payload_data()
        # Create a new entity and deserialize the payload
        new_entity = SpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        new_entity.deserialize_payload_data(payload_bytes)
        # Verify that metrics are correctly deserialized
        self.assertEqual(new_entity.data.get_value("data1"), 123)
        self.assertEqual(new_entity.data.get_value("data2"), 456)

    def test_serialize_command_payload(self):
        """Test serialization of command payload."""
        entity = SpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        # Set command metrics
        entity.commands.set_value(name="cmd1", value=True)
        entity.commands.set_value(name="cmd2", value=False)
        # Serialize command payload
        payload_bytes = entity.serialize_payload_cmd()
        self.assertIsInstance(payload_bytes, bytearray)
        # Parse the payload and check contents
        parser = SpbPayloadParser(payload_bytes)
        self.assertIsNotNone(parser.payload)
        metrics = parser.payload.get('metrics', [])
        self.assertEqual(len(metrics), 2)
        metric_names = [metric['name'] for metric in metrics]
        self.assertIn('cmd1', metric_names)
        self.assertIn('cmd2', metric_names)

    def test_deserialize_command_payload(self):
        """Test deserialization of a command payload."""
        entity = SpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        # Set command metrics
        entity.commands.set_value(name="cmd1", value=True)
        entity.commands.set_value(name="cmd2", value=False)
        payload_bytes = entity.serialize_payload_cmd()
        # Create a new entity and deserialize the payload
        new_entity = SpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        new_entity.deserialize_payload_cmd(payload_bytes)
        # Verify that metrics are correctly deserialized
        self.assertEqual(new_entity.commands.get_value("cmd1"), True)
        self.assertEqual(new_entity.commands.get_value("cmd2"), False)

    def test_is_empty(self):
        """Test the is_empty method."""
        entity = SpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        self.assertTrue(entity.is_empty())
        entity.data.set_value(name="data1", value=123)
        self.assertFalse(entity.is_empty())

    def test_debug_properties(self):
        """Test setting and getting debug properties."""
        entity = SpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        self.assertFalse(entity.debug_enabled)
        entity.debug_enabled = True
        self.assertTrue(entity.debug_enabled)
        self.assertEqual(entity.debug_id, "SPB_ENTITY")
        entity.debug_id = "NEW_DEBUG_ID"
        self.assertEqual(entity.debug_id, "NEW_DEBUG_ID")

    def test_entity_properties(self):
        """Test the entity properties."""
        entity = SpbEntity(
            spb_domain_name="Group1",
            spb_eon_name="EoN1",
            spb_eon_device_name="Device1"
        )
        self.assertEqual(entity.spb_domain_name, "Group1")
        self.assertEqual(entity.spb_eon_name, "EoN1")
        self.assertEqual(entity.spb_eon_device_name, "Device1")
        self.assertEqual(entity.entity_name, "Device1")
        self.assertEqual(entity.entity_domain, "spBv1.Group1.EoN1.Device1")

    def test_serialization_with_different_data_types(self):
        """Test serialization with different data types."""
        entity = SpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        # Set data metrics with different data types
        entity.data.set_value(name="int_metric", value=123, spb_data_type=MetricDataType.Int32)
        entity.data.set_value(name="float_metric", value=123.456)
        entity.data.set_value(name="string_metric", value="test string")
        entity.data.set_value(name="bool_metric", value=True)
        entity.data.set_value(name="datetime_metric", value=datetime.now())
        entity.data.set_value(name="uuid_metric", value=uuid.uuid4())
        entity.data.set_value(name="bytes_metric", value=b'\x00\x01\x02')
        # Serialize data payload
        payload_bytes = entity.serialize_payload_data()
        self.assertIsInstance(payload_bytes, bytearray)
        # Parse the payload and check contents
        parser = SpbPayloadParser(payload_bytes)
        self.assertIsNotNone(parser.payload)
        metrics = parser.payload.get('metrics', [])
        metric_names = [metric['name'] for metric in metrics]
        expected_metrics = [
            "int_metric",
            "float_metric",
            "string_metric",
            "bool_metric",
            "datetime_metric",
            "uuid_metric",
            "bytes_metric"
        ]
        for name in expected_metrics:
            self.assertIn(name, metric_names)

    def test_deserialization_with_different_data_types(self):
        """Test deserialization with different data types."""
        entity = SpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        # Set data metrics with different data types
        entity.data.set_value(name="int_metric", value=123, spb_data_type=MetricDataType.Int32)
        entity.data.set_value(name="float_metric", value=123.456)
        entity.data.set_value(name="string_metric", value="test string")
        entity.data.set_value(name="bool_metric", value=True)
        entity.data.set_value(name="datetime_metric", value=datetime.now())
        test_uuid = uuid.uuid4()
        entity.data.set_value(name="uuid_metric", value=test_uuid)
        entity.data.set_value(name="bytes_metric", value=b'\x00\x01\x02')
        # Serialize data payload
        payload_bytes = entity.serialize_payload_data()
        # Create a new entity and deserialize the payload
        new_entity = SpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        new_entity.deserialize_payload_data(payload_bytes)
        # Verify that metrics are correctly deserialized
        self.assertEqual(new_entity.data.get_value("int_metric"), 123)
        self.assertAlmostEqual(new_entity.data.get_value("float_metric"), 123.456, places=3)
        self.assertEqual(new_entity.data.get_value("string_metric"), "test string")
        self.assertEqual(new_entity.data.get_value("bool_metric"), True)
        self.assertIsInstance(new_entity.data.get_value("datetime_metric"), datetime)
        self.assertEqual(new_entity.data.get_value("uuid_metric"), str(test_uuid))
        self.assertEqual(new_entity.data.get_value("bytes_metric"), b'\x00\x01\x02')

    def test_metrics_callbacks(self):
        """Test that callbacks are called when metric values change."""
        entity = SpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        callback_called = False

        def metric_callback(value):
            nonlocal callback_called
            callback_called = True
            self.assertEqual(value, 999)

        entity.data.set_value(name="data1", value=123, callback_on_change=metric_callback)
        entity.data.set_value(name="data1", value=999)
        self.assertTrue(callback_called)

    def test_serialize_empty_payload(self):
        """Test serialization when no metrics are set."""
        entity = SpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        payload_bytes = entity.serialize_payload_data()
        self.assertIsInstance(payload_bytes, bytearray)
        # self.assertEqual(len(payload_bytes), 0)  # Empty payload

    def test_deserialize_invalid_payload(self):
        """Test deserialization with invalid payload data."""
        entity = SpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        invalid_payload = b'invalid_payload_data'
        self.assertIsNone(entity.deserialize_payload_data(invalid_payload))

    def test_handle_metric_updates(self):
        """Test updating existing metrics and checking is_updated flag."""
        entity = SpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        entity.data.set_value(name="data1", value=123)
        self.assertTrue(entity.data.is_updated())
        _ = entity.data["data1"].value  # Access value to reset is_updated
        self.assertFalse(entity.data.is_updated())
        entity.data.set_value(name="data1", value=456)
        self.assertTrue(entity.data.is_updated())

    def test_metricgroup_str_repr(self):
        """Test __str__ and __repr__ methods of MetricGroup."""
        entity = SpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        entity.data.set_value(name="data1", value=123)
        data_str = str(entity.data)
        data_repr = repr(entity.data)
        self.assertIn("data1", data_str)
        self.assertIn("data1", data_repr)

    def test_entity_str_repr(self):
        """Test __str__ and __repr__ methods of SpbEntity."""
        entity = SpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        entity.data.set_value(name="data1", value=123)
        entity_str = str(entity)
        entity_repr = repr(entity)
        self.assertIn("spb_domain_name", entity_str)
        self.assertIn("data1", entity_str)
        self.assertIn("spb_domain_name", entity_repr)
        self.assertIn("data1", entity_repr)


if __name__ == '__main__':
    unittest.main(buffer=False)
