import unittest
import time
from datetime import datetime, timedelta
import uuid
from io import BytesIO

from mqtt_spb_wrapper.spb_base import MetricValue, MetricDataType


class TestMetricValue(unittest.TestCase):

    def test_initialization_default(self):
        """Test default initialization."""
        mv = MetricValue(name="temperature", value=25.5)
        self.assertEqual(mv.name, "temperature")
        self.assertEqual(mv.value, 25.5)
        self.assertIsNotNone(mv.timestamp)
        self.assertFalse(mv.is_list_values())
        self.assertEqual(mv.spb_data_type, MetricDataType.Double)

    def test_initialization_with_timestamp(self):
        """Test initialization with a specific timestamp."""
        timestamp = int(time.time() * 1000)
        mv = MetricValue(name="pressure", value=101.3, timestamp=timestamp)
        self.assertEqual(mv.timestamp, timestamp)

    def test_initialization_with_list(self):
        """Test initialization with list values and timestamps."""
        values = [1, 2, 3]
        timestamps = [int(time.time() * 1000) + i * 1000 for i in range(3)]
        mv = MetricValue(name="readings", value=values, timestamp=timestamps)
        self.assertEqual(mv.value, values)
        self.assertEqual(mv.timestamp, timestamps)
        self.assertTrue(mv.is_list_values())

    def test_value_setter(self):
        """Test setting a new value."""
        mv = MetricValue(name="humidity", value=60)
        test = mv.value  # read will force is_updated to false
        self.assertFalse(mv.is_updated)
        mv.value = 65
        self.assertTrue(mv.is_updated)
        self.assertEqual(mv.value, 65)


    def test_timestamp_setter(self):
        """Test setting a new timestamp."""
        mv = MetricValue(name="humidity", value=60)
        new_timestamp = int(time.time() * 1000) + 5000
        mv.timestamp = new_timestamp
        self.assertEqual(mv.timestamp, new_timestamp)

    def test_timestamp_update(self):
        """Test updating the timestamp to current time."""
        mv = MetricValue(name="sensor", value=50)
        old_timestamp = mv.timestamp
        time.sleep(0.01)
        mv.timestamp_update()
        self.assertNotEqual(mv.timestamp, old_timestamp)
        self.assertGreater(mv.timestamp, old_timestamp)

    def test_callback_on_change(self):
        """Test that the callback is called when value changes."""
        callback_called = False

        def callback(value):
            nonlocal callback_called
            callback_called = True
            self.assertEqual(value, False)

        mv = MetricValue(name="switch", value=True, callback_on_change=callback)
        mv.value = False
        self.assertTrue(callback_called)

    def test_spb_data_type_inference(self):
        """Test automatic data type inference."""
        mv_str = MetricValue(name="status", value="OK")
        mv_bool = MetricValue(name="enabled", value=True)
        mv_int = MetricValue(name="count", value=10)
        mv_float = MetricValue(name="weight", value=70.5)
        mv_bytes = MetricValue(name="data", value=b'\x00\x01')
        mv_datetime = MetricValue(name="timestamp", value=datetime.now())
        mv_uuid = MetricValue(name="id", value=uuid.uuid4())

        self.assertEqual(mv_str.spb_data_type, MetricDataType.Text)
        self.assertEqual(mv_bool.spb_data_type, MetricDataType.Boolean)
        self.assertEqual(mv_int.spb_data_type, MetricDataType.Int64)
        self.assertEqual(mv_float.spb_data_type, MetricDataType.Double)
        self.assertEqual(mv_bytes.spb_data_type, MetricDataType.Bytes)
        self.assertEqual(mv_datetime.spb_data_type, MetricDataType.DateTime)
        self.assertEqual(mv_uuid.spb_data_type, MetricDataType.UUID)

    def test_spb_data_type_override(self):
        """Test overriding the automatic data type."""
        mv = MetricValue(name="custom", value="123", spb_data_type=MetricDataType.Int64)
        self.assertEqual(mv.spb_data_type, MetricDataType.Int64)
        self.assertEqual(mv.value, "123")

    def test_is_updated_flag(self):
        """Test the is_updated flag behavior."""
        mv = MetricValue(name="signal", value=1)
        self.assertTrue(mv.is_updated)
        _ = mv.value  # Accessing value should reset is_updated
        self.assertFalse(mv.is_updated)
        mv.value = 2
        self.assertTrue(mv.is_updated)

    def test_as_dict(self):
        """Test the as_dict method."""
        mv = MetricValue(name="test_metric", value=42)
        expected_dict = {
            "name": "test_metric",
            "value": 42,
            "timestamp": mv.timestamp,
            "spb_data_type": mv.spb_data_type,
            "is_updated": True,
            "is_list_values": False,
            'has_callback': False
        }
        self.assertEqual(mv.as_dict(), expected_dict)

    def test_set_method(self):
        """Test the set method to update value and timestamp."""
        mv = MetricValue(name="level", value=5)
        new_value = 10
        new_timestamp = int(time.time() * 1000) + 1000
        mv.set(new_value, new_timestamp)
        self.assertEqual(mv.value, new_value)
        self.assertEqual(mv.timestamp, new_timestamp)

    def test_file_type_value(self):
        """Test handling of file-like objects."""
        file_content = b"Sample data"
        file_obj = BytesIO(file_content)
        mv = MetricValue(name="file_metric", value=file_obj, spb_data_type=MetricDataType.File)
        self.assertEqual(mv.spb_data_type, MetricDataType.File)
        self.assertEqual(mv.value.getvalue(), file_content)

    def test_invalid_value_type(self):
        """Test handling of unsupported value types."""
        class UnsupportedType:
            pass

        with self.assertRaises(ValueError):
            MetricValue(name="unsupported", value=UnsupportedType())

    def test_has_callback(self):
        """Test the has_callback method."""
        mv = MetricValue(name="test", value=1)
        self.assertFalse(mv.has_callback())
        mv.callback = lambda x: x
        self.assertTrue(mv.has_callback())

    def test_metric_with_alias(self):
        """Test setting and getting the alias number."""
        mv = MetricValue(name="alias_metric", value=1.23, spb_alias_num=100)
        self.assertEqual(mv.spb_alias_num, 100)

if __name__ == '__main__':
    unittest.main()
