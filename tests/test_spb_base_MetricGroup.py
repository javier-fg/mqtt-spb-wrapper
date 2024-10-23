import unittest
import time
from datetime import datetime
from io import BytesIO

from mqtt_spb_wrapper.spb_base import  MetricGroup, MetricValue, MetricDataType

class TestMetricGroup(unittest.TestCase):

    def test_initialization(self):
        """Test that MetricGroup initializes correctly."""
        mg = MetricGroup(birth_prefix="TEST")
        self.assertEqual(mg.birth_prefix, "TEST")
        self.assertTrue(mg.is_empty())
        self.assertEqual(mg.count(), 0)

    def test_set_value(self):
        """Test setting a single value."""
        mg = MetricGroup()
        result = mg.set_value(name="temperature", value=25.5)
        self.assertTrue(result)
        self.assertFalse(mg.is_empty())
        self.assertEqual(mg.count(), 1)
        self.assertIn("temperature", mg.get_names())
        self.assertEqual(mg.get_value("temperature"), 25.5)

    def test_set_multiple_values(self):
        """Test setting multiple values."""
        mg = MetricGroup()
        values = {"pressure": 101.3, "humidity": 60}
        mg.set_dictionary(values)
        self.assertEqual(mg.count(), 2)
        self.assertEqual(mg.get_value("pressure"), 101.3)
        self.assertEqual(mg.get_value("humidity"), 60)

    def test_update_value(self):
        """Test updating an existing value."""
        mg = MetricGroup()
        mg.set_value(name="level", value=5)
        mg.set_value(name="level", value=10)
        self.assertEqual(mg.get_value("level"), 10)

    def test_remove_value(self):
        """Test removing a value."""
        mg = MetricGroup()
        mg.set_value(name="status", value="OK")
        removed = mg.remove_value("status")
        self.assertTrue(removed)
        self.assertTrue(mg.is_empty())

    def test_remove_nonexistent_value(self):
        """Test removing a value that doesn't exist."""
        mg = MetricGroup()
        removed = mg.remove_value("nonexistent")
        self.assertFalse(removed)

    def test_is_updated(self):
        """Test the is_updated method."""
        mg = MetricGroup()
        mg.set_value(name="signal", value=1)
        self.assertTrue(mg.is_updated())
        _ = mg["signal"].value  # Access value to reset is_updated
        self.assertFalse(mg.is_updated())
        mg.set_value(name="signal", value=2)
        self.assertTrue(mg.is_updated())

    def test_clear(self):
        """Test clearing all metrics."""
        mg = MetricGroup()
        mg.set_value(name="metric1", value=100)
        mg.set_value(name="metric2", value=200)
        mg.clear()
        self.assertTrue(mg.is_empty())
        self.assertEqual(mg.count(), 0)

    def test_get_value_timestamp(self):
        """Test retrieving the timestamp of a metric."""
        mg = MetricGroup()
        timestamp = int(time.time() * 1000)
        mg.set_value(name="event", value="start", timestamp=timestamp)
        self.assertEqual(mg.get_value_timestamp("event"), timestamp)

    def test_is_list_values(self):
        """Test the is_list_values method for a metric."""
        mg = MetricGroup()
        values = [1, 2, 3]
        timestamps = [0, 0, 0]
        mg.set_value(name="readings", value=values, timestamp=timestamps)
        self.assertTrue(mg.is_list_values("readings"))
        mg.set_value(name="single_reading", value=42)
        self.assertFalse(mg.is_list_values("single_reading"))

    def test_dictionary_like_operations(self):
        """Test dictionary-like access to metrics."""
        mg = MetricGroup()
        mv = MetricValue(name="voltage", value=3.3)
        mg["voltage"] = mv
        self.assertIn("voltage", mg)
        self.assertEqual(mg["voltage"].value, 3.3)
        del mg["voltage"]
        self.assertNotIn("voltage", mg)

    def test_iteration(self):
        """Test iterating over the MetricGroup."""
        mg = MetricGroup()
        mg.set_value(name="metric1", value=10)
        mg.set_value(name="metric2", value=20)
        keys = [key for key in mg]
        self.assertListEqual(sorted(keys), ["metric1", "metric2"])

    def test_length(self):
        """Test the __len__ method."""
        mg = MetricGroup()
        self.assertEqual(len(mg), 0)
        mg.set_value(name="metric1", value=10)
        self.assertEqual(len(mg), 1)

    def test_get_dictionary(self):
        """Test getting the group's metrics as a dictionary."""
        mg = MetricGroup()
        mg.set_value(name="speed", value=88)
        mg.set_value(name="acceleration", value=9.8)
        metrics_dict = mg.get_dictionary()
        self.assertEqual(len(metrics_dict), 2)
        names = [metric['name'] for metric in metrics_dict]
        self.assertIn("speed", names)
        self.assertIn("acceleration", names)

    def test_set_value_with_callback(self):
        """Test setting a value with a callback function."""
        callback_called = False

        def callback(value):
            nonlocal callback_called
            callback_called = True
            self.assertEqual(value, 100)

        mg = MetricGroup()
        mg.set_value(name="threshold", value=50, callback_on_change=callback)
        mg.set_value(name="threshold", value=100)
        self.assertTrue(callback_called)

    def test_spb_alias_num(self):
        """Test setting and retrieving spb_alias_num."""
        mg = MetricGroup()
        mg.set_value(name="alias_metric", value=1.23, spb_alias_num=100)
        self.assertEqual(mg["alias_metric"].spb_alias_num, 100)

    def test_spb_data_type(self):
        """Test setting and retrieving spb_data_type."""
        mg = MetricGroup()
        mg.set_value(name="text_metric", value="hello", spb_data_type=MetricDataType.Text)
        self.assertEqual(mg["text_metric"].spb_data_type, MetricDataType.Text)

    def test_set_value_with_none(self):
        """Test that setting a value to None is ignored."""
        mg = MetricGroup()
        result = mg.set_value(name="null_metric", value=None)
        self.assertFalse(result)
        self.assertTrue(mg.is_empty())

    def test_set_dictionary_with_timestamps(self):
        """Test setting multiple values with a specific timestamp."""
        mg = MetricGroup()
        timestamp = int(time.time() * 1000)
        values = {"temp": 22.5, "humidity": 55}
        mg.set_dictionary(values, timestamp=timestamp)
        self.assertEqual(mg.get_value_timestamp("temp"), timestamp)
        self.assertEqual(mg.get_value_timestamp("humidity"), timestamp)

    def test_has_callbacks(self):
        """Test checking if any metrics have callbacks."""
        mg = MetricGroup()
        mg.set_value(name="metric1", value=1)
        self.assertFalse(mg["metric1"].has_callback())
        mg.set_value(name="metric2", value=2, callback_on_change=lambda x: x)
        self.assertTrue(mg["metric2"].has_callback())

    def test_is_list_values_nonexistent(self):
        """Test is_list_values method for a nonexistent metric."""
        mg = MetricGroup()
        self.assertTrue(mg.is_list_values("nonexistent"))

    def test_timestamps_mismatch(self):
        """Test handling of mismatched value and timestamp lists."""
        mg = MetricGroup()
        values = [1, 2, 3]
        timestamps = [0, 0, 0]
        mg.set_value(name="mismatch_metric", value=values, timestamp=timestamps)
        self.assertEqual(mg["mismatch_metric"].value, values)
        self.assertEqual(mg["mismatch_metric"].timestamp, timestamps)

    def test_metric_value_access(self):
        """Test accessing MetricValue instances directly."""
        mg = MetricGroup()
        mg.set_value(name="direct_metric", value=123)
        metric_value = mg["direct_metric"]
        self.assertIsInstance(metric_value, MetricValue)
        self.assertEqual(metric_value.value, 123)

    def test_metricgroup_str_repr(self):
        """Test the __str__ and __repr__ methods."""
        mg = MetricGroup()
        mg.set_value(name="test_metric", value=42)
        mg_str = str(mg)
        mg_repr = repr(mg)
        self.assertIn("test_metric", mg_str)
        self.assertIn("test_metric", mg_repr)

if __name__ == '__main__':
    unittest.main()
