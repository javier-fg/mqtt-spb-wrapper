import unittest
import base64
import time
import uuid
from datetime import datetime

from mqtt_spb_wrapper.spb_base import SpbPayloadParser, Payload, MetricDataType
from mqtt_spb_wrapper.spb_protobuf.sparkplug_b import addMetric


class TestSpbPayloadParser(unittest.TestCase):

    def test_parse_simple_payload(self):
        """Test parsing a simple payload with a single metric."""
        # Create a simple payload with one metric
        payload_bytes = Payload()
        addMetric(payload_bytes, name="temperature", type=MetricDataType.Double, value=25.5, alias=None)
        payload_data = payload_bytes.SerializeToString()

        parser = SpbPayloadParser(payload_data)
        self.assertIsNotNone(parser.payload)
        self.assertIn('metrics', parser.payload)
        metrics = parser.payload['metrics']
        self.assertEqual(len(metrics), 1)
        self.assertEqual(metrics[0]['name'], 'temperature')
        self.assertEqual(metrics[0]['value'], 25.5)
        self.assertEqual(metrics[0]['datatype'], MetricDataType.Double)

    def test_parse_complex_payload(self):
        """Test parsing a complex payload with various data types."""
        # Example payload data provided by the user
        payload_bytes = bytearray(b'\x08\xe2\xfc\xf0\xdb\xaa2\x12\x12\n\x05value\x18\xe2\xfc\xf0\xdb\xaa2 \x04X\x00\x12\x13\n\x06uint16\x18\xe2\xfc\xf0\xdb\xaa2 \x06P}\x12\x17\n\x05bytes\x18\xe2\xfc\xf0\xdb\xaa2 \x11\x82\x01\x04\x01\x02\x03\x04\x12\x1a\n\x08datetime\x18\xe2\xfc\xf0\xdb\xaa2 \rX\xe2\xfc\xf0\xdb\xaa2\x12=\n\x04file\x18\xe2\xfc\xf0\xdb\xaa2 \x12\x82\x01+paho_mqtt==1.6.1\npandas==1.4.1\nPyYAML==6.0\n\x125\n\x04uuid\x18\xe2\xfc\xf0\xdb\xaa2 \x0fz$1d258807-9723-4480-99a4-f3bcae00e169\x18\x00')

        parser = SpbPayloadParser(payload_bytes)
        self.assertIsNotNone(parser.payload)
        self.assertIn('metrics', parser.payload)
        metrics = parser.payload['metrics']
        expected_metrics = [
            {
                'name': 'value',
                'value': 0,
                'timestamp': 1729453899362,
                'is_updated': False,
                'is_list_values': False,
                'spb_data_type': 4
            },
            {
                'name': 'uint16',
                'value': 125,
                'timestamp': 1729453899362,
                'is_updated': False,
                'is_list_values': False,
                'spb_data_type': 6
            },
            {
                'name': 'bytes',
                'value': b'\x01\x02\x03\x04',
                'timestamp': 1729453899362,
                'is_updated': False,
                'is_list_values': False,
                'spb_data_type': 17
            },
            {
                'name': 'datetime',
                'value': datetime(2024, 10, 20, 21, 51, 39, 362000),
                'timestamp': 1729453899362,
                'is_updated': False,
                'is_list_values': False,
                'spb_data_type': 13
            },
            {
                'name': 'file',
                'value': b'paho_mqtt==1.6.1\npandas==1.4.1\nPyYAML==6.0\n',
                'timestamp': 1729453899362,
                'is_updated': False,
                'is_list_values': False,
                'spb_data_type': 18
            },
            {
                'name': 'uuid',
                'value': '1d258807-9723-4480-99a4-f3bcae00e169',
                'timestamp': 1729453899362,
                'is_updated': False,
                'is_list_values': False,
                'spb_data_type': 15
            }
        ]

        # Create a mapping from metric name to metric data for easy comparison
        parsed_metrics = {metric['name']: metric for metric in metrics}
        expected_metrics_dict = {metric['name']: metric for metric in expected_metrics}

        for name, expected_metric in expected_metrics_dict.items():
            self.assertIn(name, parsed_metrics)
            parsed_metric = parsed_metrics[name]
            # Check each field
            self.assertEqual(parsed_metric.get('name'), expected_metric.get('name'))
            self.assertEqual(parsed_metric.get('value'), expected_metric.get('value'))
            self.assertEqual(int(parsed_metric.get('timestamp')), expected_metric.get('timestamp'))
            self.assertEqual(parsed_metric.get('datatype'), expected_metric.get('spb_data_type'))

    # def test_parse_payload_with_unknown_data(self):
    #     """Test parsing a payload with an unknown data type."""
    #     # Create a payload with an unknown data type
    #     payload_bytes = Payload()
    #     addMetric(payload_bytes, name="unknown_metric", type=MetricDataType.Unknown, value=None, alias=None)
    #     payload_data = payload_bytes.SerializeToString()
    #
    #     print(payload_data)
    #     parser = SpbPayloadParser(payload_data)
    #     print(parser.payload)
    #     self.assertIsNotNone(parser.payload)
    #     metrics = parser.payload['metrics']
    #     print(metrics)
    #     self.assertEqual(len(metrics), 1)
    #     self.assertEqual(metrics[0]['name'], 'unknown_metric')
    #     self.assertIsNone(metrics[0].get('value'))
    #     # self.assertEqual(metrics[0]['datatype'], MetricDataType.Unknown)

    def test_parse_empty_payload(self):
        """Test parsing an empty payload."""
        payload_bytes = Payload()
        payload_data = payload_bytes.SerializeToString()

        parser = SpbPayloadParser(payload_data)
        self.assertIsNotNone(parser.payload)
        self.assertNotIn('metrics', parser.payload)

    def test_parse_invalid_payload(self):
        """Test parsing an invalid payload."""
        invalid_data = b'invalid_payload_data'
        parser = SpbPayloadParser(invalid_data)
        self.assertIsNone(parser.payload)

    def test_parse_online_offline_payload(self):
        """Test parsing payloads with 'ONLINE' or 'OFFLINE' strings."""
        online_data = b'ONLINE'
        offline_data = b'OFFLINE'

        parser_online = SpbPayloadParser(online_data)
        self.assertEqual(parser_online.payload, 'ONLINE')

        parser_offline = SpbPayloadParser(offline_data)
        self.assertEqual(parser_offline.payload, 'OFFLINE')

    # def test_parse_payload_with_dataset(self):
    #     """Test parsing a payload containing a DataSet metric."""
    #     # Create a payload with a DataSet metric
    #     payload_bytes = Payload()
    #     dataset_value = {
    #         'columns': ['timestamps', 'values'],
    #         'types': [MetricDataType.Int64, MetricDataType.Double],
    #         'rows': [
    #             {'elements': [{'intValue': 1620000000000}, {'doubleValue': 1.1}]},
    #             {'elements': [{'intValue': 1620000001000}, {'doubleValue': 2.2}]},
    #             {'elements': [{'intValue': 1620000002000}, {'doubleValue': 3.3}]},
    #         ]
    #     }
    #     metric = payload_bytes.metrics.add()
    #     metric.name = 'dataset_metric'
    #     metric.datatype = MetricDataType.DataSet
    #     metric.datasetValue.CopyFrom(Payload.DataSet(**dataset_value))
    #     payload_data = payload_bytes.SerializeToString()
    #
    #     parser = SpbPayloadParser(payload_data)
    #     self.assertIsNotNone(parser.payload)
    #     metrics = parser.payload['metrics']
    #     self.assertEqual(len(metrics), 1)
    #     self.assertEqual(metrics[0]['name'], 'dataset_metric')
    #     self.assertEqual(metrics[0]['datatype'], MetricDataType.DataSet)
    #     self.assertIn('datasetValue', metrics[0])

    def test_parse_payload_with_metric_alias(self):
        """Test parsing a payload containing a metric with an alias."""
        payload_bytes = Payload()
        addMetric(payload_bytes, name="alias_metric", alias=100, type=MetricDataType.Double, value=42.0)
        payload_data = payload_bytes.SerializeToString()

        parser = SpbPayloadParser(payload_data)
        self.assertIsNotNone(parser.payload)
        metrics = parser.payload['metrics']
        self.assertEqual(len(metrics), 1)
        metric = metrics[0]
        self.assertEqual(metric['name'], 'alias_metric')
        self.assertEqual(metric['alias'], 100)
        self.assertEqual(metric['value'], 42.0)
        self.assertEqual(metric['datatype'], MetricDataType.Double)

    def test_parse_payload_with_complex_types(self):
        """Test parsing a payload with complex data types."""
        payload_bytes = Payload()
        # Add UUID metric
        addMetric(payload_bytes, name="uuid_metric", type=MetricDataType.UUID, value=str(uuid.uuid4()), alias=None)
        # Add DateTime metric
        addMetric(payload_bytes, name="datetime_metric", type=MetricDataType.DateTime, value=int(time.time() * 1000), alias=None)
        payload_data = payload_bytes.SerializeToString()

        parser = SpbPayloadParser(payload_data)
        self.assertIsNotNone(parser.payload)
        metrics = parser.payload['metrics']
        self.assertEqual(len(metrics), 2)

        metric_names = {metric['name'] for metric in metrics}
        self.assertIn('uuid_metric', metric_names)
        self.assertIn('datetime_metric', metric_names)

    def test_parse_payload_with_bytes(self):
        """Test parsing a payload containing a bytes metric."""
        payload_bytes = Payload()
        bytes_value = b'\x00\xFF\x7F'
        addMetric(payload_bytes, name="bytes_metric", type=MetricDataType.Bytes, value=bytes_value, alias=None)
        payload_data = payload_bytes.SerializeToString()

        parser = SpbPayloadParser(payload_data)
        self.assertIsNotNone(parser.payload)
        metrics = parser.payload['metrics']
        self.assertEqual(len(metrics), 1)
        metric = metrics[0]
        self.assertEqual(metric['name'], 'bytes_metric')
        decoded_value = base64.b64decode(metric['bytesValue'])
        self.assertEqual(decoded_value, bytes_value)
        self.assertEqual(metric['datatype'], MetricDataType.Bytes)

if __name__ == '__main__':
    unittest.main()
