import unittest
from unittest.mock import MagicMock, patch, call
import time
from datetime import datetime
import uuid

from mqtt_spb_wrapper import *


class TestMqttSpbEntity(unittest.TestCase):

    def setUp(self):
        # Patch the mqtt.Client class
        patcher = patch('mqtt_spb_wrapper.mqtt_spb_entity.mqtt.Client')
        self.addCleanup(patcher.stop)
        self.mock_mqtt_client_class = patcher.start()
        self.mock_mqtt_client = MagicMock()
        self.mock_mqtt_client_class.return_value = self.mock_mqtt_client

    def test_initialization(self):
        """Test initialization of MqttSpbEntity."""
        entity = MqttSpbEntity(
            spb_domain_name="Group1",
            spb_eon_name="EoN1",
            spb_eon_device_name="Device1",
            retain_birth=True,
            debug_enabled=True,
            debug_id="TEST_ENTITY",
            entity_is_scada=False
        )
        self.assertEqual(entity.spb_domain_name, "Group1")
        self.assertEqual(entity.spb_eon_name, "EoN1")
        self.assertEqual(entity.spb_eon_device_name, "Device1")
        self.assertTrue(entity._retain_birth)
        self.assertTrue(entity.debug_enabled)
        self.assertEqual(entity.debug_id, "TEST_ENTITY")
        self.assertFalse(entity._entity_is_scada)

    def test_connect(self):
        """Test connecting to the MQTT broker."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        # Mock the connect method to return successfully
        self.mock_mqtt_client.connect.return_value = 0
        # Mock is_connected to return True
        self.mock_mqtt_client.is_connected.return_value = True

        connected = entity.connect(host='test.mqtt.broker', port=1883, user='user', password='pass')
        self.assertTrue(connected)
        self.mock_mqtt_client.connect.assert_called_with('test.mqtt.broker', 1883)
        self.mock_mqtt_client.username_pw_set.assert_called_with('user', 'pass')
        self.mock_mqtt_client.loop_start.assert_called()

    def test_disconnect(self):
        """Test disconnecting from the MQTT broker."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        entity._mqtt = self.mock_mqtt_client  # Set the mock client
        entity.disconnect()
        self.mock_mqtt_client.loop_stop.assert_called()
        self.mock_mqtt_client.disconnect.assert_called()
        self.assertIsNone(entity._mqtt)

    def test_publish_birth(self):
        """Test publishing a birth message."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        entity._mqtt = self.mock_mqtt_client  # Set the mock client
        entity._mqtt.is_connected.return_value = True

        # Set some data to ensure it's not empty
        entity.data.set_value(name="temperature", value=25.5)

        entity.publish_birth()
        self.mock_mqtt_client.publish.assert_called()
        self.assertTrue(entity.is_birth_published)

    def test_publish_birth_not_connected(self):
        """Test publishing a birth message when not connected."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        entity._mqtt = self.mock_mqtt_client  # Set the mock client
        entity._mqtt.is_connected.return_value = False

        entity.publish_birth()
        self.mock_mqtt_client.publish.assert_not_called()
        self.assertFalse(entity.is_birth_published)

    def test_publish_data(self):
        """Test publishing data."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        entity._mqtt = self.mock_mqtt_client
        entity._mqtt.is_connected.return_value = True

        # Set some data
        entity.data.set_value(name="temperature", value=25.5)
        entity.publish_data()
        self.mock_mqtt_client.publish.assert_called()
        # Ensure that data is marked as not updated after publishing
        self.assertFalse(entity.data.is_updated())

    def test_publish_data_no_updates(self):
        """Test publishing data when there are no updates."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        entity._mqtt = self.mock_mqtt_client
        entity._mqtt.is_connected.return_value = True

        # Set some data but reset is_updated
        entity.data.set_value(name="temperature", value=25.5)
        _ = entity.data["temperature"].value  # Access to reset is_updated
        entity.publish_data()
        self.mock_mqtt_client.publish.assert_not_called()

    def test_mqtt_on_connect(self):
        """Test the MQTT on_connect callback."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        mock_client = MagicMock()
        entity._mqtt_on_connect(mock_client, None, None, 0)
        # Check that it subscribed to the correct topics
        expected_calls = [
            call('spBv1.0/Group1/NCMD/EoN1'),
            call('spBv1.0/Group1/STATE/+'),
        ]
        mock_client.subscribe.assert_has_calls(expected_calls, any_order=True)

    def test_mqtt_on_message(self):
        """Test the MQTT on_message callback."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        entity._loopback_topic = 'some/other/topic'  # Ensure it's not the same
        mock_msg = MagicMock()
        mock_msg.topic = 'spBv1.0/Group1/NCMD/EoN1'
        mock_msg.payload = b'\x00\x01\x02'  # Some payload

        # Mock on_message callback
        entity.on_message = MagicMock()

        with patch('mqtt_spb_wrapper.mqtt_spb_entity.SpbPayloadParser') as mock_parser_class:
            mock_parser = MagicMock()
            mock_parser.parse_payload.return_value = {'metrics': [], 'timestamp_rx': int(time.time() * 1000)}
            mock_parser_class.return_value = mock_parser

            entity._mqtt_on_message(None, None, mock_msg)
            entity.on_message.assert_called()

    def test_on_command_callback(self):
        """Test that the on_command callback is called when a command is received."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        entity._loopback_topic = 'some/other/topic'
        mock_msg = MagicMock()
        mock_msg.topic = 'spBv1.0/Group1/NCMD/EoN1'
        mock_msg.payload = b'\x00\x01\x02'  # Some payload

        # Set up a command metric
        entity.commands.set_value(name="cmd1", value=False)

        # Mock on_command callback
        entity.on_command = MagicMock()

        # Mock SpbPayloadParser
        with patch('mqtt_spb_wrapper.mqtt_spb_entity.SpbPayloadParser') as mock_parser_class:
            mock_parser = MagicMock()
            mock_parser.parse_payload.return_value = {
                'metrics': [{'name': 'cmd1', 'value': True}],
                'timestamp_rx': int(time.time() * 1000)
            }
            mock_parser_class.return_value = mock_parser

            entity._mqtt_on_message(None, None, mock_msg)
            entity.on_command.assert_called()
            # Verify that the command value was updated
            self.assertEqual(entity.commands.get_value("cmd1"), True)

    def test_is_connected(self):
        """Test the is_connected method."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        self.assertFalse(entity.is_connected())
        entity._mqtt = self.mock_mqtt_client
        entity._mqtt.is_connected.return_value = True
        self.assertTrue(entity.is_connected())

    def test_publish_death_on_disconnect(self):
        """Test that a death message is published on disconnect."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        entity._mqtt = self.mock_mqtt_client

        entity.disconnect(skip_death_publish=False)
        self.mock_mqtt_client.publish.assert_called()
        self.mock_mqtt_client.loop_stop.assert_called()
        self.mock_mqtt_client.disconnect.assert_called()

    def test_skip_publish_death_on_disconnect(self):
        """Test that no death message is published when skip_death_publish is True."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        entity._mqtt = self.mock_mqtt_client

        entity.disconnect(skip_death_publish=True)
        self.mock_mqtt_client.publish.assert_not_called()
        self.mock_mqtt_client.loop_stop.assert_called()
        self.mock_mqtt_client.disconnect.assert_called()

    def test_handle_loopback_message(self):
        """Test that loopback messages are ignored."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        entity._loopback_topic = 'test/topic'
        mock_msg = MagicMock()
        mock_msg.topic = 'test/topic'
        entity.on_message = MagicMock()

        entity._mqtt_on_message(None, None, mock_msg)
        entity.on_message.assert_not_called()

    def test_publish_birth_scada(self):
        """Test publishing a birth message when entity is SCADA."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="SCADA", entity_is_scada=True)
        entity._mqtt = self.mock_mqtt_client
        entity._mqtt.is_connected.return_value = True

        entity.publish_birth()
        self.mock_mqtt_client.publish.assert_called_with(
            'spBv1.0/Group1/STATE/SCADA', b'ONLINE', 0, True
        )
        self.assertTrue(entity.is_birth_published)

    def test_mqtt_on_message_state(self):
        """Test handling of STATE messages."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        mock_msg = MagicMock()
        mock_msg.topic = 'spBv1.0/Group1/STATE/SCADA'
        mock_msg.payload = b'ONLINE'

        # Mock on_message callback
        entity.on_message = MagicMock()

        entity._mqtt_on_message(None, None, mock_msg)

        # Ensure on_message was called once
        entity.on_message.assert_called_once()

        # Get the arguments with which on_message was called
        args, kwargs = entity.on_message.call_args

        # args should be (topic, payload)
        self.assertEqual(len(args), 2)
        topic_arg, payload_arg = args

        # Check that topic_arg is an instance of SpbTopic and has the expected topic string
        self.assertIsInstance(topic_arg, SpbTopic)
        self.assertEqual(topic_arg.topic, mock_msg.topic)

        # Check that payload_arg is 'ONLINE'
        self.assertEqual(payload_arg, 'ONLINE')

    def test_connect_already_connected(self):
        """Test that connect does nothing if already connected."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        entity._mqtt = self.mock_mqtt_client
        entity._mqtt.is_connected.return_value = True

        result = entity.connect()
        self.assertTrue(result)
        self.mock_mqtt_client.connect.assert_not_called()

    def test_disconnect_not_connected(self):
        """Test that disconnect does nothing if not connected."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        entity._mqtt = None
        entity.disconnect()
        # No exception should occur

    def test_publish_data_not_connected(self):
        """Test publishing data when not connected."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        entity._mqtt = self.mock_mqtt_client
        entity._mqtt.is_connected.return_value = False

        result = entity.publish_data()
        self.assertFalse(result)
        self.mock_mqtt_client.publish.assert_not_called()

    def test_publish_data_no_data(self):
        """Test publishing data when entity is empty."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        entity._mqtt = self.mock_mqtt_client
        entity._mqtt.is_connected.return_value = True

        result = entity.publish_data()
        self.assertFalse(result)
        self.mock_mqtt_client.publish.assert_not_called()

    def test_publish_birth_no_data(self):
        """Test publishing birth when entity is empty."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        entity._mqtt = self.mock_mqtt_client
        entity._mqtt.is_connected.return_value = True

        entity.publish_birth()
        self.mock_mqtt_client.publish.assert_not_called()
        self.assertFalse(entity.is_birth_published)

    def test_handle_incoming_cmd_with_invalid_metric(self):
        """Test handling a CMD with an invalid metric."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        entity._loopback_topic = 'some/other/topic'
        mock_msg = MagicMock()
        mock_msg.topic = 'spBv1.0/Group1/NCMD/EoN1'
        mock_msg.payload = b'\x00\x01\x02'

        # Set up a command metric
        entity.commands.set_value(name="cmd1", value=False)

        # Mock SpbPayloadParser
        with patch('mqtt_spb_wrapper.mqtt_spb_entity.SpbPayloadParser') as mock_parser_class:
            mock_parser = MagicMock()
            mock_parser.parse_payload.return_value = {
                'metrics': [{'name': 'invalid_cmd', 'value': True}],
                'timestamp_rx': int(time.time() * 1000)
            }
            mock_parser_class.return_value = mock_parser

            entity._mqtt_on_message(None, None, mock_msg)
            # Ensure the command value was not updated
            self.assertEqual(entity.commands.get_value("cmd1"), False)

    def test_handle_incoming_cmd_with_invalid_datatype(self):
        """Test handling a CMD with an invalid datatype."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        entity._loopback_topic = 'some/other/topic'
        mock_msg = MagicMock()
        mock_msg.topic = 'spBv1.0/Group1/NCMD/EoN1'
        mock_msg.payload = b'\x00\x01\x02'

        # Set up a command metric with a boolean value
        entity.commands.set_value(name="cmd1", value=False)

        # Mock SpbPayloadParser
        with patch('mqtt_spb_wrapper.mqtt_spb_entity.SpbPayloadParser') as mock_parser_class:
            mock_parser = MagicMock()
            # Provide an integer value instead of boolean
            mock_parser.parse_payload.return_value = {
                'metrics': [{'name': 'cmd1', 'value': 123}],
                'timestamp_rx': int(time.time() * 1000)
            }
            mock_parser_class.return_value = mock_parser

            entity._mqtt_on_message(None, None, mock_msg)
            # Ensure the command value was not updated
            self.assertEqual(entity.commands.get_value("cmd1"), False)

    def test_tls_configuration(self):
        """Test that TLS configuration is set correctly."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")

        # Mock the connect method to return successfully
        self.mock_mqtt_client.connect.return_value = 0
        # Mock is_connected to return True
        self.mock_mqtt_client.is_connected.return_value = True

        # Call connect with TLS parameters
        entity.connect(
            host='test.mqtt.broker',
            port=8883,
            use_tls=True,
            tls_ca_path='ca.crt',
            tls_cert_path='client.crt',
            tls_key_path='client.key',
            tls_insecure=False
        )

        # Verify that tls_set was called with correct arguments
        self.mock_mqtt_client.tls_set.assert_called_with(
            ca_certs='ca.crt',
            certfile='client.crt',
            keyfile='client.key'
        )

    def test_tls_insecure_configuration(self):
        """Test that TLS insecure configuration is set correctly."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")

        # Mock the connect method to return successfully
        self.mock_mqtt_client.connect.return_value = 0
        # Mock is_connected to return True
        self.mock_mqtt_client.is_connected.return_value = True

        # Call connect with TLS insecure parameters
        entity.connect(
            host='test.mqtt.broker',
            port=8883,
            use_tls=True,
            tls_ca_path='ca.crt',
            tls_cert_path='client.crt',
            tls_key_path='client.key',
            tls_insecure=True
        )

        # Verify that tls_set was called with correct arguments
        self.mock_mqtt_client.tls_set.assert_called()
        self.mock_mqtt_client.tls_insecure_set.assert_called_with(True)

    def test_set_on_connect_callback(self):
        """Test setting the on_connect callback."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        callback = MagicMock()
        entity.on_connect = callback

        # Simulate on_connect with a mock client
        mock_client = MagicMock()
        entity._mqtt_on_connect(mock_client, None, None, 0)

        # Verify that the callback was called with rc = 0
        callback.assert_called_with(0)

    def test_set_on_disconnect_callback(self):
        """Test setting the on_disconnect callback."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        callback = MagicMock()
        entity.on_disconnect = callback

        # Simulate on_disconnect
        entity._mqtt_on_disconnect(None, None, 0)
        callback.assert_called_with(0)

    def test_set_on_message_callback(self):
        """Test setting the on_message callback."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        callback = MagicMock()
        entity.on_message = callback

        # Simulate on_message
        mock_msg = MagicMock()
        mock_msg.topic = 'spBv1.0/Group1/STATE/SCADA'
        mock_msg.payload = b'ONLINE'

        entity._mqtt_on_message(None, None, mock_msg)

        # Ensure the callback was called once
        callback.assert_called_once()

        # Retrieve the arguments with which the callback was called
        args, kwargs = callback.call_args

        # Unpack the arguments
        topic_arg, payload_arg = args

        # Assert that the topic argument is an SpbTopic instance with the correct topic
        self.assertIsInstance(topic_arg, SpbTopic)
        self.assertEqual(topic_arg.topic, mock_msg.topic)

        # Assert that the payload argument is 'ONLINE'
        self.assertEqual(payload_arg, 'ONLINE')

    def test_entity_str_repr(self):
        """Test __str__ and __repr__ methods of MqttSpbEntity."""
        entity = MqttSpbEntity(spb_domain_name="Group1", spb_eon_name="EoN1")
        entity_str = str(entity)
        entity_repr = repr(entity)
        self.assertIn('spb_domain_name', entity_str)
        self.assertIn('spb_domain_name', entity_repr)


if __name__ == '__main__':
    unittest.main(buffer=False)
