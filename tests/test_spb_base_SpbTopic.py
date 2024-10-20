import unittest
from mqtt_spb_wrapper import SpbTopic  # Assuming SpbTopic is located in spb_base.py


class TestSpbTopic(unittest.TestCase):

    def setUp(self):
        # Setting up common example topic strings
        self.valid_topic_str = "spBv1.0/Group-001/DBIRTH/Gateway-001/SimpleEoND-01"
        self.incomplete_topic_str = "spBv1.0/Group-001/DBIRTH"
        self.malformed_topic_str = "spBv1.0_Group-001/DBIRTH_Gateway-001/SimpleEoND-01"

    def test_valid_topic_initialization(self):
        """ Test initialization with a valid topic string """
        topic = SpbTopic(self.valid_topic_str)
        self.assertEqual(topic.namespace, "spBv1.0")
        self.assertEqual(topic.domain_name, "Group-001")
        self.assertEqual(topic.message_type, "DBIRTH")
        self.assertEqual(topic.eon_name, "Gateway-001")
        self.assertEqual(topic.eon_device_name, "SimpleEoND-01")

    def test_incomplete_topic_initialization(self):
        """ Test initialization with an incomplete topic string """
        topic = SpbTopic(self.incomplete_topic_str)
        self.assertEqual(topic.namespace, "spBv1.0")
        self.assertEqual(topic.domain_name, "Group-001")
        self.assertEqual(topic.message_type, "DBIRTH")
        self.assertIsNone(topic.eon_name)  # Expecting None for missing components
        self.assertIsNone(topic.eon_device_name)

    def test_malformed_topic_initialization(self):
        """ Test initialization with a malformed topic string """
        with self.assertRaises(ValueError):
            SpbTopic(self.malformed_topic_str)

    def test_to_string_method(self):
        """ Test to_string method to ensure proper reconstruction of topic """
        topic = SpbTopic(self.valid_topic_str)
        expected_output = "spBv1.0/Group-001/DBIRTH/Gateway-001/SimpleEoND-01"
        self.assertEqual(topic.to_string(), expected_output)

    def test_partial_topic_to_string(self):
        """ Test to_string method with an incomplete topic """
        topic = SpbTopic(self.incomplete_topic_str)
        expected_output = "spBv1.0/Group-001/DBIRTH"
        self.assertEqual(topic.to_string(), expected_output)

    def test_eon_device_name_absent(self):
        """ Test when eon_device_name is absent, the to_string should handle it properly """
        topic_str = "spBv1.0/Group-001/DBIRTH/Gateway-001"
        topic = SpbTopic(topic_str)
        self.assertEqual(topic.to_string(), topic_str)
        self.assertIsNone(topic.eon_device_name)

    def test_invalid_topic_raises_error(self):
        """ Test that invalid or malformed topics raise a ValueError """
        invalid_topic_str = "invalid_topic_string"
        with self.assertRaises(ValueError):
            SpbTopic(invalid_topic_str)

    def test_message_type_extraction(self):
        """ Test that message type is properly extracted from a valid topic string """
        topic = SpbTopic(self.valid_topic_str)
        self.assertEqual(topic.message_type, "DBIRTH")

    def test_get_group_name(self):
        """ Test retrieval of group name """
        topic = SpbTopic(self.valid_topic_str)
        self.assertEqual(topic.domain_name, "Group-001")

    def test_repr_method(self):
        """ Test __repr__ method """
        topic = SpbTopic(self.valid_topic_str)
        self.assertEqual(repr(topic), self.valid_topic_str)


    def test_entity_name_with_eon_and_eond(self):
        """ Test entity_name when both eon_name and eon_device_name are present """
        topic_str = "spBv1.0/Group-001/DBIRTH/Gateway-001/Device-001"
        topic = SpbTopic(topic_str)

        # Check that the entity_name is set to eon_device_name when both are present
        self.assertEqual(topic.eon_name, "Gateway-001")
        self.assertEqual(topic.eon_device_name, "Device-001")
        self.assertEqual(topic.entity_name, "Device-001")  # entity_name should be the eon_device_name


    def test_entity_name_with_only_eon(self):
        """ Test entity_name when only eon_name is present """
        topic_str = "spBv1.0/Group-001/DBIRTH/Gateway-001"
        topic = SpbTopic(topic_str)

        # Check that the entity_name is set to eon_name when no eon_device_name is present
        self.assertEqual(topic.eon_name, "Gateway-001")
        self.assertIsNone(topic.eon_device_name)
        self.assertEqual(topic.entity_name, "Gateway-001")  # entity_name should be the eon_name

    def test_entity_name_with_no_eon_or_eond(self):
        """ Test entity_name when neither eon_name nor eon_device_name is present """
        topic_str = "spBv1.0/Group-001/DBIRTH"
        topic = SpbTopic(topic_str)

        # Check that entity_name is None when both eon_name and eon_device_name are absent
        self.assertIsNone(topic.eon_name)
        self.assertIsNone(topic.eon_device_name)
        self.assertIsNone(topic.entity_name)  # entity_name should be None in this case

if __name__ == '__main__':
    unittest.main()
