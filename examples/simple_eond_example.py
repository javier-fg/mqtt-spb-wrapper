import time
from datetime import datetime
import uuid

from mqtt_spb_wrapper import MetricDataType, MqttSpbEntityDevice

_DEBUG = True  # Enable debug messages

# MQTT Broker parameters
_MQTT_HOST = "localhost"
_MQTT_PORT = 1883
_MQTT_USER = ""
_MQTT_PASSW = ""

# Sparkplug B parameters - Create the spB entity object
domain_name = "TestDomain"
edge_node_name = "Gateway-001"
device_name = "SimpleEoND-01"

print("--- Sparkplug B example - End of Node Device - Simple")


def callback_command(payload):
    print("DEVICE received CMD: %s" % str(payload))


def callback_cmd_test(data_value):
    print("   CMD test received - value: " + str(data_value))


# Create a new SpB EoND Entity
device = MqttSpbEntityDevice(domain_name, edge_node_name, device_name, _DEBUG)

device.on_command = callback_command  # Callback for received device commands


# Set the device Attributes, Data and Commands that will be sent on the DBIRTH message --------------------------------

# --- Attributes   ----------------------------------------------------------------------------------------------------
device.attributes.set_value("description", "Simple EoN Device node")
device.attributes.set_value("type", "Simulated-EoND-device")
device.attributes.set_value("version", "0.01")

# --- Data / Telemetry  -----------------------------------------------------------------------------------------------
# Set a metric value. If no timestamp is provided, the system UTC epoch in ms will be automatically used.
device.data.set_value(
    name="value",
    value=0
)

# You can set a list of values+timestamps to be sent as spB Dataset Metric type.
# IMPORTANT: You must provide the same list size for the values and timestamps, otherwise a single
#            point will be sent ( first element )
# You can check if a value has multiple values by checking its device.data.is_list_values("values")
device.data.set_value(
    name="values",
    value=[12, 34, 45],
    timestamp=[1728973247000, 1728973248000, 1728973249000]
)

# You can also enforce a certain spB metric type.
# NOTE: if no spB metric type is provided, the type will be inferred from the python data type:
#       bool, numeric = float, string, bytes
device.data.set_value(
    name="uint16",
    value=125,
    spb_data_type=MetricDataType.UInt16,
)

# BYTES - a list of bytes or bytearray can be sent
device.data.set_value(
    name="bytes",
    value=bytes([1, 2, 3, 4]),
    # value=bytearray([1, 2, 3, 4])   # It is possible to set an bytearray
)

# DATETIME - object type - value is converted to EPOC timestamp in milliseconds
device.data.set_value(
    name="datetime",
    value=datetime.now()
)

# FILE - Can be sent from the open() object or you can send it as bytes or as a file
device.data.set_value(
    name="file",
    value=open("requirements.txt", "r")
    # Note: it can also be sent as bytes or bytearray
    # value=file_bytes,
    # spb_data_type = MetricDataType.File
)

# UUID - Universal Unique ID
device.data.set_value(
    name="uuid",
    value=uuid.uuid4()
    # Note: it can also be sent as bytes or bytearray
    # value='2a7e4d22-795a-44b8-96f8-cc74e24df6fe',
    # spb_data_type = MetricDataType.UUID
)

# DATASET - Table of values ( columns + rows ) provided as dictionary in python.
# IMPORTANT: dictionary elements should be provided like colum_name: list(values), and
#            all column values should be the same size
device.data.set_value(
    name="dataset",
    value={
        "Temperature": [23.5, 22.0, 21.8],
        "Humidity": [60.2, 58.9, 59.5],
        "Status": ["Normal", "Warning", "Alert"]
    }
)

# --- Commands  --------------------------------------------------------------------------------------------------------
device.commands.set_value(name="rebirth",
                          value=False
                          )

# You can set a callback function. Data value will be passed as argument.
device.commands.set_value(name="test",
                          value=False,
                          callback_on_change=callback_cmd_test
                          )

# Connect to the broker ------------------------------------------------------------------------------------------------
_connected = False
while not _connected:
    print("Trying to connect to broker %s:%d ..." % (_MQTT_HOST, _MQTT_PORT))

    _connected = device.connect(
        host=_MQTT_HOST,
        port=_MQTT_PORT,
        user=_MQTT_USER,
        password=_MQTT_PASSW,
    )

    if not _connected:
        print("  Error, could not connect. Trying again in a few seconds ...")
        time.sleep(3)

# Send birth message
device.publish_birth()

# Send some telemetry values ---------------------------------------
# NOTE: From another app, you can try to send commands to this entity

value = 0  # Simple counter

for i in range(5):

    # Update the data value
    device.data.set_value("value", value)

    # Send data values
    print("Sending data - value : %d" % value)
    device.publish_data()

    # Increase counter
    value += 1

    # Sleep some time
    time.sleep(5)

# Disconnect device -------------------------------------------------

# After disconnection the MQTT broker will send the entity DEATH message.
print("Disconnecting device")
device.disconnect()

print("Application finished !")