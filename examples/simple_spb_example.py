import time
from mqtt_spb_wrapper import MqttSpbEntityDevice, MqttSpbEntityScada

_DEBUG = True  # Enable debug messages

# MQTT Broker parameters
_MQTT_HOST = "localhost"
_MQTT_PORT = 1883
_MQTT_USER = ""
_MQTT_PASSW = ""

# Sparkplug B parameters - Create the spB entity object
_config_spb_domain_name = "DomainTest"
_config_spb_edge_node_name = "Edge-001"
_config_spb_device_name = "Device-01"
_config_spb_scada_name = "Scada-01"

print("--- Sparkplug B example - Simple SCADA application with a EoND and SCADA entities")

# ---------------------------------------------------------------------------------------------------------------------
# Create a new SpB EoND Entity
# ---------------------------------------------------------------------------------------------------------------------

def device_callback_command(cmd_payload):
    print("EonD received CMD payload message: %s" % str(cmd_payload))

def device_callback_cmd_test(data_value):
    print("EonD CMD <test> received - value: " + str(data_value))

def device_callback_cmd_rebirth(data_value):
    global device
    print("EoND CMD <rebirth> received - value: " + str(data_value))
    if data_value:
        device.publish_birth()
        print("EoND publishing the birth certificate !")

device = MqttSpbEntityDevice(spb_domain_name=_config_spb_domain_name,
                             spb_eon_name=_config_spb_edge_node_name,
                             spb_eon_device_name=_config_spb_device_name,
                             retain_birth=True,
                             debug_enabled=_DEBUG)
device.on_command = device_callback_command  # Callback for received device commands

# --- Data / Telemetry
device.data.set_value(name="value", value=0)

# --- Attributes
device.attributes.set_value("description", "Simple EoN Device node")
device.attributes.set_value("type", "Simulated-EoND-device")
device.attributes.set_value("version", "0.01")

# --- Commands
device.commands.set_value(name="rebirth", value=False, callback_on_change=device_callback_cmd_rebirth)
device.commands.set_value(name="test", value=False, callback_on_change=device_callback_cmd_test)


# ---------------------------------------------------------------------------------------------------------------------
# Create a new SCADA Entity
# ---------------------------------------------------------------------------------------------------------------------
# Create the SCADA entity to listen to all spB messages
scada = MqttSpbEntityScada(spb_domain_name=_config_spb_domain_name,
                           spb_scada_name=_config_spb_scada_name,
                           debug_enabled=_DEBUG)

# ATTRIBUTES - Scada application entity
scada.attributes.set_value("description", "SCADA application simple")

# Setting SCADA callbacks on received messages.
scada.callback_birth = lambda topic, payload: print("SCADA birth msg on - " + str(topic))
scada.callback_data = lambda topic, payload: print("SCADA data msg on - " + str(topic))
scada.callback_death = lambda topic, payload: print("SCADA death msg on - " + str(topic))
scada.callback_new_eon = lambda _eon_name: print("SCADA new EoN entity: " + str(_eon_name))
scada.callback_new_eond = lambda _eon_name, _eond_name: print("SCADA new EoND entity: " + str(_eond_name) + "." + str(_eon_name))

# ---------------------------------------------------------------------------------------------------------------------
# Connect to the spB MQTT broker
# ---------------------------------------------------------------------------------------------------------------------

# EoND device connection ----------------------------------------
_connected = False
while not _connected:
    print("EoND Trying to connect to broker %s:%d ..." % (_MQTT_HOST, _MQTT_PORT))
    _connected = device.connect(
        host=_MQTT_HOST,
        port=_MQTT_PORT,
        user=_MQTT_USER,
        password=_MQTT_PASSW,
    )
    if not _connected:
        print("  Error, could not connect. Trying again in a few seconds ...")
        time.sleep(3)

# Send device birth message
device.publish_birth()

# SCADA connection ----------------------------------------
_connected = False
while not _connected:
    print("SCADA Trying to connect to broker %s:%d ..." % (_MQTT_HOST, _MQTT_PORT))
    _connected = scada.connect(
        host=_MQTT_HOST,
        port=_MQTT_PORT,
        user=_MQTT_USER,
        password=_MQTT_PASSW,
        skip_death=True, # Do not publish death message - ghost SCADA entity :)
    )
    if not _connected:
        print("  Error, could not connect. Trying again in a few seconds ...")
        time.sleep(3)

# scada.publish_birth()   # Do not publish birth message - ghost SCADA entity :)

print("Waiting for SCADA initialization...")
while not scada.is_initialized():
    time.sleep(0.1)

print(" SCADA detected entities: ")
for eon in scada.entities_eon.values():
    print("  %s " % eon.entity_domain)
    for eond in scada.entities_eon[eon.entity_name].entities_eond.values():
        print("  %s " % eond.entity_domain)
print("")

# Virtual subscription to an EoND device--------------------------------------------------------------
print("SCADA subscribing to virtual device %s - %s" %(_config_spb_edge_node_name, _config_spb_device_name))
device_scada = scada.get_edge_device(_config_spb_edge_node_name, _config_spb_device_name)
print("  Current device status: " + str(device_scada))

# Set a generic callback when device birth message is received.
def scada_device_on_birth( birth_payload ):
    print("SCADA EoND received BIRTH - " + str(birth_payload))
device_scada.callback_birth = scada_device_on_birth

# DEVICE CALL BACK - Generate a callback when device send specific value, to trigger an example action.
def scada_device_on_value_data( value ):
    print("SCADA EoND %s data value: %s " % (device_scada.entity_name, str(value)))

    # ACTION example - if value = 2 then trigger a EoND rebirth action
    if value == 2:
        device_scada.send_command("rebirth", True)

device_scada.data.set_callback("value", scada_device_on_value_data)  # set callback.
print("  Current device data: " + str(device_scada.data))
print("")

# ---------------------------------------------------------------------------------------------------------------------
# APPLICATION - Send some telemetry values via EoND and some commands from the SCADA
# ---------------------------------------------------------------------------------------------------------------------
counter = 0  # Simple counter
for i in range(5):

    # Update the data value
    device.data.set_value("value", counter)

    # Send data values
    print(">>EoND Sending data - value : %d" % counter)
    device.publish_data()

    # Increase counter
    counter += 1

    # Sleep some time
    time.sleep(5)

# Disconnect device -------------------------------------------------

# After disconnection, the MQTT broker will send the device entity DEATH message.
print("Disconnecting device")
device.disconnect()

print("Application finished !")