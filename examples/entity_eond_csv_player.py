import yaml
import pandas as pd
from mqtt_spb_wrapper import *


_APP_VER = "01.220505"

_DEBUG = True   # Enable debug messages

print("--- Sparkplug B - Simulated End Of Node Device CSV player - Version " + _APP_VER + "  JFG ---")

# Load application configuration file
try:
    with open("config_csv_player.yml") as fr:
        config = yaml.load(fr, yaml.FullLoader)
    print("Configuration file loaded")
except:
    print("ERROR - Could not load config file")
    exit()

# Create the spB entity object
device = MqttSparkPlugB_Entity_Device( domain_id=config['sparkplugb']['domain_id'],
                                       edge_node_id=config['sparkplugb']['edge_node_id'],
                                       device_id=config['sparkplugb']['device_id'],
                                       debug_info=_DEBUG)

# Load data from CSV file -----------------------------------------------------------------------
print("Loading data")
df = pd.read_csv ( config['data']['file'], delimiter=";")
config_replay_interval = config['data']['replay_interval']
print("Loading data - finished")

# Remap spB device ATTR, DATA, CMD fields as specified in the config file -----------------------

# MAP the telemetry fields - references to the CSV columns ( PD series )
telemetry = {}
for k in config['data']['data']:
    value = config['data']['data'][k]
    ref = value

    # Map the CSV column to the telemetry field
    if isinstance(value, str) and "file." in value:
        try:
            field = value.split(".")[1]
            ref = df[field]
        except:
            print("WARNING - Could not map telemetry field - %s : %s"%(k, value))

    telemetry[k] = ref  # Save the reference

# MAP the attributes fields - static data - get data from CSV columns ( PD series )
attributes = {}
for k in config['data']['attributes']:
    value = config['data']['attributes'][k]
    ref = value

    if isinstance(value, str) and "file." in value:
        # Map the CSV column to the telemetry field
        try:
            field = value.split(".")[1]
            ref = df[field][0]
        except:
            print("WARNING - Could not map attribute field - %s : %s"%(k, value))

    attributes[k] = ref  # Save the reference

# MAP the commands fields - static data
commands = {}
for k in config['data']['commands']:
    value = config['data']['commands'][k]
    ref = value

    if isinstance(value, str) and "file." in value:
        # Map the CSV column to the telemetry field
        try:
            field = value.split(".")[1]
            ref = df[field][0]
        except:
            print("WARNING - Could not map commands field - %s : %s"%(k, value))

    commands[k] = ref  # Save the reference

# Fillout the device fields and console print values ---------------------------------------------------

print("--- ATTRIBUTES")
for k in attributes:
    print("  %s - %s"%(k, str(attributes[k])))
    device.attribures.add_value(k, attributes[k])

print("--- COMMANDS")
for k in commands:
    print("  %s - %s"%(k, str(commands[k])))
    device.commands.add_value(k, commands[k])

print("--- TELEMETRY")
for k in telemetry.keys():
    if isinstance(telemetry[k], pd.Series):
        value = telemetry[k][0]
    else:
        value = telemetry[k]
    print("  %s - %s"%(k, str(value)))
    device.data.add_value(k, value)


# Reply spB device data on the MQTT server ----------------------------------------------------------

# Connect to the MQTT broker
device.connect( config['mqtt']['host'],
                config['mqtt']['port'],
                config['mqtt']['user'],
                config['mqtt']['pass'])

# Iterate device data
for i in range(100):

    # Update the field telemetry data
    for k in device.data.get_name_list():
        value = telemetry[k]
        if isinstance(value, pd.Series):
            device.data.update_value(k, value[i])
        else:
            device.data.update_value(k, value)

    # Send data values
    device.publish_data()

    # Get next field data from file
    values = ""
    for v in telemetry.values():
        if values != "":
            values += "; "
        if isinstance(v, pd.Series):
            values += str(v[i]) # Pandas Series
        else:
            values += str(v)    # Static value

    print("  %s"%( values ))

    # Wait some time, reply time
    time.sleep(config_replay_interval)