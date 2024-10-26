# Revision log - Sparkplug B Wrapper

## Version 2.0.4 - PENDING RELEASE

- restructure of _mqtt.publish calls, now via a generic function ( for future usage)
- restructure the creation of topics, now via internal _spb_namespace property instead of hardcoded namespace
- added a more complex example SCADA + EoND ( simple_spb_example.py)
- fixed issue due to MetricValue callback end loop due to rebirth message. Now callback are disabled if updating data via BIRTH message.
- 

## Version 2.0.3 - remove unnecessary dependency - 241025

- removed ghost pandas dependency ( unnecessary )

## Version 2.0.2 - added support for other spB Metric datatypes - 241022

- modifications to support extra spB metric types ( bytes, File, UUID, DataSet . . .)
- Added Unit tests for most of the basic wrapper classes
- Modify simple_eond_example.py to show all possible Value types.

## Version 2.0.1 - spB Application and SCADA entities - 

**IMPORTANT:** Be aware that some of the previous classes method names may be updated, to unify name convention and clarity. Therefore, review your code if you have used previous library versions.

Overview of implemented changes:

- Implementation of Sparkplug B Application and Scada helper classes
  - Automatic discovery of domain spB entities
  - Virtual subscription to Edge Nodes (EoN) and Device Nodes (EoND) - retrieve latest data, subscribe to callbacks ( BIRTH, DEATH, MESSAGE, DATA, COMMANDS)
- Internal re-structure of helper classes and inheritance, creation of base class SpbEntity
- Update library examples based on latest changes-
- Incorporated GitHub issues and pull requests
  - Birth certificates are not persistent by default. Added a parameters to class initializations to set birth persistence.
  - Update protobuf to version 3.20.3
  - Added QoS to publish functions.
  - Fixed misspellings

