# Revision log - Sparkplug B Wrapper



## Version 2.0.1 - spB Application and SCADA entities

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

