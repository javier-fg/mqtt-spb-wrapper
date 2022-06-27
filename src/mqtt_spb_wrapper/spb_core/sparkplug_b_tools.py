#/********************************************************************************
# * Copyright (c) 2014, 2018 Saxion, Cirrus Link Solutions and others
# *
# * This program and the accompanying materials are made available under the
# * terms of the Eclipse Public License 2.0 which is available at
# * http://www.eclipse.org/legal/epl-2.0.
# *
# * SPDX-License-Identifier: EPL-2.0
# *
# * Contributors:
# *   Saxion - Javier FG
# ********************************************************************************/
from .sparkplug_b import MetricDataType

######################################################################
# Helper method for getting the value field from metrics
######################################################################
def getMetricValue(metric):

    type = metric.datatype

    if type == MetricDataType.Int8:
        return metric.int_value
    elif type == MetricDataType.Int16:
        return metric.int_value
    elif type == MetricDataType.Int32:
        return metric.int_value
    elif type == MetricDataType.Int64:
        return metric.long_value
    elif type == MetricDataType.UInt8:
        return metric.int_value
    elif type == MetricDataType.UInt16:
        return metric.int_value
    elif type == MetricDataType.UInt32:
        return metric.int_value
    elif type == MetricDataType.UInt64:
        return metric.long_value
    elif type == MetricDataType.Float:
        return metric.float_value
    elif type == MetricDataType.Double:
        return metric.double_value
    elif type == MetricDataType.Boolean:
        return metric.boolean_value
    elif type == MetricDataType.String:
        return metric.string_value
    elif type == MetricDataType.DateTime:
        return metric.long_value
    elif type == MetricDataType.Text:
        return metric.string_value
    elif type == MetricDataType.UUID:
        return metric.string_value
    elif type == MetricDataType.Bytes:
        return metric.bytes_value
    elif type == MetricDataType.File:
        return metric.bytes_value
    elif type == MetricDataType.Template:
        return metric.template_value
    else:
        print( "Invalid: " + str(type))

    return None
######################################################################