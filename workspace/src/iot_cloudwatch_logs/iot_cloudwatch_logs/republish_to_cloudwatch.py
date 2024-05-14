#!/usr/bin/env python3
#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
import rclpy
from rclpy.node import Node
from awscrt import mqtt 
import json

from telemetry_mqtt.connection_helper import ConnectionHelper

from rcl_interfaces.msg import Log

log_level_map = {
    int.from_bytes(Log.DEBUG, byteorder='big'): "DEBUG",
    int.from_bytes(Log.INFO, byteorder='big'): "INFO",
    int.from_bytes(Log.WARN, byteorder='big'): "WARN",
    int.from_bytes(Log.ERROR, byteorder='big'): "ERROR",
    int.from_bytes(Log.FATAL, byteorder='big'): "FATAL",
}

class RepublishToCloudwatch(Node):
    def __init__(self):
        super().__init__('republish_to_cloudwatch')
        self.declare_parameter("path_for_config", "")
        self.declare_parameter("discover_endpoints", False)
        self.declare_parameter("aws_iot_log_topic", "$aws/rules/ros2_logs/my_ros2_robot_thing/logs")

        path_for_config = self.get_parameter("path_for_config").get_parameter_value().string_value
        discover_endpoints = self.get_parameter("discover_endpoints").get_parameter_value().bool_value
        self.connection_helper = ConnectionHelper(self.get_logger(), path_for_config, discover_endpoints)


        self.subscription = self.create_subscription(
            Log,
            '/rosout',
            self.listener_callback,
            10)
        self.subscription  # prevent unused variable warning

    def listener_callback(self, msg):
        topic_name = self.get_parameter("aws_iot_log_topic").get_parameter_value().string_value
        level = log_level_map.get(msg.level, "")
        
        cwl_message = {
            "name": msg.name,
            "msg": msg.msg,
            "file": msg.file,
            "function": msg.function,
            "line": msg.line,
            "level": level,
            "time": f"{msg.stamp.sec}.{msg.stamp.nanosec}"
        }

        self.connection_helper.mqtt_conn.publish(
            topic=topic_name,
            payload=json.dumps(cwl_message),
            qos=mqtt.QoS.AT_LEAST_ONCE
        )

def main(args=None):
    rclpy.init(args=args)

    subscriber = RepublishToCloudwatch()

    rclpy.spin(subscriber)

    # Destroy the node explicitly
    # (optional - otherwise it will be done automatically
    # when the garbage collector destroys the node object)
    subscriber.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()
