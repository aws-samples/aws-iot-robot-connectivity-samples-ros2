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
from std_msgs.msg import String
from awscrt import mqtt 
from telemetry_mqtt.connection_helper import ConnectionHelper

RETRY_WAIT_TIME_SECONDS = 5


class MqttPublisher(Node):
    def __init__(self):
        super().__init__('mqtt_publisher')
        self.declare_parameter("path_for_config", "")
        self.declare_parameter("discover_endpoints", False)

        path_for_config = self.get_parameter("path_for_config").get_parameter_value().string_value
        discover_endpoints = self.get_parameter("discover_endpoints").get_parameter_value().bool_value
        self.connection_helper = ConnectionHelper(self.get_logger(), path_for_config, discover_endpoints)

        self.init_subs()

    def init_subs(self):
        """Subscribe to mock ros2 telemetry topic"""
        self.subscription = self.create_subscription(
            String,
            'mock_telemetry',
            self.listener_callback,
            10
        )

    def listener_callback(self, msg):
        """Callback for the mock ros2 telemetry topic"""
        message_json = msg.data
        self.get_logger().info("Received data on ROS2 {}\nPublishing to AWS IoT".format(msg.data))
        self.connection_helper.mqtt_conn.publish(
            topic="ros2_mock_telemetry_topic",
            payload=message_json,
            qos=mqtt.QoS.AT_LEAST_ONCE
        )

def main(args=None):
    rclpy.init(args=args)

    minimal_subscriber = MqttPublisher()

    rclpy.spin(minimal_subscriber)

    # Destroy the node
    minimal_subscriber.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()
