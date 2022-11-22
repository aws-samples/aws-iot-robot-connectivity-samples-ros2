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

import json
import rclpy
from rclpy.node import Node
from std_msgs.msg import String
from awscrt import mqtt, io, http, auth
from awsiot import mqtt_connection_builder


class MqttPublisher(Node):
    def __init__(self):
        super().__init__('mqtt_publisher')

        self.declare_parameter("path_for_config", "")

        path_for_config = self.get_parameter("path_for_config").get_parameter_value().string_value

        with open(path_for_config) as f:
          cert_data = json.load(f)

        self.get_logger().info("Config we are loading is :\n{}".format(cert_data))

        # Build mqtt connection
        self.mqtt_conn = mqtt_connection_builder.mtls_from_path(
            endpoint=cert_data["endpoint"],
            port= cert_data["port"],
            cert_filepath= cert_data["certificatePath"],
            pri_key_filepath= cert_data["privateKeyPath"],
            ca_filepath= cert_data["rootCAPath"],
            client_id= cert_data["clientID"],
            http_proxy_options=None,
        )
        connected_future = self.mqtt_conn.connect()
        connected_future.result()

        self.init_subs()

    def init_subs(self):
        """Subscribe to mock ros2 telemetry topic"""
        self.subscription = self.create_subscription(
            String,
            'mock_telemetry',
            self.listener_callback,
            10)

    def listener_callback(self, msg):
        """Callback for the mock ros2 telemetry topic"""
        message_json = msg.data
        self.get_logger().info("Received data on ROS2 {}\nPublishing to AWS IoT".format(msg.data))
        self.mqtt_conn.publish(
                topic="ros2_mock_telemetry_topic",
                payload=message_json,
                qos=mqtt.QoS.AT_LEAST_ONCE)

def main(args=None):
    rclpy.init(args=args)

    minimal_subscriber = MqttPublisher()

    rclpy.spin(minimal_subscriber)

    # Destroy the node
    minimal_subscriber.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()



