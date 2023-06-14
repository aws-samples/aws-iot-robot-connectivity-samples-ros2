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

from awscrt import mqtt
from awsiot import iotshadow, mqtt_connection_builder
from iot_shadow_service_msgs.srv import UpdateShadow
from iot_shadow_service_msgs.msg import ShadowUpdateSnapshot
import json
import queue
import rclpy
from rclpy.node import Node, SrvTypeRequest, SrvTypeResponse
import threading
from typing import Any


class IotShadowNode(Node):
    def __init__(self):
        super().__init__("iot_shadow_node")
        self.declare_parameter("path_for_config", "")
        self.declare_parameter("shadow_name", "")

        self.create_service(UpdateShadow, "publish_to_shadow", self.on_publish_to_shadow)
        self._update_pub = self.create_publisher(ShadowUpdateSnapshot, "shadow_update_snapshot", 0)

        path_for_config = self.get_parameter("path_for_config").get_parameter_value().string_value

        with open(path_for_config) as f:
          cert_data = json.load(f)

        self.get_logger().info("Config we are loading is :\n{}".format(cert_data))

        self._thing_name = cert_data["clientID"]
        self._shadow_name = self.get_parameter("shadow_name").get_parameter_value().string_value

        # Build MQTT Connection
        self._mqtt_conn = mqtt_connection_builder.mtls_from_path(
            endpoint=cert_data["endpoint"],
            port=cert_data["port"],
            cert_filepath=cert_data["certificatePath"],
            pri_key_filepath=cert_data["privateKeyPath"],
            ca_filepath=cert_data["rootCAPath"],
            client_id=cert_data["clientID"],
            http_proxy_options=None,
        )
        self.get_logger().info("About to try connecting to MQTT")
        connected_future = self._mqtt_conn.connect()

        # Create connection to IoT Shadow
        self._shadow_client = iotshadow.IotShadowClient(self._mqtt_conn)
        connected_future.result()

        self.get_logger().info("Connected using MQTT!")

        subscribe_future, _ = self._shadow_client.subscribe_to_named_shadow_updated_events(
            request=iotshadow.NamedShadowUpdatedSubscriptionRequest(
                thing_name=self._thing_name,
                shadow_name=self._shadow_name,
            ),
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=self.on_shadow_updated
        )

        subscribe_future.result()

        self.get_logger().info("Shadow connection complete!")

    def on_shadow_updated(self, event: iotshadow.ShadowUpdatedEvent) -> None:
        self.get_logger().info(f"Got update with contents: {event}")
        pub_msg = ShadowUpdateSnapshot()
        pub_msg.desired = json.dumps(event.current.state.desired)
        pub_msg.reported = json.dumps(event.current.state.reported)
        self._update_pub.publish(pub_msg)
        self.get_logger().info(f"Published message: {pub_msg}")

    def on_publish_to_shadow(self, request: SrvTypeRequest, response: SrvTypeResponse) -> SrvTypeResponse:
        self.get_logger().info(f"Got publish shadow request: {request}")

        next_shadow_state = iotshadow.ShadowState()
        if request.desired:
            next_shadow_state.desired = json.loads(request.desired)
        if request.reported:
            next_shadow_state.reported = json.loads(request.reported)

        shadow_update_request = iotshadow.UpdateNamedShadowRequest(
            thing_name=self._thing_name,
            shadow_name=self._shadow_name,
            state=next_shadow_state,
        )

        shadow_update_future = self._shadow_client.publish_update_named_shadow(
            request=shadow_update_request,
            qos=mqtt.QoS.AT_LEAST_ONCE,
        )

        try:
            shadow_update_future.result()
            self.get_logger().debug(f"Update successful for request: {shadow_update_request}")
            response.success = True
        except Exception as e:
            self.get_logger().error(f"Update failed for request: {shadow_update_request} with message: {e}")
            response.success = False

        return response


def main():
    rclpy.init()
    shadow_node = IotShadowNode()
    rclpy.spin(shadow_node)

    shadow_node.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()
