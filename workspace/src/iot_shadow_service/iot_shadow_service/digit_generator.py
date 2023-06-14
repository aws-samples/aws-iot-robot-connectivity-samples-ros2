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
import random
import rclpy
import threading
from iot_shadow_service_msgs.srv import UpdateShadow


def main():
    rclpy.init()
    node = rclpy.create_node('digit_generator')

    # Spin in a separate thread
    thread = threading.Thread(target=rclpy.spin, args=(node, ), daemon=True)
    thread.start()

    client = node.create_client(UpdateShadow, "publish_to_shadow")
    while not client.wait_for_service(timeout_sec=1.0):
        node.get_logger().info('service not available, waiting again...')
    req = UpdateShadow.Request()

    rate = node.create_rate(0.25)  # Every 4s
    try:
        while rclpy.ok():
            next_digit = random.randint(1, 100)
            req.desired = json.dumps({"digit": next_digit})

            node.get_logger().info(f"Next digit: {next_digit}")
            res = client.call(req)
            node.get_logger().debug(f"Result of service call: {res}")
            rate.sleep()
    except KeyboardInterrupt:
        pass

    thread.join()
    rclpy.shutdown()


if __name__ == "__main__":
    main()
