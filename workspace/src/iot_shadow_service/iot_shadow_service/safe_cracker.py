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
from rclpy.qos import ReliabilityPolicy
from threading import Lock
from iot_shadow_service_msgs.msg import ShadowUpdateSnapshot
from iot_shadow_service_msgs.srv import UpdateShadow

DIGITS_ON_DIAL = 100
MAX_SPEED = 3  # digits per spin
SPIN_FREQUENCY = 0.1  # Hz


class SafeCracker(Node):
    def __init__(self):
        super().__init__("safe_cracker")
        self._target_digit = 0
        self._current_digit = 0
        self._lock = Lock()
        self._clockwise = True

        self.create_subscription(
            ShadowUpdateSnapshot,
            "shadow_update_snapshot",
            self.on_shadow_update,
            ReliabilityPolicy.BEST_EFFORT
        )

        self._update_client = self.create_client(UpdateShadow, "publish_to_shadow")
        while not self._update_client.wait_for_service(timeout_sec=1.0):
            self.get_logger().info("Update service not available, waiting again...")

        self._update_timer = self.create_timer(SPIN_FREQUENCY, self.turn_towards_target)
        self.get_logger().info("Starting to crack safe!")

    def on_shadow_update(self, update: ShadowUpdateSnapshot) -> None:
        desired = json.loads(update.desired)
        self.get_logger().info(f"Desired shadow update is {desired}")
        desired_digit = desired.get("digit", 0)
        if desired_digit != self._target_digit:
            with self._lock:
                self._target_digit = desired_digit
                self._clockwise = not self._clockwise
            direction_str = "clockwise" if self._clockwise else "anti-clockwise"
            self.get_logger().info(f"Trying to reach {self._target_digit} from {self._current_digit} spinning {direction_str}")

    def turn_towards_target(self):
        with self._lock:
            direction_mult = 1 if self._clockwise else -1
            delta = (direction_mult * (self._target_digit - self._current_digit)) % DIGITS_ON_DIAL
            change = direction_mult * min(delta, MAX_SPEED)
            if change == 0:
                return
            self._current_digit = (self._current_digit + change) % DIGITS_ON_DIAL
            direction_str = "clockwise" if self._clockwise else "anti-clockwise"
            self.get_logger().debug(f"Trying to reach {self._target_digit} from {self._current_digit} spinning {direction_str}")

        update_target_msg = UpdateShadow.Request()
        update_target_msg.reported = json.dumps({"digit": self._current_digit})
        self.get_logger().info(f"Updating target digit with message: {update_target_msg}")
        # Ignore the result of the call; we don't need to block on this
        self._update_client.call_async(update_target_msg)


def main():
    rclpy.init()
    safe_cracker = SafeCracker()
    rclpy.spin(safe_cracker)

    safe_cracker.destroy_node()
    rclpy.shutdown()


if __name__ == "__main__":
    main()
