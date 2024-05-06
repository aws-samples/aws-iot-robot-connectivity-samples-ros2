import rclpy
from rclpy.node import Node
from awscrt import mqtt, io
from awsiot import mqtt_connection_builder
from awsiot.greengrass_discovery import DiscoveryClient
import json
import time

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

        with open(path_for_config) as f:
          cert_data = json.load(f)

        self.get_logger().info("Config we are loading is :\n{}".format(cert_data))
        if discover_endpoints:
            self.get_logger().info("Discovering endpoints for connection")
            self.connect_using_discovery(cert_data)
        else:
            self.get_logger().info("Connecting directly to endpoint")
            self.connect_to_endpoint(cert_data)

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

        self.mqtt_conn.publish(
            topic=topic_name,
            payload=json.dumps(cwl_message),
            qos=mqtt.QoS.AT_LEAST_ONCE
        )

    def connect_to_endpoint(self, cert_data):
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
        self.get_logger().info("Connected!")

    def connect_using_discovery(self, cert_data):
        tries = 0

        tls_options = io.TlsContextOptions.create_client_with_mtls_from_path(
            cert_data["certificatePath"],
            cert_data["privateKeyPath"],
        )
        tls_options.override_default_trust_store_from_path(None, cert_data["rootCAPath"])
        tls_context = io.ClientTlsContext(tls_options)

        region = cert_data["region"]
        retry_attempts = cert_data["retryAttempts"]
        retry_wait_time = cert_data["retryWaitTime"]

        discovery_client = DiscoveryClient(
            io.ClientBootstrap.get_or_create_static_default(),
            io.SocketOptions(),
            tls_context,
            region,
        )
        resp_future = discovery_client.discover(cert_data["clientID"])
        discover_response = resp_future.result()
        self.get_logger().debug(f"Discovery response is: {discover_response}")

        for tries in range(retry_attempts):
            self.get_logger().info(f"Connection attempt: {tries}")
            for gg_group in discover_response.gg_groups:
                for gg_core in gg_group.cores:
                    for connectivity_info in gg_core.connectivity:
                        try:
                            self.get_logger().debug(
                                "Trying core {} as host {}:{}".format(
                                    gg_core.thing_arn,
                                    connectivity_info.host_address,
                                    connectivity_info.port
                                )
                            )
                            self.mqtt_conn = self.build_greengrass_connection(
                                gg_group,
                                connectivity_info,
                                cert_data
                            )
                            return
                        except Exception as e:
                            self.get_logger().error(f"Connection failed with exception: {e}")
                            continue
            time.sleep(retry_wait_time)
        raise Exception("All connection attempts failed!")

    def build_greengrass_connection(self, gg_group, connectivity_info, cert_data):
        conn = mqtt_connection_builder.mtls_from_path(
            endpoint=connectivity_info.host_address,
            port=connectivity_info.port,
            cert_filepath=cert_data["certificatePath"],
            pri_key_filepath=cert_data["privateKeyPath"],
            ca_bytes=gg_group.certificate_authorities[0].encode('utf-8'),
            client_id=cert_data["clientID"],
            clean_session=False,
            keep_alive_secs=30
        )
        connect_future = conn.connect()
        connect_future.result()
        self.get_logger().info("Connected!")
        return conn


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
