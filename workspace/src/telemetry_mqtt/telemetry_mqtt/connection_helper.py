
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
import time
from awscrt import io
from awsiot import mqtt_connection_builder
from awsiot.greengrass_discovery import DiscoveryClient

class ConnectionHelper:
    def __init__(self, logger, path_for_config="", discover_endpoints=False):
        self.path_for_config = path_for_config
        self.discover_endpoints=discover_endpoints
        self.logger = logger

        with open(path_for_config) as f:
          cert_data = json.load(f)

        self.logger.info("Config we are loading is :\n{}".format(cert_data))

        if discover_endpoints:
            self.logger.info("Discovering endpoints for connection")
            self.connect_using_discovery(cert_data)
        else:
            self.logger.info("Connecting directly to endpoint")
            self.connect_to_endpoint(cert_data)

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
        self.logger.info("Connected!")

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
        self.logger.debug(f"Discovery response is: {discover_response}")

        for tries in range(retry_attempts):
            self.logger.info(f"Connection attempt: {tries}")
            for gg_group in discover_response.gg_groups:
                for gg_core in gg_group.cores:
                    for connectivity_info in gg_core.connectivity:
                        try:
                            self.logger.debug(
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
                            self.logger.error(f"Connection failed with exception: {e}")
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
        self.logger.info("Connected!")
        return conn