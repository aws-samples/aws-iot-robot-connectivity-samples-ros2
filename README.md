# Connect ROS2 robots to AWS IoT and capture telemetry

Learn how to send telemetry from a ROS2-based robot to AWS IoT Core over MQTT. This code was tested on an [Ubuntu 22.04](https://www.releases.ubuntu.com/22.04/) system on [ROS2 Humble](https://docs.ros.org/en/humble/index.html). You need to setup certificates on your device to connect to AWS IoT Core, which is setup via [AWS Command Line Interface](https://aws.amazon.com/cli/) (AWS CLI) with appropriate privileges and assumes you have AWS Console access.

Install AWS CLI using the following commands:
```
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
```

Set up appropriate credentials to run AWS CLI commands. This blog assumes you are an admin user and recommend to limit permissions only required AWS IoT services commands. To learn more about methods to set up credentials and permissions, please see [configuring AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) and [AWS Identity and Access Management](https://aws.amazon.com/iam/features/manage-permissions/).

```
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_DEFAULT_REGION=us-west-2
```

Install [AWS IoT device SDK Python](https://github.com/aws/aws-iot-device-sdk-python-v2) by running the following command

```
python3 -m pip install awsiotsdk
```

Next, clone the following repository to run this sample

```
cd ~
git clone https://github.com/aws-samples/aws-iot-robot-connectivity-samples-ros2.git
```

Install the dependencies and build the code with the following command:

```
cd ~/aws-iot-robot-connectivity-samples-ros2/workspace
colcon build
source ~/aws-iot-robot-connectivity-samples-ros2/workspace/install/setup.bash
```

Your ROS2 code is built! However, before you run the code, you will need to setup certificates to communicate with AWS IoT Core. The certificates are used for Authentication and Authorization of the AWS IoT thing associated with your robot and provide encryption in transit for the data sent and received.

Each robot must have a credential to interact with AWS IoT. All traffic to and from AWS IoT is sent securely over Transport Layer Security (TLS). AWS cloud security mechanisms protect data as it moves between AWS IoT and other AWS services.

![Security](images/iot_security.png)
You are responsible for managing device credentials X.509 certificates, as well as managing policies and permissions in AWS IoT. X.509 certificates provide AWS IoT with the ability to authenticate client and device connections. Client certificates must be registered with AWS IoT before a client can communicate with AWS IoT. X.509 certificates authenticate client and device connections to AWS IoT. X.509 certificates provide several benefits over other identification and authentication mechanisms. X.509 certificates enable asymmetric keys to be used with devices. For example, you could burn private keys into secure storage on a device so that sensitive cryptographic material never leaves the device. X.509 certificates provide stronger client authentication over other schemes, such as username and password or bearer tokens, because the private key never leaves the device.

In production at scale, [AWS IoT Device Defender](https://aws.amazon.com/iot-device-defender/) tools can be used to audit and get alerts for the security posture of IoT resources and add mitigation through build in actions such as updating a device certificate, quarantining a group of devices, or replacing default policies.

You will need to set several environment variables that are required for running the IoT API calls. These are the location of where to download the certs, and example names of what to call your devices.

```
export CERT_FOLDER_LOCATION=~/aws-iot-robot-connectivity-samples-ros2/iot_certs_and_config/
export THING_NAME=my_ros2_robot_thing
export IOT_CONFIG_TEMPLATE=~/aws-iot-robot-connectivity-samples-ros2/templates/iot_config_template.json
export IOT_POLICY_TEMPLATE=~/aws-iot-robot-connectivity-samples-ros2/templates/iot_policy_template.json
export IOT_POLICY_NAME=ros2_robot_iot_policy
```

Next, create an AWS IoT Thing, and attach corresponding certificates.

```
export THING_ARN=$(aws iot create-thing --thing-name $THING_NAME --query thingArn --output text)
export CERT_ARN=$(aws iot create-keys-and-certificate --set-as-active \
--certificate-pem-outfile ${CERT_FOLDER_LOCATION}${THING_NAME}.cert.pem  \
--public-key-outfile ${CERT_FOLDER_LOCATION}${THING_NAME}.public.key \
--private-key-outfile ${CERT_FOLDER_LOCATION}${THING_NAME}.private.key \
--query certificateArn --output text)
```

The above command will download certificates in the  `$CERT_FOLDER_LOCATION` on your robot.

Next, download a root certificate to connect our AWS IoT thing to AWS IoT Core, and and attach the certs created to the thing. AWS IoT provides client certificates that are signed by the Amazon Root certificate authority (CA). The root cert is used to verify the authenticity of the certificates created.

```
ROOT_CERT_FILE=$CERT_FOLDER_LOCATION"rootCA".crt
curl https://www.amazontrust.com/repository/AmazonRootCA1.pem > $ROOT_CERT_FILE
export CERT_ID=${CERT_ARN#*cert/}
aws iot attach-thing-principal --principal $CERT_ARN --thing-name $THING_NAME
```

You now have authentication to connect to AWS IoT Core but need a policy to define permissions and boundaries for your AWS IoT thing. A sample policy is provided in the repository purely for testing purposes, and you should modify it to suit your needs.

Create a copy of the `iot_config_template.json` file with the endpoint and certificate locations for your robot.

```
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export ENDPOINT_ADDRESS=$(aws iot describe-endpoint --endpoint-type iot:Data-ATS --query endpointAddress  --output text)

export PORT=8883
export IOT_CONFIG_FILE=~/aws-iot-robot-connectivity-samples-ros2/iot_certs_and_config/iot_config.json
cat $IOT_CONFIG_TEMPLATE >> $IOT_CONFIG_FILE
export PRIV_KEY_LOCATION=$CERT_FOLDER_LOCATION$THING_NAME.private.key
export CERT_FILE=$CERT_FOLDER_LOCATION$THING_NAME.cert.pem
sed -i -e "s/ENDPOINT/$ENDPOINT_ADDRESS/g" $IOT_CONFIG_FILE
sed -i -e "s/ROOTCA/$(echo $ROOT_CERT_FILE | sed 's_/_\\/_g')/g" $IOT_CONFIG_FILE
sed -i -e "s/PRIVATEKEY/$(echo $PRIV_KEY_LOCATION | sed 's_/_\\/_g')/g" $IOT_CONFIG_FILE
sed -i -e "s/CERTPATH/$(echo $CERT_FILE | sed 's_/_\\/_g')/g" $IOT_CONFIG_FILE
sed -i -e "s/CLIENT/$THING_NAME/g" $IOT_CONFIG_FILE
sed -i -e "s/PORT/$PORT/g" $IOT_CONFIG_FILE
cat $IOT_CONFIG_FILE
```

Next we create a policy file from a template file provided. This policy defines the permissions boundary for our AWS IoT Thing. Notice that this template only allows subscription and reception to `ros2_mock_telemetry_topic`. Modify the policy per your needs as you expand your use cases.

```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "iot:Publish",
        "iot:Receive",
        "iot:RetainPublish"
      ],
      "Resource": [
        "arn:aws:iot:us-west-2:ACCOUNT_ID:topic/ros2_mock_telemetry_topic"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "iot:Subscribe"
      ],
      "Resource": [
        "arn:aws:iot:us-west-2:ACCOUNT_ID:topicfilter/ros2_mock_telemetry_topic"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "iot:Connect"
      ],
      "Resource": [
        "arn:aws:iot:us-west-2:ACCOUNT_ID:client/CLIENT"
      ]
    }
  ]
}
```

Create a policy from the template with the appropriate information using the following commands

```
export IOT_POLICY_FILE=~/aws-iot-robot-connectivity-samples-ros2/iot_certs_and_config/iot_policy.json
cat $IOT_POLICY_TEMPLATE >> $IOT_POLICY_FILE
sed -i -e "s/ACCOUNT_ID/$ACCOUNT_ID/g" $IOT_POLICY_FILE
sed -i -e "s/CLIENT/$THING_NAME/g" $IOT_POLICY_FILE
cat $IOT_POLICY_FILE
aws iot create-policy --policy-name $IOT_POLICY_NAME --policy-document file://$IOT_POLICY_FILE
aws iot attach-policy --policy-name $IOT_POLICY_NAME --target $CERT_ARN
```

You should see a response like the following:

![Policy response](images/policy_response.png)

You have completed all the setup needed to send data to AWS IoT Core! You may now remove the AWS CLI specific credentials from the robot since you now have the IoT Certificates setup for the robot to communicate with AWS IoT Core.

Launch the rosnode with the following command:

```
source ~/aws-iot-robot-connectivity-samples-ros2/workspace/install/setup.bash
ros2 run telemetry_mqtt mock_telemetry_pub
```

This ROS2 node publishes mock telemetry data on topic  mock_telemetry  and can be verified by the following command, in a new terminal

```
source ~/aws-iot-robot-connectivity-samples-ros2/workspace/install/setup.bash
ros2 topic echo mock_telemetry
```

Next run the node, that subscribes to the mock telemetry data and publishes to AWS IoT over MQTT topic `ros2_mock_telemetry_topic`

```
export IOT_CONFIG_FILE=~/aws-iot-robot-connectivity-samples-ros2/iot_certs_and_config/iot_config.json
source ~/aws-iot-robot-connectivity-samples-ros2/workspace/install/setup.bash
ros2 run telemetry_mqtt mqtt_telemetry_pub --ros-args --param path_for_config:=$IOT_CONFIG_FILE
```

To view the data being published, login to your AWS console to the MQTT test client page on the browser by searching for IoT Core in the search bar and selecting MQTT test client as shown below.
![IoT search bar](images/iot_core_console.png)
Connect to the MQTT test client on the console as shown below
![test client](images/test_client.png)
Subscribe to the topic published by the AWS IoT thing in the client, to see mocked data being published by the ROS2 node.
![mqtt subscription](images/mqtt_subscription.png)
You can now convert any ROS2 topic data and send it over as an MQTT topic over AWS IoT Core, with a thing transformation layer from ROS2 topic to JSON-formatted messages sent over an MQTT topic.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

