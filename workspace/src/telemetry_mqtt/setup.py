from setuptools import setup

package_name = 'telemetry_mqtt'

setup(
    name=package_name,
    version='1.0.0',
    packages=[package_name],
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='vkkondur',
    maintainer_email='vkkondur@amazon.com',
    description='AWS IoT connectivity samples for ROS2',
    license='MIT-0',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'mock_telemetry_pub = telemetry_mqtt.mock_telemetry_publisher:main',
            'mqtt_telemetry_pub = telemetry_mqtt.mqtt_telemetry_publisher:main',
        ],
    },
)
