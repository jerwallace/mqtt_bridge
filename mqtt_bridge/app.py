# -*- coding: utf-8 -*-
from __future__ import absolute_import

import inject
from awscrt import mqtt
import rclpy
from rclpy.node import Node

from .bridge import create_bridge
from .mqtt_client import create_private_path_extractor
from .util import lookup_object

class MQTTBridge(Node):
    
    def create_config(mqtt_client, serializer, deserializer, mqtt_private_path):
        if isinstance(serializer, basestring):
            serializer = lookup_object(serializer)
        if isinstance(deserializer, basestring):
            deserializer = lookup_object(deserializer)
        private_path_extractor = create_private_path_extractor(mqtt_private_path)
        def config(binder):
            binder.bind('serializer', serializer)
            binder.bind('deserializer', deserializer)
            binder.bind(mqtt.Client, mqtt_client)
            binder.bind('mqtt_private_path_extractor', private_path_extractor)
        return config

    def __init__(self):
        super().__init__('mqtt_bridge_node')  

        # load parameters
        
        self.declare_parameters(
            namespace='',
            parameters=[
                ('mqtt', None),
                ('bridge', None)
            ])
        
        params = self.get_parameter("~")
        mqtt_params = params.pop("mqtt", {})
        conn_params = mqtt_params.pop("client")
        bridge_params = params.get("bridge", [])

        # create mqtt client
        mqtt_client_factory_name = self.get_parameter(
            "~mqtt_client_factory")
        mqtt_client_factory = lookup_object(mqtt_client_factory_name)
        self.mqtt_client = mqtt_client_factory(mqtt_params)
        print(self.mqtt_client.__name__)
        # load serializer and deserializer
        serializer = params.get('serializer', 'json:dumps')
        deserializer = params.get('deserializer', 'json:loads')

        # dependency injection
        config = create_config(
            self.mqtt_client, serializer, deserializer, mqtt_private_path)
        inject.configure(config)

        # configure and connect to MQTT broker
        self.mqtt_client.on_connect = _on_connect
        self.mqtt_client.on_disconnect = _on_disconnect
        self.mqtt_client.connect()

        # configure bridges
        bridges = []
        for bridge_args in bridge_params:
            bridges.append(create_bridge(**bridge_args))

    def _on_connect(self, client, userdata, flags, response_code):
        self.get_logger().info('MQTT connected')

    def _on_disconnect(self, client, userdata, response_code):
        self.get_logger().info('MQTT disconnected')
    
    def tear_down(self):
        disconnect_future = self.mqtt_client.disconnect()
        disconnect_future.result()

def main(args=None):
    rclpy.init(args=args)

    node = MQTTBridge()

    def stop_node(*args):
        node.tear_down()
        rclpy.shutdown()
        return True
    
    signal.signal(signal.SIGINT, stop_node)
    signal.signal(signal.SIGTERM, stop_node)

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        node.tear_down()
        pass
    
    node.destroy_node()
    rclpy.shutdown()

if __name__ == '__main__':
    main()

__all__ = ['MqttBridge']
