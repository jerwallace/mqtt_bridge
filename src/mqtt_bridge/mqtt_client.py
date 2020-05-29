# -*- coding: utf-8 -*-
import ssl
import rospy
import rospkg
import boto3
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import urllib
rospack = rospkg.RosPack()

def get_root_ca(): 
    certificate = urllib.URLopener()
    path = rospack.get_path('mqtt_bridge')+"/AmazonRootCA1.pem"
    certificate.retrieve("https://www.amazontrust.com/repository/AmazonRootCA1.pem", path)
    return path

def get_endpoint(): 
    aws_client = boto3.client('iot')
    response = aws_client.describe_endpoint(
        endpointType='iot:Data-ATS'
    )
    return response['endpointAddress']

def default_mqtt_client_factory(params):
    """ MQTT Client factory

    :param dict param: configuration parameters
    :return mqtt.Client: MQTT Client
    """
    # create client:
    mqtt_client = params.get('client', {})
    aws_iot_params = params.get('aws', {})
    useWebsocket = aws_iot_params.pop('useWebsocket', True)
    useInstanceProfileRole = aws_iot_params.pop('useInstanceProfileRole', True)
    endpoint = aws_iot_params.pop('endpoint', get_endpoint())
    cert = aws_iot_params.pop('rootCA', get_root_ca())

    client = AWSIoTMQTTClient(mqtt_client.pop('id', 'default_client'), useWebsocket=useWebsocket)
    
    if useWebsocket == False:
        client.configureCredentials(cert, rospack.get_path('mqtt_bridge')+"/certs/"+aws_iot_params.pop('privateKey', "private.pem.key"), rospack.get_path('mqtt_bridge')+"/certs/"+aws_iot_params.pop('certificate', "certificate.pem.crt"))
    else:
        if useInstanceProfileRole == False:
            if rospy.has_param('accessKeyId') and rospy.has_param('secretAccessKey'):
                client.configureIAMCredentials(rospy.get_param('accessKeyId'), rospy.get_param('secretAccessKey'))
            else:
                client.configureIAMCredentials(aws_iot_params.pop('accessKeyId', ""), aws_iot_params.pop('secretAccessKey', ""))
        client.configureCredentials(cert)
            
    client.configureEndpoint(endpoint, aws_iot_params.pop('port', 443))
    client.configureOfflinePublishQueueing(mqtt_client.pop('publish_queuing', -1))  # Infinite offline Publish queueing
    client.configureDrainingFrequency(mqtt_client.pop('draining_frequency', 2))  # Draining: 2 Hz
    client.configureConnectDisconnectTimeout(mqtt_client.pop('connect_timeout_in_s', 10))  # 10 sec
    client.configureMQTTOperationTimeout(mqtt_client.pop('operation_timeout_in_s', 5))  # 5 sec
    return client

def create_private_path_extractor(mqtt_private_path):
    def extractor(topic_path):
        if topic_path.startswith('~/'):
            return '{}/{}'.format(mqtt_private_path, topic_path[2:])
        return topic_path
    return extractor

__all__ = ['default_mqtt_client_factory', 'create_private_path_extractor']
