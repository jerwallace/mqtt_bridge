
import ssl
from ament_index_python.packages import get_package_share_directory
import boto3
import urllib

def get_root_ca(): 
    certificate = urllib.URLopener()
    package_share_directory = get_package_share_directory('mqtt_bridge')
    path = package_share_directory+"/AmazonRootCA1.pem"
    certificate.retrieve("https://www.amazontrust.com/repository/AmazonRootCA1.pem", path)
    return path

# Callback when connection is accidentally lost.
def on_connection_interrupted(connection, error, **kwargs):
    print("Connection interrupted. error: {}".format(error))


# Callback when an interrupted connection is re-established.
def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))

    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        print("Session did not persist. Resubscribing to existing topics...")
        resubscribe_future, _ = connection.resubscribe_existing_topics()

        # Cannot synchronously wait for resubscribe result because we're on the connection's event-loop thread,
        # evaluate result with a callback instead.
        resubscribe_future.add_done_callback(on_resubscribe_complete)


def on_resubscribe_complete(resubscribe_future):
        resubscribe_results = resubscribe_future.result()
        print("Resubscribe results: {}".format(resubscribe_results))

        for topic, qos in resubscribe_results['topics']:
            if qos is None:
                sys.exit("Server rejected resubscribe to topic: {}".format(topic))

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
    connection_type = aws_iot_params.pop('connection_type', 'websocket')

    event_loop_group = io.EventLoopGroup(1)
    host_resolver = io.DefaultHostResolver(event_loop_group)
    client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)
    
    if connection_type == 'greengrass':
        import awsiot.greengrasscoreipc
        import awsiot.greengrasscoreipc.model as model
        ipc_client = awsiot.greengrasscoreipc.connect()
        return ipc_client
    else:
        from awscrt import io, mqtt, auth, http
        from awsiot import mqtt_connection_builder
        endpoint = aws_iot_params.pop('endpoint', get_endpoint())
        cert = aws_iot_params.pop('rootCA', get_root_ca())
        if connection_type == 'websocket':
            package_share_directory = get_package_share_directory('mqtt_bridge')
            mqtt_connection = mqtt_connection_builder.mtls_from_path(
                endpoint=endpoint,
                cert_filepath=package_share_directory+"/certs/"+aws_iot_params.pop('privateKey', "private.pem.key"),
                pri_key_filepath=package_share_directory+"/certs/"+aws_iot_params.pop('certificate', "certificate.pem.crt"),
                client_bootstrap=client_bootstrap,
                ca_filepath=cert,
                on_connection_interrupted=on_connection_interrupted,
                on_connection_resumed=on_connection_resumed,
                client_id=args.client_id,
                clean_session=False,
                keep_alive_secs=6)
        else:
            credentials_provider = auth.AwsCredentialsProvider.new_default_chain(client_bootstrap)
        mqtt_connection = mqtt_connection_builder.websockets_with_default_aws_signing(
            endpoint=endpoint,
            client_bootstrap=client_bootstrap,
            region=args.signing_region,
            credentials_provider=credentials_provider,
            websocket_proxy_options=None,
            ca_filepath=cert,
            on_connection_interrupted=on_connection_interrupted,
            on_connection_resumed=on_connection_resumed,
            client_id=args.client_id,
            clean_session=False,
            keep_alive_secs=6)
    
        connect_future = mqtt_connection.connect()
        connect_future.result()
        return mqtt_connection

def create_private_path_extractor(mqtt_private_path):
    def extractor(topic_path):
        if topic_path.startswith('~/'):
            return '{}/{}'.format(mqtt_private_path, topic_path[2:])
        return topic_path
    return extractor

__all__ = ['default_mqtt_client_factory', 'create_private_path_extractor']