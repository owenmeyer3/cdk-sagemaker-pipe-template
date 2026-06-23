import boto3, time, logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def endpoint_exists(sm_client, endpoint_name):
    try:
        sm_client.describe_endpoint(EndpointName=endpoint_name)
        return True
    except sm_client.exceptions.ClientError:
        return False


def endpoint_config_exists(sm_client, config_name):
    try:
        sm_client.describe_endpoint_config(EndpointConfigName=config_name)
        return True
    except sm_client.exceptions.ClientError:
        return False


def handler(event, context):
    sm_client = boto3.client('sagemaker')
    
    model_name = event['model_name']
    model_package_group_name = event['model_package_group_name']  
    model_package_version_param = event['model_package_version_param']
    instance_type = event['instance_type_param']
    data_capture_dir = event['data_capture_dir'] # 's3://omm-test-bucket/data-capture/abalone'
    endpoint_name=f'{model_package_group_name}-{model_package_version_param}-endpoint',
    endpoint_config_name = endpoint_name + "-config"

    # Create or select endpoint
    if not endpoint_config_exists(sm_client, endpoint_config_name):
        print("creating endpoint config")
        response = sm_client.create_endpoint_config(
            EndpointConfigName=endpoint_config_name,
            ProductionVariants=[
                {
                    'VariantName': 'AllTraffic',
                    'ModelName': model_name,
                    'InstanceType': instance_type,
                    'InitialInstanceCount': 1,
                    'InitialVariantWeight': 1.0
                }
            ],
            DataCaptureConfig={
                'EnableCapture': True,
                'InitialSamplingPercentage': 100,
                'DestinationS3Uri': data_capture_dir,
                'CaptureOptions': [
                    {'CaptureMode': 'Input'},
                    {'CaptureMode': 'Output'}
                ]
            }
        )
        while not endpoint_config_exists(sm_client, endpoint_config_name):
            time.sleep(5)
        print(response['EndpointConfigArn'])
    else:
        print(f"using existing endpoint config {endpoint_config_name}")

    # Create or update endpoint
    if endpoint_exists(sm_client, endpoint_name):
        print("updating endpoint")
        response = sm_client.update_endpoint(EndpointName=endpoint_name, EndpointConfigName=endpoint_config_name)
        print(response)
    else:
        print("creating endpoint")
        response = sm_client.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=endpoint_config_name)
        print(response)

    # Wait for endpoint to be InService
    waiter = sm_client.get_waiter('endpoint_in_service')
    waiter.wait(EndpointName=endpoint_name)
    
    return {'endpoint_name': endpoint_name}