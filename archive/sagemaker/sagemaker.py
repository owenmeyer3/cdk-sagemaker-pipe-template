import boto3, logging, time
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def transform_job_handler(event, context):
    sm_client = boto3.client('sagemaker')

    model_name = event['model_name']
    s3_data_source = event['s3_data_source']
    transform_out_dir = event['transform_out_dir']
    instance_type =  event['instance_type']
    execution_id=event['execution_id']

    job_name = f"TransformJob-{execution_id}"
    
    response = sm_client.create_transform_job(
        TransformJobName=job_name,
        ModelName=model_name,
        # MaxConcurrentTransforms=1,
        # ModelClientConfig={
        #     'InvocationsTimeoutInSeconds': 600,
        #     'InvocationsMaxRetries': 3
        # },
        # MaxPayloadInMB=6,
        BatchStrategy='MultiRecord',
        # Environment={'string': 'string'},
        TransformInput={
            'DataSource': {
                'S3DataSource': {
                    'S3DataType': 'S3Prefix',
                    'S3Uri': s3_data_source
                }
            },
            'ContentType': 'text/csv',
            # 'CompressionType': 'None'|'Gzip',
            'SplitType': 'Line'
        },
        TransformOutput={
            'S3OutputPath': transform_out_dir,
            'Accept': 'text/csv',
            'AssembleWith': 'Line',
            # 'KmsKeyId': 'string'
        },
        # DataCaptureConfig={
        #     'DestinationS3Uri': 'string',
        #     'KmsKeyId': 'string',
        #     'GenerateInferenceId': True|False
        # },
        TransformResources={
            'InstanceType': instance_type,
            'InstanceCount': 1,
            # 'VolumeKmsKeyId': 'string',
            # 'TransformAmiVersion': 'string'
        },
        # DataProcessing={
        #     'InputFilter': 'string',
        #     'OutputFilter': 'string',
        #     'JoinSource': 'Input'|'None'
        # },
        # Tags=[
        #     {
        #         'Key': 'string',
        #         'Value': 'string'
        #     },
        # ],
        # ExperimentConfig={
        #     'ExperimentName': 'string',
        #     'TrialName': 'string',
        #     'TrialComponentDisplayName': 'string',
        #     'RunName': 'string'
        # }
    )

    # Poll until complete
    while True:
        response = sm_client.describe_transform_job(TransformJobName=job_name)
        status = response['TransformJobStatus']

        if status == 'Completed':
            return {
                'TRANSFORM_JOB_ARM': response['TransformJobArn'],
                'JOB_NAME': job_name,
                'OUTPUT_PATH': transform_out_dir,
                'STATUS': status
            }
        elif status == 'Failed':
            raise Exception(f"Transform job failed: {response.get('FailureReason')}")
        elif status == 'Stopped':
            raise Exception(f"Transform job stopped unexpectedly")

        time.sleep(15) 