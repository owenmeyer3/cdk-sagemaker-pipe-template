import boto3, logging, json, datetime

from sagemaker.model_monitor import DefaultModelMonitor
from sagemaker.model_monitor.dataset_format import DatasetFormat

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
sm_client = boto3.client('sagemaker')

def run_dq_bl_job_handler(event, context):
    name = event['name']
    role_arn = event['role_arn']
    monitor_dir = event['monitor_dir']
    instance_type = event['instance_type']
    image_uri = event['image_uri']
    instance_count = event['instance_count']
    volume_size_in_gb = event['volume_size_in_gb']
    max_runtime_in_seconds = event['max_runtime_in_seconds']
    dataset_format = event['dataset_format'] # {'csv': {'header': True}},
    execution_id=event['execution_id']

    environment = {
        'dataset_source': monitor_dir+'/baseline.csv',
        'output_path': f'{monitor_dir}/info',
        'dataset_format': json.dumps(dataset_format),
        'analysis_type': 'DATA_QUALITY',
        'publish_cloudwatch_metrics': 'Disabled'
    }

    response = sm_client.create_processing_job(
        ProcessingJobName=f"{name}-{execution_id}",
        ProcessingResources={
            'ClusterConfig': {
                'InstanceCount': instance_count,
                'InstanceType': instance_type,
                'VolumeSizeInGB': volume_size_in_gb
            }
        },
        AppSpecification={
            'ImageUri': image_uri,
            # No ContainerEntrypoint — let the container use its default 'analyze' script
        },
        Environment=environment,
        RoleArn=role_arn,
        StoppingCondition={
            'MaxRuntimeInSeconds': max_runtime_in_seconds
        }
    )

    my_default_monitor = DefaultModelMonitor(
        role=role,
        instance_count=1,
        instance_type='ml.m5.xlarge',
        volume_size_in_gb=20,
        max_runtime_in_seconds=3600,
    )


    return {'PROCESSING_JOB_ARN':response["ProcessingJobArn"]}


def run_mq_bl_job_handler(event, context):
    name = event['name']
    role_arn = event['role_arn']
    monitor_dir = event['monitor_dir']
    inference_attribute = event['inference_attribute'] # column name of model prediction in dataset
    ground_truth_attribute = event['ground_truth_attribute'] # column name of true label in dataset
    problem_type = event['problem_type']
    instance_type = event['instance_type']
    image_uri = event['image_uri']
    instance_count = event['instance_count']
    volume_size_in_gb = event['volume_size_in_gb']
    max_runtime_in_seconds = event['max_runtime_in_seconds']
    dataset_format = event['dataset_format'] # {'csv': {'header': True}},
    execution_id=event['execution_id']

    environment = {
        'dataset_source': monitor_dir+'/baseline.csv',
        'output_path': f'{monitor_dir}/info',
        'dataset_format': json.dumps(dataset_format),
        'analysis_type': 'MODEL_QUALITY',
        'problem_type': problem_type,
        'inference_attribute': inference_attribute,
        'ground_truth_attribute': ground_truth_attribute,
        'publish_cloudwatch_metrics': 'Disabled'
    }
    if event['probability_attribute']: environment['probability_attribute'] = event['probability_attribute']
    if event['probability_threshold_attribute']: environment['probability_threshold_attribute'] = str(event['probability_threshold_attribute'])
    if event['positive_label']: environment['positive_label'] = event['positive_label']
    
    response = sm_client.create_processing_job(
        ProcessingJobName=f"{name}-{execution_id}",
        ProcessingResources={
            'ClusterConfig': {
                'InstanceCount': instance_count,
                'InstanceType': instance_type,
                'VolumeSizeInGB': volume_size_in_gb
            }
        },
        AppSpecification={
            'ImageUri': image_uri,
        },
        Environment=environment,
        RoleArn=role_arn,
        StoppingCondition={
            'MaxRuntimeInSeconds': max_runtime_in_seconds
        }
    )
    return {'PROCESSING_JOB_ARN':response["ProcessingJobArn"]}


def run_mb_bl_job_handler(event, context):
    name = event['name']
    role_arn = event['role_arn']
    monitor_dir = event['monitor_dir']
    inference_attribute = event['inference_attribute'] # column name of model prediction in dataset
    ground_truth_attribute = event['ground_truth_attribute'] # column name of true label in dataset
    problem_type = event['problem_type']
    instance_type = event['instance_type']
    image_uri = event['image_uri']
    instance_count = event['instance_count']
    volume_size_in_gb = event['volume_size_in_gb']
    max_runtime_in_seconds = event['max_runtime_in_seconds']
    dataset_format = event['dataset_format'] # {'csv': {'header': True}},
    execution_id=event['execution_id']


    environment = {
        'dataset_source': monitor_dir+'/baseline.csv',
        'output_path': f'{monitor_dir}/info',
        'dataset_format': json.dumps(dataset_format),
        'analysis_type': 'BIAS',
        'problem_type': problem_type,
        'inference_attribute': inference_attribute,
        'ground_truth_attribute': ground_truth_attribute,
        'publish_cloudwatch_metrics': 'Disabled'
    }
    if event['probability_attribute']: environment['probability_attribute'] = event['probability_attribute']
    if event['probability_threshold_attribute']: environment['probability_threshold_attribute'] = str(event['probability_threshold_attribute'])
    if event['positive_label']: environment['positive_label'] = event['positive_label']
    if event['exclude_features_attribute']: environment['exclude_features_attribute'] = event['exclude_features_attribute']

    response = sm_client.create_processing_job(
        ProcessingJobName=f"{name}-{execution_id}",
        ProcessingResources={
            'ClusterConfig': {
                'InstanceCount': instance_count,
                'InstanceType': instance_type,
                'VolumeSizeInGB': volume_size_in_gb
            }
        },
        AppSpecification={
            'ImageUri': image_uri,
        },
        Environment=environment,
        RoleArn=role_arn,
        StoppingCondition={
            'MaxRuntimeInSeconds': max_runtime_in_seconds
        }
    )
    return {'PROCESSING_JOB_ARN':response["ProcessingJobArn"]}

def run_me_bl_job_handler(event, context):
    name = event['name']
    role_arn = event['role_arn']
    monitor_dir = event['monitor_dir']
    instance_type = event['instance_type']
    image_uri = event['image_uri']
    instance_count = event['instance_count']
    volume_size_in_gb = event['volume_size_in_gb']
    max_runtime_in_seconds = event['max_runtime_in_seconds']
    dataset_format = event['dataset_format'] # {'csv': {'header': True}},
    execution_id=event['execution_id']

    environment = {
        'dataset_source': monitor_dir+'/baseline.csv',
        'output_path': f'{monitor_dir}/info',
        'dataset_format': json.dumps(dataset_format),
        'analysis_type': 'EXPLAINABILITY',
        'publish_cloudwatch_metrics': 'Disabled'
    }
    if event['inference_attribute']: environment['inference_attribute'] = event['inference_attribute']
    if event['probability_attribute']: environment['probability_attribute'] = event['probability_attribute']
    if event['exclude_features_attribute']: environment['exclude_features_attribute'] = event['exclude_features_attribute']

    response = sm_client.create_processing_job(
        ProcessingJobName=f"{name}-{execution_id}",
        ProcessingResources={
            'ClusterConfig': {
                'InstanceCount': instance_count,
                'InstanceType': instance_type,
                'VolumeSizeInGB': volume_size_in_gb
            }
        },
        AppSpecification={
            'ImageUri': image_uri,
        },
        Environment=environment,
        RoleArn=role_arn,
        StoppingCondition={
            'MaxRuntimeInSeconds': max_runtime_in_seconds
        }
    )
    return {'PROCESSING_JOB_ARN':response["ProcessingJobArn"]}