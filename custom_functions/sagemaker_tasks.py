import os, pathlib, datetime, json
from aws_cdk import (
    Duration,
    aws_ec2 as ec2,
    aws_stepfunctions_tasks as tasks,
    aws_stepfunctions as stepfunctions,
    aws_logs as logs
)
from custom_constructs.CNetwork import CNetwork
from custom_constructs.CLambda import CLambdaFunction
from custom_constructs.CECS import CFargateTaskDefinition
from custom_constructs.utils import get_local_project_root

def get_transform_task(
    scope, 
    construct_id, 
    job_name, 
    model_name_lkp, 
    instance_type_lkp, 
    s3_data_source=None, 
    s3_data_source_lkp=None, 
    transform_out_dir=None, 
    transform_out_dir_lkp=None,
    max_retries=3,
    timeout_in_seconds=300,
    instance_count=1,
    content_type='text/csv',
    split_type='Line'
):
    transform_job_name = f"{job_name}-{datetime.datetime.now().strftime('%Y-%m-%d-H-%M-%S')}"
    create_transform_job_step = tasks.CallAwsService(
        scope, construct_id,
        service='sagemaker',
        action='createTransformJob',
        parameters={
            'TransformJobName': transform_job_name,
            'ModelName': stepfunctions.JsonPath.string_at(model_name_lkp),
            'ModelClientConfig': {
                'InvocationsMaxRetries': max_retries,
                'InvocationsTimeoutInSeconds': timeout_in_seconds
            },
            'TransformInput': {
                'DataSource': {
                    'S3DataSource': {
                        'S3Uri': stepfunctions.JsonPath.string_at(s3_data_source_lkp) if s3_data_source_lkp else s3_data_source,
                        'S3DataType': 'S3Prefix'
                    }
                },
                'ContentType': content_type,
                'SplitType': split_type
            },
            'TransformOutput': {
                'S3OutputPath': stepfunctions.JsonPath.string_at(transform_out_dir_lkp) if transform_out_dir_lkp else transform_out_dir
            },
            'TransformResources': {
                'InstanceCount': instance_count,
                'InstanceType': stepfunctions.JsonPath.string_at(instance_type_lkp)
            }
        },
        result_path=f"$.{construct_id}Task",
        iam_resources=['*'],
        integration_pattern=stepfunctions.IntegrationPattern.REQUEST_RESPONSE
    )

    wait_step = stepfunctions.Wait(scope, f"{create_transform_job_step.id}Wait", time=stepfunctions.WaitTime.duration(Duration.seconds(30)))

    poll_status_step = tasks.CallAwsService(
        scope, 
        f"{create_transform_job_step.id}PollStatus",
        service='sagemaker',
        action='describeTransformJob',
        parameters={ 'TransformJobName': transform_job_name },
        iam_resources=['*'],
        result_path=f"$.{create_transform_job_step.id}PollStatusTask",
    )
    job_status_lkp=f'{poll_status_step._result_path}.TransformJobStatus'
    out_dir_lkp=f'{poll_status_step._result_path}.TransformOutput.S3OutputPath'

    end_step = stepfunctions.Pass(scope, f"{create_transform_job_step.id}End")

    transform_failed = stepfunctions.Fail(
        scope, f"{create_transform_job_step.id}TransformFailed",
        cause='Transform job failed',
        error='TransformJobFailed'
    )

    
    is_transform_done_step = stepfunctions.Choice(scope, f"{create_transform_job_step.id}IsTransformDone") \
        .when(stepfunctions.Condition.string_equals(job_status_lkp, 'Completed'), \
            end_step \
        ).when(stepfunctions.Condition.string_equals(job_status_lkp, 'Failed'), \
            transform_failed \
        ).when(stepfunctions.Condition.string_equals(job_status_lkp, 'Stopped'), \
            transform_failed \
        ).otherwise(wait_step)

    return [
        create_transform_job_step \
        .next(wait_step) \
        .next(poll_status_step) \
        .next(is_transform_done_step),
        end_step,
        out_dir_lkp
    ]

def get_dq_bl_task(
    scope, 
    construct_id,
    job_name,
    baseline_job_role,
    monitor_dir,
    instance_type_lkp,
    image_uri="156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer",
    dataset_format={'csv': {'header': True}},
    instance_count=1,
    volume_size=20,
    max_runtime=1800

):
    analysis_type='DATA_QUALITY'
    environment = {
        'dataset_source': '/opt/ml/processing/sm_input',
        'output_path': '/opt/ml/processing/sm_output',
        'dataset_format': json.dumps(dataset_format),
        'analysis_type': analysis_type,
        'publish_cloudwatch_metrics': 'Disabled'
    }

    return tasks.CallAwsService(
        scope, 
        construct_id,
        service='sagemaker',
        action='createProcessingJob',
        parameters={
            'ProcessingJobName': f"{job_name}-{datetime.datetime.now().strftime('%Y-%m-%d-H-%M-%S')}",
            'ProcessingResources': {
                'ClusterConfig': {
                    'InstanceCount': instance_count,
                    'InstanceType': stepfunctions.JsonPath.string_at(instance_type_lkp),
                    'VolumeSizeInGB': volume_size
                }
            },
            'AppSpecification': {
                'ImageUri': image_uri
            },
            'Environment': environment,
            'ProcessingInputs':[{
                'InputName': 'input',
                'S3Input': {
                    'S3Uri': monitor_dir+'/baseline.csv',
                    'LocalPath': '/opt/ml/processing/sm_input',
                    'S3DataType': 'S3Prefix',
                    'S3InputMode': 'File'
                }
            }],
            'ProcessingOutputConfig':{
                'Outputs': [{
                    'OutputName': 'output',
                    'S3Output': {
                        'S3Uri': f'{monitor_dir}/info',
                        'LocalPath': '/opt/ml/processing/sm_output',
                        'S3UploadMode': 'EndOfJob'
                    }
                }]
            },
            'RoleArn': baseline_job_role.role_arn,
            'StoppingCondition': {
                'MaxRuntimeInSeconds': max_runtime
            }
        },
        result_path=f"$.{construct_id}Task",
        iam_resources=['*'],
        integration_pattern=stepfunctions.IntegrationPattern.REQUEST_RESPONSE  
    )

def get_mq_bl_task(
    scope, 
    construct_id,
    job_name,
    baseline_job_role,
    monitor_dir,
    instance_type_lkp,
    problem_type,
    inference_attribute,
    ground_truth_attribute,
    probability_attribute=None,
    probability_threshold_attribute=None,
    positive_label=None,
    image_uri="156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer",
    dataset_format={'csv': {'header': True}},
    instance_count=1,
    volume_size=20,
    max_runtime=1800

):
    analysis_type='MODEL_QUALITY'
    environment = {
        'dataset_source': '/opt/ml/processing/sm_input',
        'output_path': '/opt/ml/processing/sm_output',
        'dataset_format': json.dumps(dataset_format),
        'analysis_type': analysis_type,
        'problem_type': problem_type,
        'inference_attribute': inference_attribute,
        'ground_truth_attribute': ground_truth_attribute,
        'publish_cloudwatch_metrics': 'Disabled'
    }

    if probability_attribute: environment['probability_attribute'] = probability_attribute
    if probability_threshold_attribute: environment['probability_threshold_attribute'] = probability_threshold_attribute
    if positive_label: environment['positive_label'] = positive_label

    return tasks.CallAwsService(
        scope, 
        construct_id,
        service='sagemaker',
        action='createProcessingJob',
        parameters={
            'ProcessingJobName': f"{job_name}-{datetime.datetime.now().strftime('%Y-%m-%d-H-%M-%S')}",
            'ProcessingResources': {
                'ClusterConfig': {
                    'InstanceCount': instance_count,
                    'InstanceType': stepfunctions.JsonPath.string_at(instance_type_lkp),
                    'VolumeSizeInGB': volume_size
                }
            },
            'AppSpecification': {
                'ImageUri': image_uri
            },
            'Environment': environment,
            'ProcessingInputs':[{
                'InputName': 'input',
                'S3Input': {
                    'S3Uri': monitor_dir+'/baseline.csv',
                    'LocalPath': '/opt/ml/processing/sm_input',
                    'S3DataType': 'S3Prefix',
                    'S3InputMode': 'File'
                }
            }],
            'ProcessingOutputConfig':{
                'Outputs': [{
                    'OutputName': 'output',
                    'S3Output': {
                        'S3Uri': f'{monitor_dir}/info',
                        'LocalPath': '/opt/ml/processing/sm_output',
                        'S3UploadMode': 'EndOfJob'
                    }
                }]
            },
            'RoleArn': baseline_job_role.role_arn,
            'StoppingCondition': {
                'MaxRuntimeInSeconds': max_runtime
            }
        },
        result_path=f"$.{construct_id}Task",
        iam_resources=['*'],
        integration_pattern=stepfunctions.IntegrationPattern.REQUEST_RESPONSE  
    )

def get_mb_bl_task(
    scope, 
    construct_id,
    job_name,
    baseline_job_role,
    monitor_dir,
    instance_type_lkp,
    problem_type,
    inference_attribute,
    ground_truth_attribute,
    probability_attribute=None,
    probability_threshold_attribute=None,
    positive_label=None,
    exclude_features_attribute=None,
    image_uri="156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer",
    dataset_format={'csv': {'header': True}},
    instance_count=1,
    volume_size=20,
    max_runtime=1800

):
    environment = {
        'dataset_source': '/opt/ml/processing/sm_input',
        'output_path': '/opt/ml/processing/sm_output',
        'dataset_format': json.dumps(dataset_format),
        'analysis_type': 'MODEL_BIAS',
        'problem_type': problem_type,
        'inference_attribute': inference_attribute,
        'ground_truth_attribute': ground_truth_attribute,
        'publish_cloudwatch_metrics': 'Disabled'
    }
    if probability_attribute: environment['probability_attribute'] = probability_attribute
    if probability_threshold_attribute: environment['probability_threshold_attribute'] = probability_threshold_attribute
    if positive_label: environment['positive_label'] = positive_label
    if exclude_features_attribute: environment['exclude_features_attribute'] = exclude_features_attribute

    return tasks.CallAwsService(
        scope, 
        construct_id,
        service='sagemaker',
        action='createProcessingJob',
        parameters={
            'ProcessingJobName': f"{job_name}-{datetime.datetime.now().strftime('%Y-%m-%d-H-%M-%S')}",
            'ProcessingResources': {
                'ClusterConfig': {
                    'InstanceCount': instance_count,
                    'InstanceType': stepfunctions.JsonPath.string_at(instance_type_lkp),
                    'VolumeSizeInGB': volume_size
                }
            },
            'AppSpecification': {
                'ImageUri': image_uri
            },
            'ProcessingInputs':[{
                'InputName': 'input',
                'S3Input': {
                    'S3Uri': monitor_dir+'/baseline.csv',
                    'LocalPath': '/opt/ml/processing/sm_input',
                    'S3DataType': 'S3Prefix',
                    'S3InputMode': 'File'
                }
            }],
            'ProcessingOutputConfig':{
                'Outputs': [{
                    'OutputName': 'output',
                    'S3Output': {
                        'S3Uri': f'{monitor_dir}/info',
                        'LocalPath': '/opt/ml/processing/sm_output',
                        'S3UploadMode': 'EndOfJob'
                    }
                }]
            },
            'Environment': environment,
            'RoleArn': baseline_job_role.role_arn,
            'StoppingCondition': {
                'MaxRuntimeInSeconds': max_runtime
            }
        },
        result_path=f"$.{construct_id}Task",
        iam_resources=['*'],
        integration_pattern=stepfunctions.IntegrationPattern.REQUEST_RESPONSE  
    )

def get_me_bl_task(
    scope, 
    construct_id,
    job_name,
    baseline_job_role,
    monitor_dir,
    instance_type_lkp,
    inference_attribute,
    probability_attribute=None,
    exclude_features_attribute=None,
    image_uri="156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer",
    dataset_format={'csv': {'header': True}},
    instance_count=1,
    volume_size=20,
    max_runtime=1800

):
    environment = {
        'dataset_source': '/opt/ml/processing/sm_input',
        'output_path': '/opt/ml/processing/sm_output',
        'dataset_format': json.dumps(dataset_format),
        'analysis_type': 'MODEL_EXPLAINABILITY',
        'inference_attribute': inference_attribute,
        'publish_cloudwatch_metrics': 'Disabled'
    }
    if probability_attribute: environment['probability_attribute'] = probability_attribute
    if exclude_features_attribute: environment['exclude_features_attribute'] = exclude_features_attribute

    return tasks.CallAwsService(
        scope, 
        construct_id,
        service='sagemaker',
        action='createProcessingJob',
        parameters={
            'ProcessingJobName': f"{job_name}-{datetime.datetime.now().strftime('%Y-%m-%d-H-%M-%S')}",
            'ProcessingResources': {
                'ClusterConfig': {
                    'InstanceCount': instance_count,
                    'InstanceType': stepfunctions.JsonPath.string_at(instance_type_lkp),
                    'VolumeSizeInGB': volume_size
                }
            },
            'AppSpecification': {
                'ImageUri': image_uri
            },
            'Environment': environment,
            'ProcessingInputs':[{
                'InputName': 'input',
                'S3Input': {
                    'S3Uri': monitor_dir+'/baseline.csv',
                    'LocalPath': '/opt/ml/processing/sm_input',
                    'S3DataType': 'S3Prefix',
                    'S3InputMode': 'File'
                }
            }],
            'ProcessingOutputConfig':{
                'Outputs': [{
                    'OutputName': 'output',
                    'S3Output': {
                        'S3Uri': f'{monitor_dir}/info',
                        'LocalPath': '/opt/ml/processing/sm_output',
                        'S3UploadMode': 'EndOfJob'
                    }
                }]
            },
            'RoleArn': baseline_job_role.role_arn,
            'StoppingCondition': {
                'MaxRuntimeInSeconds': max_runtime
            }
        },
        result_path=f"$.{construct_id}Task",
        iam_resources=['*'],
        integration_pattern=stepfunctions.IntegrationPattern.REQUEST_RESPONSE  
    )


# ModelBiasMonitor
# ModelExplainabilityMonitor