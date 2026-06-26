import boto3, logging, json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

##############################################
############### DELETE MONITORS JOBS ##############
##############################################
def delete_monitor_job(sm_client, endpoint_name, monitoring_type): # 'DataQuality':|'ModelQuality'|'ModelBias'|'ModelExplainability'
    if monitoring_type == 'DataQuality':
        for job in sm_client.list_data_quality_job_definitions(EndpointName=endpoint_name)['JobDefinitionSummaries']:
            logger.info(f"DELETING:{job['MonitoringJobDefinitionName']}")
            response = sm_client.delete_data_quality_job_definition(JobDefinitionName=job['MonitoringJobDefinitionName'])
    elif monitoring_type == 'ModelQuality':
        for job in sm_client.list_model_quality_job_definitions(EndpointName=endpoint_name)['JobDefinitionSummaries']:
            logger.info(f"DELETING:{job['MonitoringJobDefinitionName']}")
            response = sm_client.delete_model_quality_job_definition(JobDefinitionName=job['MonitoringJobDefinitionName'])
    elif monitoring_type == 'ModelBias':
        for job in sm_client.list_model_bias_job_definitions(EndpointName=endpoint_name)['JobDefinitionSummaries']:
            logger.info(f"DELETING:{job['MonitoringJobDefinitionName']}")
            response = sm_client.delete_model_bias_job_definition(JobDefinitionName=job['MonitoringJobDefinitionName'])
    elif monitoring_type == 'ModelExplainability':
        for job in sm_client.list_model_explainability_job_definitions(EndpointName=endpoint_name)['JobDefinitionSummaries']:
            logger.info(f"DELETING:{job['MonitoringJobDefinitionName']}")
            response = sm_client.delete_model_explainability_job_definition(JobDefinitionName=job['MonitoringJobDefinitionName'])
    else:
        logger.info(f" INVALID monitoring_type. Choose 'DataQuality':|'ModelQuality'|'ModelBias'|'ModelExplainability' ")


##############################################
############### DELETE MONITORS ##############
##############################################
def delete_monitor(sm_client, endpoint_name, monitoring_type): # 'DataQuality':|'ModelQuality'|'ModelBias'|'ModelExplainability'
    schedules = sm_client.list_monitoring_schedules(EndpointName=endpoint_name)
    for schedule in schedules['MonitoringScheduleSummaries']:
        name=schedule['MonitoringScheduleName']
        logger.info(f"schedule: {name}")
        detail = sm_client.describe_monitoring_schedule(MonitoringScheduleName=name)
        logger.info(detail['MonitoringType'])
        if detail['MonitoringType'] == monitoring_type:
            logger.info(f"deleting {detail['MonitoringType']} monitor: {name}")
            response = sm_client.delete_monitoring_schedule(MonitoringScheduleName=name)


##############################################
############### JOB DEFINITIONS ##############
##############################################

def create_data_quality_job_definition(        
    sm_client, 
    name, 
    role_arn,
    deploy_type,
    monitor_dir,
    image_uri="156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer",
    instance_count=1, 
    instance_type='ml.m5.large', 
    volume_size_in_gb=20, 
    max_runtime_in_seconds=1800,  
    dataset_format={'Csv': {'Header': True}}, # {'Csv':{'Header': True|False},'Json': {'Line': True|False}, Parquet': {}}
    endpoint_name=None, 
    data_cature_dir=None
    ):
    if deploy_type == 'realtime':
        job_input={
            'EndpointInput': {
                'EndpointName': endpoint_name,
                'LocalPath': '/opt/ml/processing/input/endpoint'#,
                # 'S3InputMode': 'Pipe'|'File',
                # 'S3DataDistributionType': 'FullyReplicated'|'ShardedByS3Key',
                # 'FeaturesAttribute': 'string',
                # 'InferenceAttribute': 'string',
                # 'ProbabilityAttribute': 'string',
                # 'ProbabilityThresholdAttribute': 123.0,
                # 'StartTimeOffset': 'string',
                # 'EndTimeOffset': 'string',
                # 'ExcludeFeaturesAttribute': 'string'
            }
        }
    else:
        job_input={
            'BatchTransformInput': {
                'DataCapturedDestinationS3Uri': f'{data_cature_dir}/',
                'DatasetFormat': dataset_format,
                'LocalPath': '/opt/ml/processing/input'#,
                # 'S3InputMode': 'Pipe'|'File',
                # 'S3DataDistributionType': 'FullyReplicated'|'ShardedByS3Key',
                # 'FeaturesAttribute': 'string',
                # 'InferenceAttribute': 'string',
                # 'ProbabilityAttribute': 'string',
                # 'ProbabilityThresholdAttribute': 123.0,
                # 'StartTimeOffset': 'string',
                # 'EndTimeOffset': 'string',
                # 'ExcludeFeaturesAttribute': 'string'
            }
        }

    response = sm_client.create_data_quality_job_definition(
        JobDefinitionName=name,
        DataQualityBaselineConfig={
            #'BaseliningJobName': 'string',
            "ConstraintsResource": {"S3Uri": f'{monitor_dir}/info/constraints.json'},
            "StatisticsResource": {"S3Uri": f'{monitor_dir}/info/statistics.json'}
        },
        DataQualityAppSpecification={
            'ImageUri': image_uri#,
            # 'ContainerEntrypoint': ['string',],
            # 'ContainerArguments': ['string',],
            # 'RecordPreprocessorSourceUri': 'string',
            # 'PostAnalyticsProcessorSourceUri': 'string',
            # 'Environment': {'string': 'string'}
        },
        DataQualityJobInput=job_input,
        DataQualityJobOutputConfig={
            'MonitoringOutputs': [
                {
                    'S3Output': {
                        'S3Uri': f'{monitor_dir}/reports',
                        'LocalPath': '/opt/ml/processing/output'#,
                        # 'S3UploadMode': 'Continuous'|'EndOfJob'
                    }
                },
            ],
            # 'KmsKeyId': 'string'
        },
        JobResources={
            'ClusterConfig': {
                'InstanceCount': instance_count,
                'InstanceType': instance_type,
                'VolumeSizeInGB': volume_size_in_gb#,
                # 'VolumeKmsKeyId': 'string'
            }
        },
        NetworkConfig={
            # 'EnableInterContainerTrafficEncryption': True|False,
            # 'EnableNetworkIsolation': True|False,
            # 'VpcConfig': vpc_config
        },
        RoleArn=role_arn,
        StoppingCondition={
            'MaxRuntimeInSeconds': max_runtime_in_seconds
        },
        # Tags=[{'Key': 'string', 'Value': 'string'},]
    )

    return response


def create_model_bias_job_definition(        
    sm_client, 
    name, 
    role_arn,
    deploy_type,
    monitor_dir,
    ground_truth_dir,
    image_uri="156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer",
    instance_count=1, 
    instance_type='ml.m5.large', 
    volume_size_in_gb=20, 
    max_runtime_in_seconds=1800,  
    dataset_format={'Csv': {'Header': True}}, # {'Csv':{'Header': True|False},'Json': {'Line': True|False}, Parquet': {}}
    endpoint_name=None, 
    data_cature_dir=None
    ):
    if deploy_type == 'realtime':
        job_input={
            'EndpointInput': {
                'EndpointName': endpoint_name,
                'LocalPath': '/opt/ml/processing/input/endpoint'#,
                # 'S3InputMode': 'Pipe'|'File',
                # 'S3DataDistributionType': 'FullyReplicated'|'ShardedByS3Key',
                # 'FeaturesAttribute': 'string',
                # 'InferenceAttribute': 'string',
                # 'ProbabilityAttribute': 'string',
                # 'ProbabilityThresholdAttribute': 123.0,
                # 'StartTimeOffset': 'string',
                # 'EndTimeOffset': 'string',
                # 'ExcludeFeaturesAttribute': 'string'
            }
        }
    else:
        job_input={
            'BatchTransformInput': {
                'DataCapturedDestinationS3Uri': f'{data_cature_dir}/',
                'DatasetFormat': dataset_format,
                'LocalPath': '/opt/ml/processing/input'#,
                # 'S3InputMode': 'Pipe'|'File',
                # 'S3DataDistributionType': 'FullyReplicated'|'ShardedByS3Key',
                # 'FeaturesAttribute': 'string',
                # 'InferenceAttribute': 'string',
                # 'ProbabilityAttribute': 'string',
                # 'ProbabilityThresholdAttribute': 123.0,
                # 'StartTimeOffset': 'string',
                # 'EndTimeOffset': 'string',
                # 'ExcludeFeaturesAttribute': 'string'
            }
        }
    job_input['GroundTruthS3Input']={'S3Uri': ground_truth_dir}

    response = sm_client.create_model_bias_job_definition(
        JobDefinitionName=name,
        ModelBiasBaselineConfig={
            #'BaseliningJobName': 'string',
            "ConstraintsResource": {"S3Uri": f'{monitor_dir}/info/constraints.json'}
        },
        ModelBiasAppSpecification={
            'ImageUri': image_uri,
            'ConfigUri': f'{monitor_dir}/check_output',
            # 'Environment': {'string': 'string'}
        },
        ModelBiasJobInput=job_input,
        ModelBiasJobOutputConfig={
            'MonitoringOutputs': [
                {
                    'S3Output': {
                        'S3Uri': f'{monitor_dir}/reports',
                        'LocalPath': '/opt/ml/processing/output'#,
                        # 'S3UploadMode': 'Continuous'|'EndOfJob'
                    }
                },
            ],
            # 'KmsKeyId': 'string'
        },
        JobResources={
            'ClusterConfig': {
                'InstanceCount': instance_count,
                'InstanceType': instance_type,
                'VolumeSizeInGB': volume_size_in_gb#,
                # 'VolumeKmsKeyId': 'string'
            }
        },
        NetworkConfig={
            # 'EnableInterContainerTrafficEncryption': True|False,
            # 'EnableNetworkIsolation': True|False,
            # 'VpcConfig': vpc_config
        },
        RoleArn=role_arn,
        StoppingCondition={
            'MaxRuntimeInSeconds': max_runtime_in_seconds
        },
        # Tags=[{'Key': 'string', 'Value': 'string'},]
    )

    return response


def create_model_explainability_job_definition(        
    sm_client, 
    name, 
    role_arn,
    deploy_type,
    monitor_dir,
    image_uri="156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer",
    instance_count=1, 
    instance_type='ml.m5.large', 
    volume_size_in_gb=20, 
    max_runtime_in_seconds=1800,  
    dataset_format={'Csv': {'Header': True}}, # {'Csv':{'Header': True|False},'Json': {'Line': True|False}, Parquet': {}}
    endpoint_name=None, 
    data_cature_dir=None
    ):
    if deploy_type == 'realtime':
        job_input={
            'EndpointInput': {
                'EndpointName': endpoint_name,
                'LocalPath': '/opt/ml/processing/input/endpoint'#,
                # 'S3InputMode': 'Pipe'|'File',
                # 'S3DataDistributionType': 'FullyReplicated'|'ShardedByS3Key',
                # 'FeaturesAttribute': 'string',
                # 'InferenceAttribute': 'string',
                # 'ProbabilityAttribute': 'string',
                # 'ProbabilityThresholdAttribute': 123.0,
                # 'StartTimeOffset': 'string',
                # 'EndTimeOffset': 'string',
                # 'ExcludeFeaturesAttribute': 'string'
            }
        }
    else:
        job_input={
            'BatchTransformInput': {
                'DataCapturedDestinationS3Uri': f'{data_cature_dir}/',
                'DatasetFormat': dataset_format,
                'LocalPath': '/opt/ml/processing/input'#,
                # 'S3InputMode': 'Pipe'|'File',
                # 'S3DataDistributionType': 'FullyReplicated'|'ShardedByS3Key',
                # 'FeaturesAttribute': 'string',
                # 'InferenceAttribute': 'string',
                # 'ProbabilityAttribute': 'string',
                # 'ProbabilityThresholdAttribute': 123.0,
                # 'StartTimeOffset': 'string',
                # 'EndTimeOffset': 'string',
                # 'ExcludeFeaturesAttribute': 'string'
            }
        }

    response = sm_client.create_model_explainability_job_definition(
        JobDefinitionName=name,
        ModelExplainabilityBaselineConfig={
            #'BaseliningJobName': 'string',
            "ConstraintsResource": {"S3Uri": f'{monitor_dir}/info/constraints.json'}
        },
        ModelExplainabilityAppSpecification={
            'ImageUri': image_uri,
            'ConfigUri': f'{monitor_dir}/check_output',
            # 'Environment': {'string': 'string'}
        },
        ModelExplainabilityJobInput=job_input,
        ModelExplainabilityJobOutputConfig={
            'MonitoringOutputs': [
                {
                    'S3Output': {
                        'S3Uri': f'{monitor_dir}/reports',
                        'LocalPath': '/opt/ml/processing/output'#,
                        # 'S3UploadMode': 'Continuous'|'EndOfJob'
                    }
                },
            ],
            # 'KmsKeyId': 'string'
        },
        JobResources={
            'ClusterConfig': {
                'InstanceCount': instance_count,
                'InstanceType': instance_type,
                'VolumeSizeInGB': volume_size_in_gb#,
                # 'VolumeKmsKeyId': 'string'
            }
        },
        NetworkConfig={
            # 'EnableInterContainerTrafficEncryption': True|False,
            # 'EnableNetworkIsolation': True|False,
            # 'VpcConfig': vpc_config
        },
        RoleArn=role_arn,
        StoppingCondition={
            'MaxRuntimeInSeconds': max_runtime_in_seconds
        },
        # Tags=[{'Key': 'string', 'Value': 'string'},]
    )

    return response


def create_model_quality_job_definition(        
    sm_client, 
    name, 
    role_arn,
    deploy_type,
    monitor_dir,
    ground_truth_label,
    ground_truth_dir,
    problem_type,
    image_uri="156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer",
    instance_count=1, 
    instance_type='ml.m5.large', 
    volume_size_in_gb=20, 
    max_runtime_in_seconds=1800,  
    dataset_format={'Csv': {'Header': True}}, # {'Csv':{'Header': True|False},'Json': {'Line': True|False}, Parquet': {}}
    endpoint_name=None, 
    data_cature_dir=None
):
    if deploy_type == 'realtime':
        job_input={
            'EndpointInput': {
                'EndpointName': endpoint_name,
                'LocalPath': '/opt/ml/processing/input/endpoint',
                # 'S3InputMode': 'Pipe'|'File',
                # 'S3DataDistributionType': 'FullyReplicated'|'ShardedByS3Key',
                # 'FeaturesAttribute': 'string',
                'InferenceAttribute': ground_truth_label,
                # 'ProbabilityAttribute': 'string',
                # 'ProbabilityThresholdAttribute': 123.0,
                # 'StartTimeOffset': 'string',
                # 'EndTimeOffset': 'string',
                # 'ExcludeFeaturesAttribute': 'string'
            }
        }
    else:
        job_input={
            'BatchTransformInput': {
                'DataCapturedDestinationS3Uri': f'{data_cature_dir}/',
                'DatasetFormat': dataset_format,
                'LocalPath': '/opt/ml/processing/input'#,
                # 'S3InputMode': 'Pipe'|'File',
                # 'S3DataDistributionType': 'FullyReplicated'|'ShardedByS3Key',
                # 'FeaturesAttribute': 'string',
                # 'InferenceAttribute': 'string',
                # 'ProbabilityAttribute': 'string',
                # 'ProbabilityThresholdAttribute': 123.0,
                # 'StartTimeOffset': 'string',
                # 'EndTimeOffset': 'string',
                # 'ExcludeFeaturesAttribute': 'string'
            }
        }
    job_input['GroundTruthS3Input']={'S3Uri': ground_truth_dir}

    response = sm_client.create_model_quality_job_definition(
        JobDefinitionName=name,
        ModelQualityBaselineConfig={
            #'BaseliningJobName': 'string',
            "ConstraintsResource": {"S3Uri": f'{monitor_dir}/info/constraints.json'}
        },
        ModelQualityAppSpecification={
            'ImageUri': image_uri,
            'ProblemType': problem_type,
            # 'ContainerEntrypoint': ['string',],
            # 'ContainerArguments': ['string',],
            # 'RecordPreprocessorSourceUri': 'string',
            # 'PostAnalyticsProcessorSourceUri': 'string',
            # 'Environment': {'string': 'string'}
        },
        ModelQualityJobInput=job_input,
        ModelQualityJobOutputConfig={
            'MonitoringOutputs': [
                {
                    'S3Output': {
                        'S3Uri': f'{monitor_dir}/reports',
                        'LocalPath': '/opt/ml/processing/output'#,
                        # 'S3UploadMode': 'Continuous'|'EndOfJob'
                    }
                },
            ],
            # 'KmsKeyId': 'string'
        },
        JobResources={
            'ClusterConfig': {
                'InstanceCount': instance_count,
                'InstanceType': instance_type,
                'VolumeSizeInGB': volume_size_in_gb#,
                # 'VolumeKmsKeyId': 'string'
            }
        },
        NetworkConfig={
            # 'EnableInterContainerTrafficEncryption': True|False,
            # 'EnableNetworkIsolation': True|False,
            # 'VpcConfig': vpc_config
        },
        RoleArn=role_arn,
        StoppingCondition={
            'MaxRuntimeInSeconds': max_runtime_in_seconds
        },
        # Tags=[{'Key': 'string', 'Value': 'string'},]
    )
    return response


##############################################
############### CREATE SCHEDULES ##############
##############################################
# 'DataQuality'|'ModelQuality'|'ModelBias'|'ModelExplainability'
def create_data_quality_monitoring_schedule(
    sm_client, 
    name,
    role_arn,
    deploy_type,
    monitor_dir,
    image_uri="156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer",
    instance_count=1,
    instance_type='ml.m5.large', 
    volume_size_in_gb=20, 
    max_runtime_in_seconds=1800,
    dataset_format={'Csv': {'Header': True}}, # {'Csv':{'Header': True|False},'Json': {'Line': True|False}, Parquet': {}} 
    schedule_expression='cron(0 * ? * * *)', 
    data_analysis_start_time="-PT2H", 
    data_analysis_end_time="-PT1H",
    endpoint_name=None, 
    data_cature_dir=None
):

    job_definition_name = f'{name}-job'

    response = create_data_quality_job_definition(        
        sm_client, 
        job_definition_name,
        role_arn, 
        deploy_type, 
        monitor_dir, 
        image_uri,
        instance_count=instance_count,
        instance_type=instance_type, 
        volume_size_in_gb=volume_size_in_gb, 
        max_runtime_in_seconds=max_runtime_in_seconds,
        dataset_format=dataset_format,
        endpoint_name=endpoint_name, 
        data_cature_dir=data_cature_dir, 
    )

    response = sm_client.create_monitoring_schedule(
        MonitoringScheduleName=name,
        MonitoringScheduleConfig={
            'ScheduleConfig': {
                'ScheduleExpression': schedule_expression,
                'DataAnalysisStartTime': data_analysis_start_time,
                'DataAnalysisEndTime': data_analysis_end_time
            },
            'MonitoringJobDefinitionName': job_definition_name,
            'MonitoringType': 'DataQuality'
        },
        # Tags=[{'Key': 'string', 'Value': 'string'},]
    )
    return response


def create_model_bias_monitoring_schedule(
    sm_client, 
    name,
    role_arn,
    deploy_type,
    monitor_dir,
    ground_truth_dir,
    image_uri="156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer",
    instance_count=1,
    instance_type='ml.m5.large', 
    volume_size_in_gb=20, 
    max_runtime_in_seconds=1800,
    dataset_format={'Csv': {'Header': True}}, # {'Csv':{'Header': True|False},'Json': {'Line': True|False}, Parquet': {}} 
    schedule_expression='cron(0 * ? * * *)', 
    data_analysis_start_time="-PT2H", 
    data_analysis_end_time="-PT1H",
    endpoint_name=None, 
    data_cature_dir=None
):
    job_definition_name = f'{name}-job'

    response = create_model_bias_job_definition(        
        sm_client, 
        job_definition_name,
        role_arn,
        deploy_type, 
        monitor_dir, 
        ground_truth_dir,
        image_uri=image_uri,
        instance_count=instance_count, 
        instance_type=instance_type, 
        volume_size_in_gb=volume_size_in_gb, 
        max_runtime_in_seconds=max_runtime_in_seconds,
        dataset_format=dataset_format,
        endpoint_name=endpoint_name, 
        data_cature_dir=data_cature_dir, 
    )

    response = sm_client.create_monitoring_schedule(
        MonitoringScheduleName=name,
        MonitoringScheduleConfig={
            'ScheduleConfig': {
                'ScheduleExpression': schedule_expression,
                'DataAnalysisStartTime': data_analysis_start_time,
                'DataAnalysisEndTime': data_analysis_end_time
            },
            'MonitoringJobDefinitionName': job_definition_name,
            'MonitoringType': 'ModelBias'
        },
        # Tags=[{'Key': 'string', 'Value': 'string'},]
    )
    return response


def create_model_explainability_monitoring_schedule(
    sm_client, 
    name,
    role_arn,
    deploy_type,
    monitor_dir,
    image_uri="156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer",
    instance_count=1,
    instance_type='ml.m5.large', 
    volume_size_in_gb=20, 
    max_runtime_in_seconds=1800,
    dataset_format={'Csv': {'Header': True}}, # {'Csv':{'Header': True|False},'Json': {'Line': True|False}, Parquet': {}} 
    schedule_expression='cron(0 * ? * * *)', 
    data_analysis_start_time="-PT2H", 
    data_analysis_end_time="-PT1H",
    endpoint_name=None, 
    data_cature_dir=None
):

    job_definition_name = f'{name}-job'

    response = create_model_explainability_job_definition(        
        sm_client, 
        job_definition_name,
        role_arn, 
        deploy_type, 
        monitor_dir, 
        image_uri=image_uri,
        instance_count=instance_count, 
        instance_type=instance_type, 
        volume_size_in_gb=volume_size_in_gb, 
        max_runtime_in_seconds=max_runtime_in_seconds,  
        dataset_format=dataset_format,
        endpoint_name=endpoint_name, 
        data_cature_dir=data_cature_dir
    )

    response = sm_client.create_monitoring_schedule(
        MonitoringScheduleName=name,
        MonitoringScheduleConfig={
            'ScheduleConfig': {
                'ScheduleExpression': schedule_expression,
                'DataAnalysisStartTime': data_analysis_start_time,
                'DataAnalysisEndTime': data_analysis_end_time
            },
            'MonitoringJobDefinitionName': job_definition_name,
            'MonitoringType': 'ModelExplainability'
        },
        # Tags=[{'Key': 'string', 'Value': 'string'},]
    )
    return response


def create_model_quality_monitoring_schedule(
    sm_client, 
    name,
    role_arn,
    deploy_type,
    problem_type, # 'BinaryClassification'|'MulticlassClassification'|'Regression'
    ground_truth_label,
    monitor_dir,
    ground_truth_dir,
    image_uri="156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer",
    instance_count=1,
    instance_type='ml.m5.large', 
    volume_size_in_gb=20, 
    max_runtime_in_seconds=1800,
    dataset_format={'Csv': {'Header': True}}, # {'Csv':{'Header': True|False},'Json': {'Line': True|False}, Parquet': {}} 
    schedule_expression='cron(0 * ? * * *)', 
    data_analysis_start_time="-PT2H", 
    data_analysis_end_time="-PT1H",
    endpoint_name=None, 
    data_cature_dir=None
):

    job_definition_name = f'{name}-job'

    response = create_model_quality_job_definition(        
        sm_client, 
        job_definition_name, 
        role_arn,
        deploy_type, 
        monitor_dir, 
        ground_truth_label,
        ground_truth_dir,
        problem_type,
        image_uri=image_uri,
        instance_count=instance_count, 
        instance_type=instance_type, 
        volume_size_in_gb=volume_size_in_gb, 
        max_runtime_in_seconds=max_runtime_in_seconds,  
        dataset_format=dataset_format,
        endpoint_name=endpoint_name, 
        data_cature_dir=data_cature_dir
    )

    response = sm_client.create_monitoring_schedule(
        MonitoringScheduleName=name,
        MonitoringScheduleConfig={
            'ScheduleConfig': {
                'ScheduleExpression': schedule_expression,
                'DataAnalysisStartTime': data_analysis_start_time,
                'DataAnalysisEndTime': data_analysis_end_time
            },
            'MonitoringJobDefinitionName': job_definition_name,
            'MonitoringType': 'ModelQuality'
        },
        # Tags=[{'Key': 'string', 'Value': 'string'},]
    )
    return response


def data_quality_handler(event, context):
    monitoring_type='DataQuality'

    endpoint_name = event['endpoint_name'] if 'endpoint_name' in event else None
    data_cature_dir = event['data_cature_dir'] if 'data_cature_dir' in event else None
    name = event['name']
    role_arn = event['monitor_role']
    deploy_type = event['deploy_type']
    monitor_dir = event['monitor_dir']
    image_uri = event['image_uri'] if 'image_uri' in event else "156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer"
    instance_count = event['instance_count'] if 'instance_count' in event else 1
    instance_type = event['instance_type'] if 'instance_type' in event else 'ml.m5.large'
    volume_size_in_gb = event['volume_size_in_gb'] if 'volume_size_in_gb' in event else 20
    max_runtime_in_seconds = event['max_runtime_in_seconds'] if 'max_runtime_in_seconds' in event else 1800
    dataset_format = json.loads(event['dataset_format'] if 'dataset_format' in event else {'Csv': {'Header': True}}), # {'Csv':{'Header': True|False},'Json': {'Line': True|False}, Parquet': {}} 
    schedule_expression = event['schedule_expression'] if 'schedule_expression' in event else 'cron(0 * ? * * *)'
    data_analysis_start_time = event['data_analysis_start_time'] if 'data_analysis_start_time' in event else "-PT2H"
    data_analysis_end_time = event['data_analysis_end_time'] if 'data_analysis_end_time' in event else "-PT1H"

    sm_client = boto3.client('sagemaker')
    delete_monitor(sm_client, endpoint_name, monitoring_type)
    delete_monitor_job(sm_client, endpoint_name, monitoring_type)

    result = create_data_quality_monitoring_schedule(
        sm_client, 
        f'{endpoint_name}-{name}',
        role_arn,
        deploy_type,
        monitor_dir,
        image_uri=image_uri,
        instance_count=instance_count,
        instance_type=instance_type, 
        volume_size_in_gb=volume_size_in_gb, 
        max_runtime_in_seconds=max_runtime_in_seconds,
        dataset_format=dataset_format, 
        schedule_expression=schedule_expression, 
        data_analysis_start_time=data_analysis_start_time, 
        data_analysis_end_time=data_analysis_end_time,
        endpoint_name=endpoint_name, 
        data_cature_dir=data_cature_dir
    )
    return {}


def model_bias_handler(event, context):
    monitoring_type='ModelBias'

    endpoint_name = event['endpoint_name'] if 'endpoint_name' in event else None
    data_cature_dir = event['data_cature_dir'] if 'data_cature_dir' in event else None
    name = event['name']
    role_arn = event['monitor_role']
    deploy_type = event['deploy_type']
    monitor_dir = event['monitor_dir']
    ground_truth_dir = event['ground_truth_dir']
    image_uri = event['image_uri'] if 'image_uri' in event else "156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer"
    instance_count = event['instance_count'] if 'instance_count' in event else 1
    instance_type = event['instance_type'] if 'instance_type' in event else 'ml.m5.large'
    volume_size_in_gb = event['volume_size_in_gb'] if 'volume_size_in_gb' in event else 20
    max_runtime_in_seconds = event['max_runtime_in_seconds'] if 'max_runtime_in_seconds' in event else 1800
    dataset_format = json.loads(event['dataset_format'] if 'dataset_format' in event else {'Csv': {'Header': True}}), # {'Csv':{'Header': True|False},'Json': {'Line': True|False}, Parquet': {}} 
    schedule_expression = event['schedule_expression'] if 'schedule_expression' in event else 'cron(0 * ? * * *)'
    data_analysis_start_time = event['data_analysis_start_time'] if 'data_analysis_start_time' in event else "-PT2H"
    data_analysis_end_time = event['data_analysis_end_time'] if 'data_analysis_end_time' in event else "-PT1H"

    sm_client = boto3.client('sagemaker')
    delete_monitor(sm_client, endpoint_name, monitoring_type)
    delete_monitor_job(sm_client, endpoint_name, monitoring_type)

    result = create_model_bias_monitoring_schedule(
        sm_client, 
        f'{endpoint_name}-{name}',
        role_arn,
        deploy_type,
        monitor_dir,
        ground_truth_dir,
        image_uri=image_uri,
        instance_count=instance_count,
        instance_type=instance_type, 
        volume_size_in_gb=volume_size_in_gb, 
        max_runtime_in_seconds=max_runtime_in_seconds,
        dataset_format=dataset_format, 
        schedule_expression=schedule_expression, 
        data_analysis_start_time=data_analysis_start_time, 
        data_analysis_end_time=data_analysis_end_time,
        endpoint_name=endpoint_name, 
        data_cature_dir=data_cature_dir
    )
    return {}


def model_explainability_handler(event, context):
    monitoring_type='ModelExplainability'

    endpoint_name = event['endpoint_name'] if 'endpoint_name' in event else None
    data_cature_dir = event['data_cature_dir'] if 'data_cature_dir' in event else None
    name = event['name']
    role_arn = event['monitor_role']
    deploy_type = event['deploy_type']
    monitor_dir = event['monitor_dir']
    image_uri = event['image_uri'] if 'image_uri' in event else "156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer"
    instance_count = event['instance_count'] if 'instance_count' in event else 1
    instance_type = event['instance_type'] if 'instance_type' in event else 'ml.m5.large'
    volume_size_in_gb = event['volume_size_in_gb'] if 'volume_size_in_gb' in event else 20
    max_runtime_in_seconds = event['max_runtime_in_seconds'] if 'max_runtime_in_seconds' in event else 1800
    dataset_format = json.loads(event['dataset_format'] if 'dataset_format' in event else {'Csv': {'Header': True}}), # {'Csv':{'Header': True|False},'Json': {'Line': True|False}, Parquet': {}} 
    schedule_expression = event['schedule_expression'] if 'schedule_expression' in event else 'cron(0 * ? * * *)'
    data_analysis_start_time = event['data_analysis_start_time'] if 'data_analysis_start_time' in event else "-PT2H"
    data_analysis_end_time = event['data_analysis_end_time'] if 'data_analysis_end_time' in event else "-PT1H"
    
    sm_client = boto3.client('sagemaker')
    delete_monitor(sm_client, endpoint_name, monitoring_type)
    delete_monitor_job(sm_client, endpoint_name, monitoring_type)

    result = create_model_explainability_monitoring_schedule(
        sm_client, 
        f'{endpoint_name}-{name}',
        role_arn,
        deploy_type,
        monitor_dir,
        image_uri=image_uri,
        instance_count=instance_count,
        instance_type=instance_type, 
        volume_size_in_gb=volume_size_in_gb, 
        max_runtime_in_seconds=max_runtime_in_seconds,
        dataset_format=dataset_format, 
        schedule_expression=schedule_expression, 
        data_analysis_start_time=data_analysis_start_time, 
        data_analysis_end_time=data_analysis_end_time,
        endpoint_name=endpoint_name, 
        data_cature_dir=data_cature_dir
    )
    return {}


def model_quality_handler(event, context):
    monitoring_type='ModelQuality'

    endpoint_name = event['endpoint_name'] if 'endpoint_name' in event else None
    data_cature_dir = event['data_cature_dir'] if 'data_cature_dir' in event else None
    name = event['name']
    role_arn = event['monitor_role']
    deploy_type = event['deploy_type']
    problem_type = event['problem_type'] # 'BinaryClassification'|'MulticlassClassification'|'Regression'
    ground_truth_label = event['ground_truth_label']
    monitor_dir = event['monitor_dir']
    ground_truth_dir = event['ground_truth_dir']
    image_uri = event['image_uri'] if 'image_uri' in event else "156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer"
    instance_count = event['instance_count'] if 'instance_count' in event else 1
    instance_type = event['instance_type'] if 'instance_type' in event else 'ml.m5.large'
    volume_size_in_gb = event['volume_size_in_gb'] if 'volume_size_in_gb' in event else 20
    max_runtime_in_seconds = event['max_runtime_in_seconds'] if 'max_runtime_in_seconds' in event else 1800
    dataset_format = json.loads(event['dataset_format'] if 'dataset_format' in event else {'Csv': {'Header': True}}), # {'Csv':{'Header': True|False},'Json': {'Line': True|False}, Parquet': {}} 
    schedule_expression = event['schedule_expression'] if 'schedule_expression' in event else 'cron(0 * ? * * *)'
    data_analysis_start_time = event['data_analysis_start_time'] if 'data_analysis_start_time' in event else "-PT2H"
    data_analysis_end_time = event['data_analysis_end_time'] if 'data_analysis_end_time' in event else "-PT1H"

    sm_client = boto3.client('sagemaker')
    delete_monitor(sm_client, endpoint_name, monitoring_type)
    delete_monitor_job(sm_client, endpoint_name, monitoring_type)

    result = create_model_quality_monitoring_schedule(
        sm_client, 
        f'{endpoint_name}-{name}',
        role_arn,
        deploy_type,
        problem_type,
        ground_truth_label,
        monitor_dir,
        ground_truth_dir,
        image_uri=image_uri,
        instance_count=instance_count,
        instance_type=instance_type, 
        volume_size_in_gb=volume_size_in_gb, 
        max_runtime_in_seconds=max_runtime_in_seconds,
        dataset_format=dataset_format, 
        schedule_expression=schedule_expression, 
        data_analysis_start_time=data_analysis_start_time, 
        data_analysis_end_time=data_analysis_end_time,
        endpoint_name=endpoint_name, 
        data_cature_dir=data_cature_dir
    )
    return {}