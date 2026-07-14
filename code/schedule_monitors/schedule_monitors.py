import boto3, logging, json, time
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_job_input(deploy_type, **kwargs):
    job_input = {}
    if deploy_type == 'realtime':
        endpoint_input = {}
        if kwargs.get('endpoint_name'): endpoint_input['EndpointName'] = kwargs['endpoint_name']
        if kwargs.get('rt_local_path'): endpoint_input['LocalPath'] = kwargs['rt_local_path'] # '/opt/ml/processing/input/endpoint'
        if kwargs.get('s3_input_mode'): endpoint_input['S3InputMode'] = kwargs['s3_input_mode'] # 'Pipe'|'File',
        if kwargs.get('s3_data_distribution_type'): endpoint_input['S3DataDistributionType'] = kwargs['s3_data_distribution_type'] # 'FullyReplicated'|'ShardedByS3Key'
        if kwargs.get('features_attribute'): endpoint_input['FeaturesAttribute'] = kwargs['features_attribute'] # 'string'
        if kwargs.get('inference_attribute'): endpoint_input['InferenceAttribute'] = kwargs['inference_attribute'] # 'string'
        if kwargs.get('probability_attribute'): endpoint_input['ProbabilityAttribute'] = kwargs['probability_attribute'] # 'string'
        if kwargs.get('probability_threshold_attribute'): endpoint_input['ProbabilityThresholdAttribute'] = kwargs['probability_threshold_attribute'] # 'int'
        if kwargs.get('start_time_offset'): endpoint_input['StartTimeOffset'] = kwargs['start_time_offset'] # 'string'
        if kwargs.get('end_time_offset'): endpoint_input['EndTimeOffset'] = kwargs['end_time_offset'] # 'string'
        if kwargs.get('exclude_features_attribute'): endpoint_input['ExcludeFeaturesAttribute'] = kwargs['exclude_features_attribute'] # 'string'
        job_input={'EndpointInput': endpoint_input}
    else:
        transform_input = {}
        if kwargs.get('data_capture_dir'): transform_input['DataCapturedDestinationS3Uri'] = f"{kwargs['data_capture_dir']}/" # '{data_capture_dir}/',
        if kwargs.get('dataset_format'): transform_input['DatasetFormat'] = kwargs['dataset_format'] # {'Csv':{'Header': True|False},'Json': {'Line': True|False}, Parquet': {}}
        if kwargs.get('bch_local_path'): transform_input['LocalPath'] = kwargs['bch_local_path'] # '/opt/ml/processing/input'
        if kwargs.get('s3_input_mode'): transform_input['S3InputMode'] = kwargs['s3_input_mode'] # 'Pipe'|'File',
        if kwargs.get('s3_data_distribution_type'): transform_input['S3DataDistributionType'] = kwargs['s3_data_distribution_type'] # 'FullyReplicated'|'ShardedByS3Key'
        if kwargs.get('features_attribute'): transform_input['FeaturesAttribute'] = kwargs['features_attribute'] # 'string'
        if kwargs.get('inference_attribute'): transform_input['InferenceAttribute'] = kwargs['inference_attribute'] # 'string'
        if kwargs.get('probability_attribute'): transform_input['ProbabilityAttribute'] = kwargs['probability_attribute'] # 'string'
        if kwargs.get('probability_threshold_attribute'): transform_input['ProbabilityThresholdAttribute'] = kwargs['probability_threshold_attribute'] # 'int'
        if kwargs.get('start_time_offset'): transform_input['StartTimeOffset'] = kwargs['start_time_offset'] # 'string'
        if kwargs.get('end_time_offset'): transform_input['EndTimeOffset'] = kwargs['end_time_offset'] # 'string'
        if kwargs.get('exclude_features_attribute'): transform_input['ExcludeFeaturesAttribute'] = kwargs['exclude_features_attribute'] # 'string'
        job_input={'BatchTransformInput': transform_input}
    
    if kwargs.get('ground_truth_dir'): job_input['GroundTruthS3Input'] = {'S3Uri': kwargs['ground_truth_dir']}
    
    return job_input

##############################################
############### DELETE MONITORS JOBS ##############
##############################################
def wait_for_job_deletion(sm_client, job_definition_name, timeout_seconds=60, poll_interval=2):
    start = time.time()
    while time.time() - start < timeout_seconds:
        try:
            sm_client.describe_data_quality_job_definition(JobDefinitionName=job_definition_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFound":
                return True
            raise  # anything else (throttling, permissions) shouldn't be silently swallowed
        time.sleep(poll_interval)
    raise TimeoutError(f"{job_definition_name} still exists after {timeout_seconds}s")

def delete_monitor_job(sm_client, endpoint_name, monitoring_type): # 'DataQuality':|'ModelQuality'|'ModelBias'|'ModelExplainability'
    if monitoring_type == 'DataQuality':
        for job in sm_client.list_data_quality_job_definitions(EndpointName=endpoint_name)['JobDefinitionSummaries']:
            logger.info(f"DELETING:{job['MonitoringJobDefinitionName']}")
            response = sm_client.delete_data_quality_job_definition(JobDefinitionName=job['MonitoringJobDefinitionName'])
            wait_for_job_deletion(sm_client, job['MonitoringJobDefinitionName'])
    elif monitoring_type == 'ModelQuality':
        for job in sm_client.list_model_quality_job_definitions(EndpointName=endpoint_name)['JobDefinitionSummaries']:
            logger.info(f"DELETING:{job['MonitoringJobDefinitionName']}")
            response = sm_client.delete_model_quality_job_definition(JobDefinitionName=job['MonitoringJobDefinitionName'])
            wait_for_job_deletion(sm_client, job['MonitoringJobDefinitionName'])
    elif monitoring_type == 'ModelBias':
        for job in sm_client.list_model_bias_job_definitions(EndpointName=endpoint_name)['JobDefinitionSummaries']:
            logger.info(f"DELETING:{job['MonitoringJobDefinitionName']}")
            response = sm_client.delete_model_bias_job_definition(JobDefinitionName=job['MonitoringJobDefinitionName'])
            wait_for_job_deletion(sm_client, job['MonitoringJobDefinitionName'])
    elif monitoring_type == 'ModelExplainability':
        for job in sm_client.list_model_explainability_job_definitions(EndpointName=endpoint_name)['JobDefinitionSummaries']:
            logger.info(f"DELETING:{job['MonitoringJobDefinitionName']}")
            response = sm_client.delete_model_explainability_job_definition(JobDefinitionName=job['MonitoringJobDefinitionName'])
            wait_for_job_deletion(sm_client, job['MonitoringJobDefinitionName'])
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
    data_capture_dir=None
    ):

    job_input=get_job_input(
        deploy_type, 
        endpoint_name=endpoint_name,
        rt_local_path='/opt/ml/processing/input/endpoint',
        bch_local_path='/opt/ml/processing/input',
        dataset_format=dataset_format,
        data_capture_dir=data_capture_dir
    )

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
    data_capture_dir=None
    ):

    job_input=get_job_input(
        deploy_type, 
        endpoint_name=endpoint_name,
        rt_local_path='/opt/ml/processing/input/endpoint',
        bch_local_path='/opt/ml/processing/input',
        dataset_format=dataset_format,
        data_capture_dir=data_capture_dir,
        ground_truth_dir=ground_truth_dir
    )

    response = sm_client.create_model_bias_job_definition(
        JobDefinitionName=name,
        ModelBiasBaselineConfig={
            #'BaseliningJobName': 'string',
            "ConstraintsResource": {"S3Uri": f'{monitor_dir}/info/constraints.json'}
        },
        ModelBiasAppSpecification={
            'ImageUri': image_uri,
            'ConfigUri': f'{monitor_dir}/info/analysis_config.json',
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
    data_capture_dir=None
    ):

    job_input=get_job_input(
        deploy_type, 
        endpoint_name=endpoint_name,
        rt_local_path='/opt/ml/processing/input/endpoint',
        bch_local_path='/opt/ml/processing/input',
        dataset_format=dataset_format,
        data_capture_dir=data_capture_dir
    )

    response = sm_client.create_model_explainability_job_definition(
        JobDefinitionName=name,
        ModelExplainabilityBaselineConfig={
            #'BaseliningJobName': 'string',
            "ConstraintsResource": {"S3Uri": f'{monitor_dir}/info/analysis.json'}
        },
        ModelExplainabilityAppSpecification={
            'ImageUri': image_uri,
            'ConfigUri': f'{monitor_dir}/info/analysis_config.json',
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
    predict_label,
    ground_truth_dir,
    problem_type,
    image_uri="156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer",
    instance_count=1, 
    instance_type='ml.m5.large', 
    volume_size_in_gb=20, 
    max_runtime_in_seconds=1800,  
    dataset_format={'Csv': {'Header': True}}, # {'Csv':{'Header': True|False},'Json': {'Line': True|False}, Parquet': {}}
    endpoint_name=None, 
    data_capture_dir=None
):

    job_input=get_job_input(
        deploy_type, 
        endpoint_name=endpoint_name,
        rt_local_path='/opt/ml/processing/input/endpoint',
        inference_attribute=predict_label,
        bch_local_path='/opt/ml/processing/input',
        dataset_format=dataset_format,
        data_capture_dir=data_capture_dir,
        ground_truth_dir=ground_truth_dir
    )

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
    data_capture_dir=None
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
        data_capture_dir=data_capture_dir, 
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
    data_capture_dir=None
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
        data_capture_dir=data_capture_dir, 
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
    data_capture_dir=None
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
        data_capture_dir=data_capture_dir
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
    predict_label,
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
    data_capture_dir=None
):

    job_definition_name = f'{name}-job'

    response = create_model_quality_job_definition(        
        sm_client, 
        job_definition_name, 
        role_arn,
        deploy_type, 
        monitor_dir, 
        predict_label,
        ground_truth_dir,
        problem_type,
        image_uri=image_uri,
        instance_count=instance_count, 
        instance_type=instance_type, 
        volume_size_in_gb=volume_size_in_gb, 
        max_runtime_in_seconds=max_runtime_in_seconds,  
        dataset_format=dataset_format,
        endpoint_name=endpoint_name, 
        data_capture_dir=data_capture_dir
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
    data_capture_dir = event['data_capture_dir'] if 'data_capture_dir' in event else None
    name = event['name']
    role_arn = event['monitor_role']
    deploy_type = event['deploy_type']
    monitor_dir = event['monitor_dir']
    image_uri = event['image_uri'] if 'image_uri' in event else "156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer"
    instance_count = event['instance_count'] if 'instance_count' in event else 1
    instance_type = event['instance_type'] if 'instance_type' in event else 'ml.m5.large'
    volume_size_in_gb = event['volume_size_in_gb'] if 'volume_size_in_gb' in event else 20
    max_runtime_in_seconds = event['max_runtime_in_seconds'] if 'max_runtime_in_seconds' in event else 1800
    dataset_format = json.loads(event['dataset_format'] if 'dataset_format' in event else '{"Csv": {"Header": true}}'), # {'Csv':{'Header': True|False},'Json': {'Line': True|False}, Parquet': {}} 
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
        data_capture_dir=data_capture_dir
    )
    return {}


def model_bias_handler(event, context):
    monitoring_type='ModelBias'

    endpoint_name = event['endpoint_name'] if 'endpoint_name' in event else None
    data_capture_dir = event['data_capture_dir'] if 'data_capture_dir' in event else None
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
    dataset_format = json.loads(event['dataset_format'] if 'dataset_format' in event else '{"Csv": {"Header": true}}'), # {'Csv':{'Header': True|False},'Json': {'Line': True|False}, Parquet': {}} 
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
        data_capture_dir=data_capture_dir
    )
    return {}


def model_explainability_handler(event, context):
    monitoring_type='ModelExplainability'

    endpoint_name = event['endpoint_name'] if 'endpoint_name' in event else None
    data_capture_dir = event['data_capture_dir'] if 'data_capture_dir' in event else None
    name = event['name']
    role_arn = event['monitor_role']
    deploy_type = event['deploy_type']
    monitor_dir = event['monitor_dir']
    image_uri = event['image_uri'] if 'image_uri' in event else "156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer"
    instance_count = event['instance_count'] if 'instance_count' in event else 1
    instance_type = event['instance_type'] if 'instance_type' in event else 'ml.m5.large'
    volume_size_in_gb = event['volume_size_in_gb'] if 'volume_size_in_gb' in event else 20
    max_runtime_in_seconds = event['max_runtime_in_seconds'] if 'max_runtime_in_seconds' in event else 1800
    dataset_format = json.loads(event['dataset_format'] if 'dataset_format' in event else '{"Csv": {"Header": true}}'), # {'Csv':{'Header': True|False},'Json': {'Line': True|False}, Parquet': {}} 
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
        data_capture_dir=data_capture_dir
    )
    return {}


def model_quality_handler(event, context):
    monitoring_type='ModelQuality'

    endpoint_name = event['endpoint_name'] if 'endpoint_name' in event else None
    data_capture_dir = event['data_capture_dir'] if 'data_capture_dir' in event else None
    name = event['name']
    role_arn = event['monitor_role']
    deploy_type = event['deploy_type']
    problem_type = event['problem_type'] # 'BinaryClassification'|'MulticlassClassification'|'Regression'
    predict_label = event['predict_label']
    monitor_dir = event['monitor_dir']
    ground_truth_dir = event['ground_truth_dir']
    image_uri = event['image_uri'] if 'image_uri' in event else "156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer"
    instance_count = event['instance_count'] if 'instance_count' in event else 1
    instance_type = event['instance_type'] if 'instance_type' in event else 'ml.m5.large'
    volume_size_in_gb = event['volume_size_in_gb'] if 'volume_size_in_gb' in event else 20
    max_runtime_in_seconds = event['max_runtime_in_seconds'] if 'max_runtime_in_seconds' in event else 1800
    dataset_format = json.loads(event['dataset_format'] if 'dataset_format' in event else '{"Csv": {"Header": true}}'), # {'Csv':{'Header': True|False},'Json': {'Line': True|False}, Parquet': {}} 
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
        predict_label,
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
        data_capture_dir=data_capture_dir
    )
    return {}