import boto3, logging, json, time
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_job_input(deploy_type, **kwargs):
    job_input = {}

    if kwargs.get('features_attribute'): endpoint_input['FeaturesAttribute'] = "0"

    if problem_type == 'BinaryClassification': #|'MulticlassClassification'|'Regression'
        if kwargs.get('probability_attribute'): endpoint_input['ProbabilityAttribute'] = "0"
        if kwargs.get('inference_attribute'): endpoint_input['InferenceAttribute'] = "1"
    else:
        if kwargs.get('inference_attribute'): endpoint_input['InferenceAttribute'] = "0"




    if kwargs.get('inference_attribute'): endpoint_input['InferenceAttribute'] = "0"
    if kwargs.get('probability_attribute'): endpoint_input['ProbabilityAttribute'] = kwargs['probability_attribute']
    if kwargs.get('probability_threshold_attribute'): endpoint_input['ProbabilityThresholdAttribute'] = kwargs['probability_threshold_attribute']


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
def wait_for_schedule_deletion(sm_client, schedule_name, timeout_seconds=60, poll_interval=2):
    start = time.time()
    while time.time() - start < timeout_seconds:
        try:
            sm_client.describe_monitoring_schedule(MonitoringScheduleName=schedule_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFound":
                return True
            raise
        time.sleep(poll_interval)
    raise TimeoutError(f"{schedule_name} still exists after {timeout_seconds}s")

def delete_monitor(sm_client, endpoint_name, monitoring_type): # 'DataQuality':|'ModelQuality'|'ModelBias'|'ModelExplainability'
    schedules = sm_client.list_monitoring_schedules(EndpointName=endpoint_name)
    for schedule in schedules['MonitoringScheduleSummaries']:
        name=schedule['MonitoringScheduleName']
        logger.info(f"schedule: {name}")
        detail = sm_client.describe_monitoring_schedule(MonitoringScheduleName=name)
        logger.info(detail['MonitoringType'])
        if detail['MonitoringType'] == monitoring_type:
            logger.info(f"DELETING {detail['MonitoringType']} monitor: {name}")
            response = sm_client.delete_monitoring_schedule(MonitoringScheduleName=name)
            wait_for_schedule_deletion(sm_client, schedule['MonitoringScheduleName'], timeout_seconds=120, poll_interval=5)


##############################################
############### JOB DEFINITIONS ##############
##############################################

def create_data_quality_job_definition(        
    sm_client, 
    name, 
    role_arn,
    deploy_type,
    monitor_dir,
    image_uri,
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
    image_uri,
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
            "ConstraintsResource": {"S3Uri": f'{monitor_dir}/info/analysis.json'}
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
    image_uri,
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
    image_uri,
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
        ModelQualityJobInput={
            'EndpointInput': {
                'EndpointName': endpoint_name,
                'LocalPath': '/opt/ml/processing/input/endpoint',
                # 'S3InputMode': 'Pipe'|'File',
                # 'S3DataDistributionType': 'FullyReplicated'|'ShardedByS3Key',
                'FeaturesAttribute': 'string',
                'InferenceAttribute': 'string',
                'ProbabilityAttribute': 'string',
                'ProbabilityThresholdAttribute': 123.0,
                'StartTimeOffset': 'string',
                'EndTimeOffset': 'string',
                'ExcludeFeaturesAttribute': 'string'
            },
            'BatchTransformInput': {
                'DataCapturedDestinationS3Uri': data_capture_dir,
                'DatasetFormat': dataset_format,
                'LocalPath': '/opt/ml/processing/input',
                'S3InputMode': 'Pipe'|'File',
                'S3DataDistributionType': 'FullyReplicated'|'ShardedByS3Key',
                'FeaturesAttribute': 'string',
                'InferenceAttribute': 'string',
                'ProbabilityAttribute': 'string',
                'ProbabilityThresholdAttribute': 123.0,
                'StartTimeOffset': 'string',
                'EndTimeOffset': 'string',
                'ExcludeFeaturesAttribute': 'string'
            },
            'GroundTruthS3Input': {
                'S3Uri': 'string'
            }
        },
        # ModelQualityJobInput=job_input,
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

def get_sagemaker_monitor_analyzer_image_uri(region, version=""):
    img_data = { # https://github.com/aws/sagemaker-python-sdk/blob/7e7b6f18c26a460be2faec8a66953e6f710cef84/sagemaker-core/src/sagemaker/core/image_uri_config/model-monitor.json#L37
        "scope": [
            "monitoring"
        ],
        "versions": {
            "": {
                "registries": {
                    "af-south-1": "875698925577",
                    "ap-east-1": "001633400207",
                    "ap-northeast-1": "574779866223",
                    "ap-northeast-2": "709848358524",
                    "ap-northeast-3": "990339680094",
                    "ap-south-1": "126357580389",
                    "ap-southeast-1": "245545462676",
                    "ap-southeast-2": "563025443158",
                    "ap-southeast-3": "669540362728",
                    "ca-central-1": "536280801234",
                    "cn-north-1": "453000072557",
                    "cn-northwest-1": "453252182341",
                    "eu-central-1": "048819808253",
                    "eu-central-2": "590183933784",
                    "eu-north-1": "895015795356",
                    "eu-south-1": "933208885752",
                    "eu-south-2": "437450045455",
                    "eu-west-1": "468650794304",
                    "eu-west-2": "749857270468",
                    "eu-west-3": "680080141114",
                    "il-central-1": "843974653677",
                    "me-central-1": "588750061953",
                    "me-south-1": "607024016150",
                    "sa-east-1": "539772159869",
                    "us-east-1": "156813124566",
                    "us-east-2": "777275614652",
                    "us-isof-east-1": "853188333426",
                    "us-isof-south-1": "467912361380",
                    "us-west-1": "890145073186",
                    "us-west-2": "159807026194"
                },
                "repository": "sagemaker-model-monitor-analyzer"
            }
        }
    }
    acct = img_data['versions'][version]["registries"][region]
    repo = img_data['versions'][version]["repository"]
    uri = f"{acct}.dkr.ecr.{region}.amazonaws.com/{repo}:{version}"
    return uri.rstrip(":") if uri.endswith(":") else uri

def get_sagemaker_clarify_processor_image_uri(region, version="1.0"):
    img_data = { # https://github.com/aws/sagemaker-python-sdk/blob/7e7b6f18c26a460be2faec8a66953e6f710cef84/sagemaker-core/src/sagemaker/core/image_uri_config/clarify.json
        "processing": {
            "versions": {
                "1.0": {
                    "registries": {
                        "af-south-1": "811711786498",
                        "ap-east-1": "098760798382",
                        "ap-northeast-1": "377024640650",
                        "ap-northeast-2": "263625296855",
                        "ap-northeast-3": "912233562940",
                        "ap-south-1": "452307495513",
                        "ap-southeast-1": "834264404009",
                        "ap-southeast-2": "007051062584",
                        "ap-southeast-3": "705930551576",
                        "ca-central-1": "675030665977",
                        "cn-north-1": "122526803553",
                        "cn-northwest-1": "122578899357",
                        "eu-central-1": "017069133835",
                        "eu-central-2": "730335477804",
                        "eu-north-1": "763603941244",
                        "eu-south-1": "638885417683",
                        "eu-west-1": "131013547314",
                        "eu-west-2": "440796970383",
                        "eu-west-3": "341593696636",
                        "me-south-1": "835444307964",
                        "sa-east-1": "520018980103",
                        "us-east-1": "205585389593",
                        "us-east-2": "211330385671",
                        "us-gov-west-1": "598674086554",
                        "us-isof-east-1": "579539705040",
                        "us-isof-south-1": "411392592546",
                        "us-west-1": "740489534195",
                        "us-west-2": "306415355426"
                    },
                    "repository": "sagemaker-clarify-processing"
                }
            }
        }
    }
    acct = img_data['processing']['versions'][version]["registries"][region]
    repo = img_data['processing']['versions'][version]["repository"]
    uri = f"{acct}.dkr.ecr.{region}.amazonaws.com/{repo}:{version}"
    return uri.rstrip(":") if uri.endswith(":") else uri


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
    image_uri,
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
    image_uri,
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
    image_uri,
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
    image_uri,
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
    image_uri = event['image_uri'] if 'image_uri' in event else get_sagemaker_monitor_analyzer_image_uri('us-west-2')
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
    image_uri = event['image_uri'] if 'image_uri' in event else get_sagemaker_clarify_processor_image_uri('us-west-2', version="1.0")
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
    image_uri = event['image_uri'] if 'image_uri' in event else get_sagemaker_clarify_processor_image_uri('us-west-2', version="1.0")
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
    image_uri = event['image_uri'] if 'image_uri' in event else get_sagemaker_monitor_analyzer_image_uri('us-west-2')
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