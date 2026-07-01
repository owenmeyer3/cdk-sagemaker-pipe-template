spark_image='173754725891.dkr.ecr.us-east-1.amazonaws.com/sagemaker-spark-processing:3.5-cpu-py312-v1.4'

from sagemaker.model_monitor import DefaultModelMonitor
from sagemaker.model_monitor.dataset_format import DatasetFormat
def create_dq_baseline(role, baseline_dataset, output_s3_uri):
    my_default_monitor = DefaultModelMonitor(
        role=role,
        instance_count=1,
        instance_type='ml.m5.xlarge',
        volume_size_in_gb=20,
        max_runtime_in_seconds=3600,
    )

    my_default_monitor.suggest_baseline(
        baseline_dataset=baseline_dataset,
        dataset_format=DatasetFormat.csv(header=True),
        output_s3_uri=output_s3_uri,
        wait=True
    )


from sagemaker.core.resources import ProcessingJob

processing_job = ProcessingJob.create(
    processing_job_name="sklearn-processing",
    role_arn=role,
    app_specification={
        "image_uri": "sklearn-processing-image-uri",
        "container_entrypoint": ["python3", "/opt/ml/processing/input/code/preprocessing.py"]
    },
    processing_resources={
        "cluster_config": {
            "instance_count": 1,
            "instance_type": "ml.m5.xlarge",
            "volume_size_in_gb": 30
        }
    },
    processing_inputs=[
        {
            "input_name": "code",
            "s3_input": {
                "s3_uri": "s3://path/to/preprocessing.py",
                "local_path": "/opt/ml/processing/input/code",
                "s3_data_type": "S3Prefix",
                "s3_input_mode": "File"
            }
        },
        {
            "input_name": "input-data",
            "s3_input": {
                "s3_uri": "s3://path/to/my/input-data.csv",
                "local_path": "/opt/ml/processing/input",
                "s3_data_type": "S3Prefix",
                "s3_input_mode": "File"
            }
        }
    ],
    processing_output_config={
        "outputs": [
            {"output_name": "train", "s3_output": {"s3_uri": "s3://output/train", "local_path": "/opt/ml/processing/output/train", "s3_upload_mode": "EndOfJob"}},
            {"output_name": "validation", "s3_output": {"s3_uri": "s3://output/validation", "local_path": "/opt/ml/processing/output/validation", "s3_upload_mode": "EndOfJob"}},
            {"output_name": "test", "s3_output": {"s3_uri": "s3://output/test", "local_path": "/opt/ml/processing/output/test", "s3_upload_mode": "EndOfJob"}}
        ]
    }
)

    return tasks.CallAwsService(
        scope, 
        construct_id,
        service='sagemaker',
        action='createProcessingJob',
        parameters={
            'ProcessingJobName': f"{job_name}-{execution_id}",
            'ProcessingResources': {
                'ClusterConfig': {
                    'InstanceCount': instance_count,
                    'InstanceType': stepfunctions.JsonPath.string_at(instance_type_lkp),
                    'VolumeSizeInGB': volume_size
                }
            },
            'AppSpecification': {
                "image_uri": "sklearn-processing-image-uri",
                "container_entrypoint": ["python3", "/opt/ml/processing/input/code/preprocessing.py"]
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