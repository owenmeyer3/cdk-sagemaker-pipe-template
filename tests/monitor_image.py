import boto3
import json

sm_client = boto3.client('sagemaker', region_name='us-east-1')

# response = sm_client.create_processing_job(
#     ProcessingJobName='inspect-model-monitor-container',
#     AppSpecification={
#         'ImageUri': '156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer',
#         'ContainerEntrypoint': ['/bin/bash', '-c'],
#         'ContainerArguments': [
#             'find / -not -path "*/proc/*" -not -path "*/sys/*" 2>/dev/null && env'
#         ]
#     },
#     ProcessingResources={
#         'ClusterConfig': {
#             'InstanceCount': 1,
#             'InstanceType': 'ml.m5.large',
#             'VolumeSizeInGB': 20
#         }
#     },
#     ProcessingOutputConfig={
#         'Outputs': [
#             {
#                 'OutputName': 'inspection_output',
#                 'S3Output': {
#                     'S3Uri': 's3://omm-test-bucket/pipelines/abalone/container-inspect',
#                     'LocalPath': '/opt/ml/processing/output',
#                     'S3UploadMode': 'EndOfJob'
#                 }
#             }
#         ]
#     },
#     RoleArn="arn:aws:iam::088461143167:role/SageMakerExecutionRole-1",
#     StoppingCondition={
#         'MaxRuntimeInSeconds': 300
#     }
# )

# print(response)


# response = sm_client.create_processing_job(
#     ProcessingJobName='inspect-model-monitor-container-2',
#     AppSpecification={
#         'ImageUri': '156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer',
#         'ContainerEntrypoint': ['/bin/bash', '-c'],
#         'ContainerArguments': [
#             'cat /opt/program/bootstrap.py && echo "---" && cat /opt/program/analysis/analytics_input.py && echo "---" && cat /opt/program/script_framework/preprocessor_entrypoint.py && echo "---" && cat /opt/program/analysis/default_data_analyzer.py'
#         ]
#     },
#     ProcessingResources={
#         'ClusterConfig': {
#             'InstanceCount': 1,
#             'InstanceType': 'ml.m5.large',
#             'VolumeSizeInGB': 20
#         }
#     },
#     ProcessingOutputConfig={
#         'Outputs': [
#             {
#                 'OutputName': 'inspection_output',
#                 'S3Output': {
#                     'S3Uri': 's3://omm-test-bucket/pipelines/abalone/container-inspect',
#                     'LocalPath': '/opt/ml/processing/output',
#                     'S3UploadMode': 'EndOfJob'
#                 }
#             }
#         ]
#     },
#     RoleArn="arn:aws:iam::088461143167:role/SageMakerExecutionRole-1",
#     StoppingCondition={
#         'MaxRuntimeInSeconds': 300
#     }
# )


# response = sm_client.create_processing_job(
#     ProcessingJobName='inspect-model-monitor-container-4',
#     AppSpecification={
#         'ImageUri': '156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer',
#         'ContainerEntrypoint': ['/bin/bash', '-c'],
#         'ContainerArguments': [
#             'cat /opt/program/data_analyzer.py && echo "---" && cat /opt/amazon/program/analysis/data_analyzer.py && echo "---" && find /opt/program -name "*.sh" && find /opt/program -name "entrypoint*" && cat /opt/ml/config/processingjobconfig.json 2>/dev/null'
#         ]
#     },
#     ProcessingResources={
#         'ClusterConfig': {
#             'InstanceCount': 1,
#             'InstanceType': 'ml.m5.large',
#             'VolumeSizeInGB': 20
#         }
#     },
#     ProcessingOutputConfig={
#         'Outputs': [{
#             'OutputName': 'inspection_output',
#             'S3Output': {
#                 'S3Uri': 's3://omm-test-bucket/pipelines/abalone/container-inspect',
#                 'LocalPath': '/opt/ml/processing/output',
#                 'S3UploadMode': 'EndOfJob'
#             }
#         }]
#     },
#     RoleArn="arn:aws:iam::088461143167:role/SageMakerExecutionRole-1",
#     StoppingCondition={'MaxRuntimeInSeconds': 300}
# )


response = sm_client.create_processing_job(
    ProcessingJobName='inspect-model-monitor-container-9',
    AppSpecification={
        'ImageUri': '156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer',
        'ContainerEntrypoint': ['/bin/bash', '-c'],
        'ContainerArguments': [
            'cat /opt/program/analyze'
        ]
    },
    ProcessingResources={
        'ClusterConfig': {
            'InstanceCount': 1,
            'InstanceType': 'ml.m5.large',
            'VolumeSizeInGB': 20
        }
    },
    ProcessingOutputConfig={
        'Outputs': [{
            'OutputName': 'inspection_output',
            'S3Output': {
                'S3Uri': 's3://omm-test-bucket/pipelines/abalone/container-inspect',
                'LocalPath': '/opt/ml/processing/output',
                'S3UploadMode': 'EndOfJob'
            }
        }]
    },
    RoleArn="arn:aws:iam::088461143167:role/SageMakerExecutionRole-1",
    StoppingCondition={'MaxRuntimeInSeconds': 300}
)


def create_data_quality_baseline_job(
    sm_client,
    name,
    role_arn,
    monitor_dir,
    baseline_dataset_uri,
    image_uri="156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer",
    instance_count=1,
    instance_type='ml.m5.large',
    volume_size_in_gb=20,
    max_runtime_in_seconds=1800,
    dataset_format={'csv': {'header': True}},
):
    # Usage
    # create_data_quality_baseline_job(
    #     sm_client=sm_client,
    #     name='abalone-data-quality-baseline',
    #     role_arn=role_arn,
    #     monitor_dir='s3://omm-test-bucket/pipelines/abalone/data-quality-monitor',
    #     baseline_dataset_uri='s3://omm-test-bucket/pipelines/abalone/data/training/',
    # )
    response = sm_client.create_processing_job(
        ProcessingJobName=name,
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
        Environment={
            'dataset_source': baseline_dataset_uri,
            'output_path': f'{monitor_dir}/info',
            'dataset_format': json.dumps(dataset_format),
            'analysis_type': 'DATA_QUALITY',
            'publish_cloudwatch_metrics': 'Disabled'
        },
        RoleArn=role_arn,
        StoppingCondition={
            'MaxRuntimeInSeconds': max_runtime_in_seconds
        }
    )
    return response


def create_model_quality_baseline_job(
    sm_client,
    name,
    role_arn,
    monitor_dir,
    baseline_dataset_uri,
    problem_type,                    # 'BinaryClassification'|'MulticlassClassification'|'Regression'
    inference_attribute,             # column name of model prediction in dataset
    ground_truth_attribute,          # column name of true label in dataset
    image_uri="156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer",
    instance_count=1,
    instance_type='ml.m5.large',
    volume_size_in_gb=20,
    max_runtime_in_seconds=1800,
    dataset_format={'csv': {'header': True}},
    probability_attribute=None,      # column name of prediction probability (classification only)
    probability_threshold_attribute=None,  # threshold for binary classification
    positive_label=None,             # positive class label (binary classification only)
):
    environment = {
        'dataset_source': baseline_dataset_uri,
        'output_path': f'{monitor_dir}/info',
        'dataset_format': json.dumps(dataset_format),
        'analysis_type': 'MODEL_QUALITY',
        'problem_type': problem_type,
        'inference_attribute': inference_attribute,
        'ground_truth_attribute': ground_truth_attribute,
        'publish_cloudwatch_metrics': 'Disabled'
    }
    if probability_attribute: environment['probability_attribute'] = probability_attribute
    if probability_threshold_attribute: environment['probability_threshold_attribute'] = str(probability_threshold_attribute)
    if positive_label: environment['positive_label'] = positive_label

    response = sm_client.create_processing_job(
        ProcessingJobName=name,
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
    return response


def create_model_bias_baseline_job(
    sm_client,
    name,
    role_arn,
    monitor_dir,
    baseline_dataset_uri,
    problem_type,                    # 'BinaryClassification'|'MulticlassClassification'|'Regression'
    inference_attribute,             # column name of model prediction in dataset
    ground_truth_attribute,          # column name of true label in dataset
    image_uri="156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer",
    instance_count=1,
    instance_type='ml.m5.large',
    volume_size_in_gb=20,
    max_runtime_in_seconds=1800,
    dataset_format={'csv': {'header': True}},
    probability_attribute=None,      # column name of prediction probability (classification only)
    probability_threshold_attribute=None,  # threshold for binary classification
    positive_label=None,             # positive class label (binary classification only)
    exclude_features_attribute=None, # comma separated list of features to exclude from bias analysis
):
    environment = {
        'dataset_source': baseline_dataset_uri,
        'output_path': f'{monitor_dir}/info',
        'dataset_format': json.dumps(dataset_format),
        'analysis_type': 'BIAS',
        'problem_type': problem_type,
        'inference_attribute': inference_attribute,
        'ground_truth_attribute': ground_truth_attribute,
        'publish_cloudwatch_metrics': 'Disabled'
    }
    if probability_attribute: environment['probability_attribute'] = probability_attribute
    if probability_threshold_attribute: environment['probability_threshold_attribute'] = str(probability_threshold_attribute)
    if positive_label: environment['positive_label'] = positive_label
    if exclude_features_attribute: environment['exclude_features_attribute'] = exclude_features_attribute

    response = sm_client.create_processing_job(
        ProcessingJobName=name,
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
    return response


def create_model_quality_baseline_job(
    sm_client,
    name,
    role_arn,
    monitor_dir,
    baseline_dataset_uri,
    problem_type,                    # 'BinaryClassification'|'MulticlassClassification'|'Regression'
    inference_attribute,             # column name of model prediction in dataset
    ground_truth_attribute,          # column name of true label in dataset
    image_uri="156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer",
    instance_count=1,
    instance_type='ml.m5.large',
    volume_size_in_gb=20,
    max_runtime_in_seconds=1800,
    dataset_format={'csv': {'header': True}},
    probability_attribute=None,      # column name of prediction probability (classification only)
    probability_threshold_attribute=None,  # threshold for binary classification
    positive_label=None,             # positive class label (binary classification only)
):
    environment = {
        'dataset_source': baseline_dataset_uri,
        'output_path': f'{monitor_dir}/info',
        'dataset_format': json.dumps(dataset_format),
        'analysis_type': 'MODEL_QUALITY',
        'problem_type': problem_type,
        'inference_attribute': inference_attribute,
        'ground_truth_attribute': ground_truth_attribute,
        'publish_cloudwatch_metrics': 'Disabled'
    }
    if probability_attribute: environment['probability_attribute'] = probability_attribute
    if probability_threshold_attribute: environment['probability_threshold_attribute'] = str(probability_threshold_attribute)
    if positive_label: environment['positive_label'] = positive_label

    response = sm_client.create_processing_job(
        ProcessingJobName=name,
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
    return response


def create_model_bias_baseline_job(
    sm_client,
    name,
    role_arn,
    monitor_dir,
    baseline_dataset_uri,
    problem_type,                    # 'BinaryClassification'|'MulticlassClassification'|'Regression'
    inference_attribute,             # column name of model prediction in dataset
    ground_truth_attribute,          # column name of true label in dataset
    image_uri="156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer",
    instance_count=1,
    instance_type='ml.m5.large',
    volume_size_in_gb=20,
    max_runtime_in_seconds=1800,
    dataset_format={'csv': {'header': True}},
    probability_attribute=None,      # column name of prediction probability (classification only)
    probability_threshold_attribute=None,  # threshold for binary classification
    positive_label=None,             # positive class label (binary classification only)
    exclude_features_attribute=None, # comma separated list of features to exclude from bias analysis
):
    environment = {
        'dataset_source': baseline_dataset_uri,
        'output_path': f'{monitor_dir}/info',
        'dataset_format': json.dumps(dataset_format),
        'analysis_type': 'BIAS',
        'problem_type': problem_type,
        'inference_attribute': inference_attribute,
        'ground_truth_attribute': ground_truth_attribute,
        'publish_cloudwatch_metrics': 'Disabled'
    }
    if probability_attribute: environment['probability_attribute'] = probability_attribute
    if probability_threshold_attribute: environment['probability_threshold_attribute'] = str(probability_threshold_attribute)
    if positive_label: environment['positive_label'] = positive_label
    if exclude_features_attribute: environment['exclude_features_attribute'] = exclude_features_attribute

    response = sm_client.create_processing_job(
        ProcessingJobName=name,
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
    return response


def create_model_explainability_baseline_job(
    sm_client,
    name,
    role_arn,
    monitor_dir,
    baseline_dataset_uri,
    image_uri="156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer",
    instance_count=1,
    instance_type='ml.m5.large',
    volume_size_in_gb=20,
    max_runtime_in_seconds=1800,
    dataset_format={'csv': {'header': True}},
    inference_attribute=None,        # column name of model prediction in dataset
    probability_attribute=None,      # column name of prediction probability (classification only)
    exclude_features_attribute=None, # comma separated list of features to exclude
):
    environment = {
        'dataset_source': baseline_dataset_uri,
        'output_path': f'{monitor_dir}/info',
        'dataset_format': json.dumps(dataset_format),
        'analysis_type': 'EXPLAINABILITY',
        'publish_cloudwatch_metrics': 'Disabled'
    }
    if inference_attribute: environment['inference_attribute'] = inference_attribute
    if probability_attribute: environment['probability_attribute'] = probability_attribute
    if exclude_features_attribute: environment['exclude_features_attribute'] = exclude_features_attribute

    response = sm_client.create_processing_job(
        ProcessingJobName=name,
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
    return response