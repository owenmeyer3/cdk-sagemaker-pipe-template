import os, pathlib, datetime, json
from aws_cdk import (
    Duration,
    aws_ec2 as ec2,
    aws_stepfunctions_tasks as tasks,
    aws_stepfunctions as stepfunctions,
    aws_logs as logs
)
from custom_constructs.CNetwork import Network
from custom_constructs.utils import get_local_project_root

_MISSING = object()

# helper to mirror what JsonPath.string_at() does under the hood —
# a dynamic field gets a ".$" suffix key pointing at a JSONPath string,
# a static field keeps its plain key and literal value
# '**' can change a dict to keywords OR in this case merge dicts like:
# >>> print({**{'a': 'b'}, 'c': 'd'})
#         {'a': 'b', 'c': 'd'}
def _field(key, dynamic_path=None, static_value=None):
    return {f"{key}.$": dynamic_path} if dynamic_path else {key: static_value}

def _monitor_file_field(key, filename, dynamic_path=None, static_value=None):
    return {f"{key}.$": f"States.Format('{{}}/{filename}', {dynamic_path})"} if dynamic_path else {key: f"{static_value}/{filename}"}

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


class ClarifyCheckTask(stepfunctions.CustomState):
    def __init__(  
        self,  
        scope,
        construct_id,
        job_name,
        analysis_config_dir,
        image_region='us-east-1',
        role=None,              role_arn=None, 
        dataset_lkp=None,       dataset=None, 
        s3_out_dir_lkp=None,    s3_out_dir=None,
        instance_type_lkp=None, instance_type=None,
        instance_count=1,
        volume_size_in_gb=20,
        max_runtime_in_seconds=1800
    ):
        super().__init__(
            scope, construct_id,
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::sagemaker:createProcessingJob.sync",
                "Parameters": {
                    "ProcessingJobName.$": f"States.Format('{job_name}-{{}}-{{}}', $$.Execution.Name, $$.State.RetryCount)",
                    "AppSpecification": {
                        "ImageUri": get_sagemaker_clarify_processor_image_uri(image_region)
                    },
                    "ProcessingInputs": [
                        {
                            "InputName": "analysis_config",
                            "S3Input": {
                                "S3Uri":f'{analysis_config_dir}/analysis_config.json', # this file decides the monitor type
                                # **_monitor_file_field("S3Uri", "analysis_config.json", dynamic_path=monitor_dir, static_value=dataset), # this file decides the monitor type
                                "LocalPath": "/opt/ml/processing/input/config",
                                "S3DataType": "S3Prefix",
                                "S3InputMode": "File",
                            },
                        },
                        {
                            "InputName": "dataset",
                            "S3Input": {
                                **_field("S3Uri", dynamic_path=dataset_lkp, static_value=dataset),
                                "LocalPath": "/opt/ml/processing/input/data",
                                "S3DataType": "S3Prefix",
                                "S3InputMode": "File",
                            },
                        },
                    ],
                    "ProcessingOutputConfig": {
                        "Outputs": [
                            {
                                "OutputName": "analysis_result",
                                "S3Output": {
                                    "S3Uri.$": f"States.Format('{{}}/{{}}', {s3_out_dir_lkp}, $$.Execution.Name)" if s3_out_dir_lkp else f"States.Format('{s3_out_dir}/{{}}', $$.Execution.Name)",
                                    "LocalPath": "/opt/ml/processing/output",
                                    "S3UploadMode": "EndOfJob",
                                },
                            }
                        ]
                    },
                    "ProcessingResources": {
                        "ClusterConfig": {
                            "InstanceCount": instance_count,
                            **_field("InstanceType", dynamic_path=instance_type_lkp, static_value=instance_type),
                            "VolumeSizeInGB": volume_size_in_gb,
                        }
                    },
                    "RoleArn": role_arn if role_arn else role.role_arn,
                    "StoppingCondition": {"MaxRuntimeInSeconds": max_runtime_in_seconds},
                },
                "Retry": [ {
                    "ErrorEquals": ["States.TaskFailed"],
                    "IntervalSeconds": 10,
                    "MaxAttempts": 5,
                    "BackoffRate": 2.0
                } ],
                "ResultPath": f"$.{construct_id}Task",
            },
        )

class DataQualityCheckTask(stepfunctions.CustomState):
    def __init__(
        self,  
        scope,
        construct_id,
        job_name,
        image_region='us-east-1',
        role=None,                    role_arn=None,
        dataset_lkp=None,             dataset=None,          # baseline / merged predictions+labels dataset
        dataset_format_json='{"csv": {"header": true, "output_columns_position": "START"}}',
        s3_out_dir_lkp=None,          s3_out_dir=None,
        instance_type_lkp=None,       instance_type=None,
        instance_count=1,
        volume_size_in_gb=20,
        max_runtime_in_seconds=1800,
        publish_cloudwatch_metrics="Disabled",
    ):
        # this monitor conputes stats on all columns, so it doesnt matter which are features
        # we leave target and predictions out since we dnt care to monitor target/pred drift
        super().__init__(
            scope, construct_id,
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::sagemaker:createProcessingJob.sync",
                "Parameters": {
                    "ProcessingJobName.$": f"States.Format('{job_name}-{{}}-{{}}', $$.Execution.Name, $$.State.RetryCount)",
                    "AppSpecification": {
                        "ImageUri": get_sagemaker_monitor_analyzer_image_uri(image_region)
                    },
                    "Environment": {
                        "dataset_format": dataset_format_json,
                        "dataset_source": "/opt/ml/processing/input/baseline_dataset_input",
                        "output_path": "/opt/ml/processing/output",
                        "publish_cloudwatch_metrics": publish_cloudwatch_metrics,
                        # "analysis_type": "DATA_QUALITY", omitting this means DATA_QUALITY. Specifying it means MODEL_QUALITY
                    },
                    "ProcessingInputs": [
                        {
                            "InputName": "baseline_dataset_input",
                            "S3Input": {
                                **_field("S3Uri", dynamic_path=dataset_lkp, static_value=dataset),
                                "LocalPath": "/opt/ml/processing/input/baseline_dataset_input",
                                "S3DataType": "S3Prefix",
                                "S3InputMode": "File",
                            },
                        }
                    ],
                    "ProcessingOutputConfig": {
                        "Outputs": [
                            {
                                "OutputName": "monitoring_output",
                                "S3Output": {
                                    "S3Uri.$": f"States.Format('{{}}/{{}}', {s3_out_dir_lkp}, $$.Execution.Name)" if s3_out_dir_lkp else f"States.Format('{s3_out_dir}/{{}}', $$.Execution.Name)",
                                    "LocalPath": "/opt/ml/processing/output",
                                    "S3UploadMode": "EndOfJob",
                                },
                            }
                        ]
                    },
                    "ProcessingResources": {
                        "ClusterConfig": {
                            "InstanceCount": instance_count,
                            **_field("InstanceType", dynamic_path=instance_type_lkp, static_value=instance_type),
                            "VolumeSizeInGB": volume_size_in_gb,
                        }
                    },
                    "RoleArn": role_arn if role_arn else role.role_arn,
                    "StoppingCondition": {"MaxRuntimeInSeconds": max_runtime_in_seconds},
                },
                "Retry": [ {
                    "ErrorEquals": ["States.TaskFailed"],
                    "IntervalSeconds": 10,
                    "MaxAttempts": 5,
                    "BackoffRate": 2.0
                } ],
                "ResultPath": f"$.{construct_id}Task",
            },
        )


class ModelQualityCheckTask(stepfunctions.CustomState):
    def __init__(
        self,  
        scope,
        construct_id,
        job_name,
        problem_type, # 'BinaryClassification', 'MulticlassClassification', 'Regression'
        image_region='us-east-1',
        role=None,                    role_arn=None,
        dataset_lkp=None,             dataset=None,          # baseline / merged predictions+labels dataset
        dataset_format_json='{"csv": {"header": true, "output_columns_position": "START"}}',
        s3_out_dir_lkp=None,          s3_out_dir=None,
        instance_type_lkp=None,       instance_type=None,
        instance_count=1,
        volume_size_in_gb=20,
        max_runtime_in_seconds=1800,
        publish_cloudwatch_metrics="Disabled",
        inference_attribute=_MISSING,
        probability_attribute=_MISSING,
        probability_threshold_attribute=_MISSING,
        ground_truth_attribute=_MISSING,
    ):    
        env = {
            "dataset_format": dataset_format_json,
            "dataset_source": "/opt/ml/processing/input/baseline_dataset_input",
            "output_path": "/opt/ml/processing/output",
            "publish_cloudwatch_metrics": publish_cloudwatch_metrics,
            "analysis_type": "MODEL_QUALITY",
            "problem_type":problem_type
        }

        if inference_attribute is not _MISSING: env["inference_attribute"] = inference_attribute
        if probability_attribute is not _MISSING: env["probability_attribute"] = probability_attribute
        if probability_threshold_attribute is not _MISSING: env["probability_threshold_attribute"] = str(probability_threshold_attribute)
        if ground_truth_attribute is not _MISSING: env["ground_truth_attribute"] = ground_truth_attribute
        # this monitors does not include features

        super().__init__(
            scope, construct_id,
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::sagemaker:createProcessingJob.sync",
                "Parameters": {
                    "ProcessingJobName.$": f"States.Format('{job_name}-{{}}-{{}}', $$.Execution.Name, $$.State.RetryCount)",
                    "AppSpecification": {
                        "ImageUri": get_sagemaker_monitor_analyzer_image_uri(image_region)
                    },
                    "Environment": env,
                    "ProcessingInputs": [
                        {
                            "InputName": "baseline_dataset_input",
                            "S3Input": {
                                **_field("S3Uri", dynamic_path=dataset_lkp, static_value=dataset),
                                "LocalPath": "/opt/ml/processing/input/baseline_dataset_input",
                                "S3DataType": "S3Prefix",
                                "S3InputMode": "File",
                            },
                        }
                    ],
                    "ProcessingOutputConfig": {
                        "Outputs": [
                            {
                                "OutputName": "monitoring_output",
                                "S3Output": {
                                    "S3Uri.$": f"States.Format('{{}}/{{}}', {s3_out_dir_lkp}, $$.Execution.Name)" if s3_out_dir_lkp else f"States.Format('{s3_out_dir}/{{}}', $$.Execution.Name)",
                                    "LocalPath": "/opt/ml/processing/output",
                                    "S3UploadMode": "EndOfJob",
                                },
                            }
                        ]
                    },
                    "ProcessingResources": {
                        "ClusterConfig": {
                            "InstanceCount": instance_count,
                            **_field("InstanceType", dynamic_path=instance_type_lkp, static_value=instance_type),
                            "VolumeSizeInGB": volume_size_in_gb,
                        }
                    },
                    "RoleArn": role_arn if role_arn else role.role_arn,
                    "StoppingCondition": {"MaxRuntimeInSeconds": max_runtime_in_seconds},
                },
                "Retry": [ {
                    "ErrorEquals": ["States.TaskFailed"],
                    "IntervalSeconds": 10,
                    "MaxAttempts": 5,
                    "BackoffRate": 2.0
                } ],
                "ResultPath": f"$.{construct_id}Task",
            },
        )


class TransformTask(stepfunctions.CustomState):
    def __init__(
        self,  
        scope, 
        construct_id, 
        job_name,
        model_name_lkp=None,    model_name=None, 
        instance_type_lkp=None, instance_type=None,
        s3_data_source=None,    s3_data_source_lkp=None, 
        s3_out_dir=None,        s3_out_dir_lkp=None,
        instance_count=1,
        max_retries=3,
        timeout_in_seconds=300,
        content_type='text/csv',
        split_type='Line', assemble_with='Line',
        join_source='Input', # e.g. 'Input' - joins input cols + prediction into each output line
        output_filter='$' # only relevant when join_source is set
    ):
        parameters={
            "TransformJobName.$": f"States.Format('{job_name}-{{}}-{{}}', $$.Execution.Name, $$.State.RetryCount)",
            **_field("ModelName", dynamic_path=model_name_lkp, static_value=model_name),
            "ModelClientConfig": {
                "InvocationsMaxRetries": max_retries,
                "InvocationsTimeoutInSeconds": timeout_in_seconds,
            },
            "TransformInput": {
                "DataSource": {
                    "S3DataSource": {
                        **_field("S3Uri", dynamic_path=s3_data_source_lkp, static_value=s3_data_source),
                        "S3DataType": "S3Prefix",
                    }
                },
                "ContentType": content_type,
                "SplitType": split_type,
            },
            "TransformOutput": {
                "S3OutputPath.$": f"States.Format('{{}}/{{}}', {s3_out_dir_lkp}, $$.Execution.Name)" if s3_out_dir_lkp else f"States.Format('{s3_out_dir}/{{}}', $$.Execution.Name)",
                "Accept": content_type,
                "AssembleWith": assemble_with
            },
            "TransformResources": {
                "InstanceCount": instance_count,
                **_field("InstanceType", dynamic_path=instance_type_lkp, static_value=instance_type),
            }
        }
        
        if join_source is not None:
            parameters["DataProcessing"] = {"JoinSource": join_source, "OutputFilter": output_filter}

        source_json={
            "Type": "Task",
            "Resource": "arn:aws:states:::sagemaker:createTransformJob.sync",
            "Parameters":parameters,
            "ResultPath": f"$.{construct_id}Task",
        }

        super().__init__(
            scope, construct_id,
            state_json=source_json,
        )