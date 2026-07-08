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

# helper to mirror what JsonPath.string_at() does under the hood —
# a dynamic field gets a ".$" suffix key pointing at a JSONPath string,
# a static field keeps its plain key and literal value
def _field(key, dynamic_path=None, static_value=None):
    return {f"{key}.$": dynamic_path} if dynamic_path else {key: static_value}

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
    
    return stepfunctions.CustomState(
        scope, construct_id,
        state_json={
            "Type": "Task",
            "Resource": "arn:aws:states:::sagemaker:createTransformJob.sync",
            "Parameters": {
                "TransformJobName.$": f"States.Format('{job_name}-{{}}', $$.Execution.Name)",
                "ModelName.$": model_name_lkp,
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
                    **_field("S3OutputPath", dynamic_path=transform_out_dir_lkp, static_value=transform_out_dir),
                },
                "TransformResources": {
                    "InstanceCount": instance_count,
                    "InstanceType.$": instance_type_lkp,
                },
            },
            "ResultPath": f"$.{construct_id}Task",
        },
    )

# def get_model_bias_check(
#         scope,
#         construct_id,
#         image_uri,
#         input_s3_uri,
# ):
#     model_bias_check = stepfunctions.CustomState(
#         scope, construct_id,
#         state_json={
#             "Type": "Task",
#             "Resource": "arn:aws:states:::sagemaker:createProcessingJob.sync",
#             "Parameters": {
#                 "ProcessingJobName.$": "States.Format('bias-check-{}', $$.Execution.Name)",
#                 "AppSpecification": {
#                     "ImageUri": image_uri
#                 },
#                 "ProcessingInputs": [
#                     {
#                         "InputName": "analysis_config",
#                         "S3Input": {
#                             "S3Uri.$": "$.biasAnalysisConfigS3Uri",
#                             "LocalPath": "/opt/ml/processing/input/config",
#                             "S3DataType": "S3Prefix",
#                             "S3InputMode": "File",
#                         },
#                     }
#                 ],
#                 "ProcessingOutputConfig": {
#                     "Outputs": [
#                         {
#                             "OutputName": "analysis_result",
#                             "S3Output": {
#                                 "S3Uri.$": "States.Format('s3://your-bucket/bias-output/{}', $$.Execution.Name)",
#                                 "LocalPath": "/opt/ml/processing/output",
#                                 "S3UploadMode": "EndOfJob",
#                             },
#                         }
#                     ]
#                 },
#                 "ProcessingResources": {
#                     "ClusterConfig": {
#                         "InstanceCount": 1,
#                         "InstanceType": "ml.m5.xlarge",
#                         "VolumeSizeInGB": 20,
#                     }
#                 },
#                 "RoleArn": clarify_role.role_arn,
#                 "StoppingCondition": {"MaxRuntimeInSeconds": 1800},
#             },
#             "ResultPath": "$.modelBiasResult",
#         },
#     )
    