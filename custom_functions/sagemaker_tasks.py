import os, pathlib
from aws_cdk import (
    Duration,
    aws_ec2 as ec2,
    aws_stepfunctions_tasks as tasks,
    aws_stepfunctions as stepfunctions
)
from custom_constructs.CNetwork import CNetwork
from custom_constructs.CLambda import CLambdaFunction
from custom_constructs.CECS import CFargateTaskDefinition
from custom_constructs.utils import get_local_project_root
def instance_class(instance):
    return instance.split('.')[1]

def instance_size(instance):
    return instance.split('.')[2]



def get_baseline_transform_task(scope, model_name_lkp, baseline_file_lkp, transform_instance_dtl_lkp):
    return tasks.SageMakerCreateTransformJob(scope, "Transform",
        transform_job_name="BaselineTransformJob",
        model_name=stepfunctions.JsonPath.string_at(model_name_lkp),
        model_client_options=tasks.ModelClientOptions(
            invocations_max_retries=3,  # default is 0
            invocations_timeout=Duration.minutes(5)
        ),
        transform_input=tasks.TransformInput(
            transform_data_source=tasks.TransformDataSource(
                s3_data_source=tasks.TransformS3DataSource(
                    s3_uri=stepfunctions.JsonPath.string_at(baseline_file_lkp),
                    s3_data_type=tasks.S3DataType.S3_PREFIX
                )
            )
        ),
        transform_output=tasks.TransformOutput(
            s3_output_path=scope.baseline_dir
        ),
        transform_resources=tasks.TransformResources(
            instance_count=1,
            instance_type=ec2.InstanceType.of(
                # stepfunctions.JsonPath.string_at(f'{transform_instance_dtl_lkp}.class'),
                # stepfunctions.JsonPath.string_at(f'{transform_instance_dtl_lkp}.type')
                ec2.InstanceClass.M5,
                ec2.InstanceSize.XLARGE
            )
        )
    )
    # Output
    # {
    #     "TransformJobName": "my-transform-job",
    #     "TransformJobArn": "arn:aws:sagemaker:us-east-1:...",
    #     "TransformJobStatus": "Completed",
    #     "ModelName": "abalone-v1",
    #     "TransformInput": {"DataSource": { "S3DataSource": { "S3DataType": "S3Prefix", "S3Uri": "s3://bucket/input/" } }, "ContentType": "text/csv", "SplitType": "Line" },
    #     "TransformOutput": { "S3OutputPath": "s3://bucket/output/", "AssembleWith": "Line" },
    #     "TransformResources": { "InstanceType": "ml.m5.large", "InstanceCount": 1 },
    #     "CreationTime": "...",
    #     "TransformStartTime": "...",
    #     "TransformEndTime": "..."
    # }

def get_batch_transform_task(scope, model_name_lkp, batch_input_dir_lkp, transform_instance_dtl_lkp):
    return tasks.SageMakerCreateTransformJob(scope, "Transform",
        transform_job_name="BatchTransformJob",
        model_name=stepfunctions.JsonPath.string_at(model_name_lkp),
        model_client_options=tasks.ModelClientOptions(
            invocations_max_retries=3,  # default is 0
            invocations_timeout=Duration.minutes(5)
        ),
        transform_input=tasks.TransformInput(
            transform_data_source=tasks.TransformDataSource(
                s3_data_source=tasks.TransformS3DataSource(
                    s3_uri=stepfunctions.JsonPath.string_at(batch_input_dir_lkp),
                    s3_data_type=tasks.S3DataType.S3_PREFIX
                )
            )
        ),
        transform_output=tasks.TransformOutput(
            s3_output_path=scope.batch_out_dir
        ),
        transform_resources=tasks.TransformResources(
            instance_count=1,
            instance_type=ec2.InstanceType.of(
                # stepfunctions.JsonPath.string_at(f'{transform_instance_dtl_lkp}.class'),
                # stepfunctions.JsonPath.string_at(f'{transform_instance_dtl_lkp}.type')
                exec(f'ec2.InstanceClass.M5'),
                exec(f'ec2.InstanceSize.XLARGE')
            )
        )
    )
# ec2.InstanceType.constructor
#     exec(f'ec2.InstanceClass.M5'),
#     exec(f'ec2.InstanceSize.XLARGE')
#     exec(f'ec2.InstanceClass.{stepfunctions.JsonPath.string_at(f'{transform_instance_dtl_lkp}.class')}'),
#     exec(f'ec2.InstanceSize.{stepfunctions.JsonPath.string_at(f'{transform_instance_dtl_lkp}.type')}')

# instance_type=ec2.InstanceType.of(
#     exec(f'ec2.InstanceClass.{stepfunctions.JsonPath.string_at(f'{transform_instance_dtl_lkp}.class')}'),
#     exec(f'ec2.InstanceSize.{stepfunctions.JsonPath.string_at(f'{transform_instance_dtl_lkp}.type')}')
# )
    # Output
    # {
    #     "TransformJobName": "my-transform-job",
    #     "TransformJobArn": "arn:aws:sagemaker:us-east-1:...",
    #     "TransformJobStatus": "Completed",
    #     "ModelName": "abalone-v1",
    #     "TransformInput": {"DataSource": { "S3DataSource": { "S3DataType": "S3Prefix", "S3Uri": "s3://bucket/input/" } }, "ContentType": "text/csv", "SplitType": "Line" },
    #     "TransformOutput": { "S3OutputPath": "s3://bucket/output/", "AssembleWith": "Line" },
    #     "TransformResources": { "InstanceType": "ml.m5.large", "InstanceCount": 1 },
    #     "CreationTime": "...",
    #     "TransformStartTime": "...",
    #     "TransformEndTime": "..."
    # }