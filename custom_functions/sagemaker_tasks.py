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



def get_baseline_transform_task(scope, model_name_lookup):
    tasks.SageMakerCreateTransformJob(scope, "Transform",
        # transform_job_name="MyTransformJob",
        model_name=stepfunctions.JsonPath.string_at(model_name_lookup),
        model_client_options=tasks.ModelClientOptions(
            invocations_max_retries=3,  # default is 0
            invocations_timeout=Duration.minutes(5)
        ),
        transform_input=tasks.TransformInput(
            transform_data_source=tasks.TransformDataSource(
                s3_data_source=tasks.TransformS3DataSource(
                    s3_uri=scope.baseline_file,
                    s3_data_type=tasks.S3DataType.S3_PREFIX
                )
            )
        ),
        transform_output=tasks.TransformOutput(
            s3_output_path=scope.baseline_dir
        ),
        transform_resources=tasks.TransformResources(
            instance_count=1,
            instance_type=ec2.InstanceType.of(instance_class(scope.transform_instance_type), instance_size(scope.transform_instance_type))
        )
    )

def get_batch_transform_task(scope, model_name_lookup, batch_input_dir_lookup):
    tasks.SageMakerCreateTransformJob(scope, "Transform",
        # transform_job_name="MyTransformJob",
        model_name=stepfunctions.JsonPath.string_at(model_name_lookup),
        model_client_options=tasks.ModelClientOptions(
            invocations_max_retries=3,  # default is 0
            invocations_timeout=Duration.minutes(5)
        ),
        transform_input=tasks.TransformInput(
            transform_data_source=tasks.TransformDataSource(
                s3_data_source=tasks.TransformS3DataSource(
                    s3_uri=stepfunctions.JsonPath.string_at(model_name_lookup),
                    s3_data_type=tasks.S3DataType.S3_PREFIX
                )
            )
        ),
        transform_output=tasks.TransformOutput(
            s3_output_path=scope.batch_out_dir
        ),
        transform_resources=tasks.TransformResources(
            instance_count=1,
            instance_type=ec2.InstanceType.of(instance_class(scope.transform_instance_type), instance_size(scope.transform_instance_type))
        )
    )