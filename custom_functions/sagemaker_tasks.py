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
    create_transform_job_step = tasks.CallAwsService(
        scope, construct_id,
        service='sagemaker',
        action='createTransformJob',
        parameters={
            'TransformJobName': stepfunctions.JsonPath.format(f'{job_name}-{{}}', stepfunctions.JsonPath.string_at('$$.Execution.Name')),
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
        parameters={ 'TransformJobName': stepfunctions.JsonPath.format(f'{job_name}-{{}}', stepfunctions.JsonPath.string_at('$$.Execution.Name')) },
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