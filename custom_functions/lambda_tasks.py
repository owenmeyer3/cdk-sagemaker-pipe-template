import os, pathlib, json
from aws_cdk import (
    aws_logs as logs,
    aws_stepfunctions as stepfunctions
)
from custom_constructs.CNetwork import CNetwork
from custom_constructs.CLambda import CLambdaFunction
from custom_constructs.CECS import CFargateTaskDefinition
from custom_constructs.utils import get_local_project_root

def sm_transform_fn_task(scope, construct_id, function_name, model_name_lkp, instance_type_lkp, s3_data_source=None, s3_data_source_lkp=None, transform_out_dir=None, transform_out_dir_lkp=None):
    lambda_function = CLambdaFunction(
        scope, construct_id,
        use_docker=False,
        function_name=function_name,
        code_path='code/sagemaker',
        handler='sagemaker.transform_job_handler',
        role=scope.lambda_execution_role_arn,
        log_group_name=f"/lambda/{function_name}",
        log_retention=logs.RetentionDays.ONE_MONTH,
        runtime='python3.11'
    )
    # outputs=['TransformJobArn', 'JobName', 'OutputPath', 'Status']

    task = lambda_function.generate_task(
        payload={
            'model_name': stepfunctions.JsonPath.string_at(model_name_lkp),
            's3_data_source': stepfunctions.JsonPath.string_at(s3_data_source_lkp) if s3_data_source_lkp else s3_data_source,
            'transform_out_dir': stepfunctions.JsonPath.string_at(transform_out_dir_lkp) if transform_out_dir_lkp else transform_out_dir,
            'instance_type': stepfunctions.JsonPath.string_at(instance_type_lkp)
        },
        # result_selector={}
    )
    return [task, lambda_function]

def parse_instances_fn_task(scope, construct_id, function_name, monitor_instance_lkp, transform_instance_lkp, endpoint_instance_lkp):
    lambda_function = CLambdaFunction(
        scope, construct_id,
        use_docker=False,
        function_name=function_name,
        code_path='code/utils',
        handler='utils.instance_parse_handler',
        role=scope.lambda_execution_role_arn,
        log_group_name=f"/lambda/{function_name}",
        log_retention=logs.RetentionDays.ONE_MONTH,
        runtime='python3.11'
    )
    # outputs=['monitor_instance.class', 'monitor_instance.size', 'transform_instance.class', 'transform_instance.size', 'endpoint_instance.class', 'endpoint_instance.size']

    task = lambda_function.generate_task(
        payload={
            'monitor_instance': stepfunctions.JsonPath.string_at(monitor_instance_lkp),
            'transform_instance': stepfunctions.JsonPath.string_at(transform_instance_lkp),
            'endpoint_instance': stepfunctions.JsonPath.string_at(endpoint_instance_lkp)
        },
        # result_selector={}
    )
    return [task, lambda_function]

def get_get_or_create_model_from_registry_fn_task(scope, construct_id, function_name, model_package_group_name, model_package_version_lkp):
    stepfunctions.JsonPath.string_at(model_package_version_lkp)

    lambda_function = CLambdaFunction(
        scope, construct_id,
        use_docker=False,
        function_name=function_name,
        code_path='code/get_or_create_model_from_registry',
        handler='get_or_create_model_from_registry.handler',
        role=scope.lambda_execution_role_arn,
        log_group_name=f"/lambda/{function_name}",
        log_retention=logs.RetentionDays.ONE_MONTH,
        runtime='python3.11'
    )
    # outputs=['model_name', 'model_package_arn']
    
    task = lambda_function.generate_task(
        payload={
            'model_package_group_name': model_package_group_name,
            'model_package_version': stepfunctions.JsonPath.string_at(model_package_version_lkp)
        },
        # result_selector={}
    )

    return [task, lambda_function]


def prep_baseline_sets_fn_task(scope, construct_id, function_name, baseline_file_lkp, target_name, target_type, baseline_dir):
    lambda_function = CLambdaFunction(
        scope, construct_id,
        use_docker=False,
        function_name=function_name,
        code_path='code/baselining/',
        handler='baselining.prep_baseline_sets_handler',
        role=scope.lambda_execution_role_arn,
        log_group_name=f"/lambda/{function_name}",
        log_retention=logs.RetentionDays.ONE_MONTH,
        runtime='python3.11'
    )
    # outputs=['baseline_X_dir', 'baseline_X_file', 'baseline_X_filename']

    task = lambda_function.generate_task(
        payload={
            'baseline_file': stepfunctions.JsonPath.string_at(baseline_file_lkp),
            'target_name':target_name,
            'target_type': target_type,
            'baseline_X_file_dest_dir':baseline_dir
        },
        # result_selector={}
    )
    
    return [task, lambda_function]


def get_baseline_preds_fn_task(scope, construct_id, function_name, transform_out_dir_lkp, target_name, target_type):
    lambda_function = CLambdaFunction(
        scope, construct_id,
        use_docker=False,
        function_name=function_name,
        code_path='code/baselining/',
        handler='baselining.get_baseline_preds_handler',
        role=scope.lambda_execution_role_arn,
        log_group_name=f"/lambda/{function_name}",
        log_retention=logs.RetentionDays.ONE_MONTH,
        runtime='python3.11'
    )
    # outputs=['baseline_pred_file']

    task = lambda_function.generate_task(
        payload={
            'transform_out_dir': stepfunctions.JsonPath.string_at(transform_out_dir_lkp),
            'baseline_X_filename':target_name,
            'baseline_pred_file_dest': target_type,
        },
        # result_selector={}
    )
    
    return [task, lambda_function]



def make_baseline_sets_fn_task(scope, construct_id, function_name, baseline_file_lkp, baseline_pred_file_lkp, dq_monitor_dir, db_monitor_dir, mq_monitor_dir, mb_monitor_dir, me_monitor_dir, target_name, prediction_name, baseline_X_file_lkp, target_type):
    lambda_function = CLambdaFunction(
        scope, construct_id,
        use_docker=False,
        function_name=function_name,
        code_path='code/baselining/',
        handler='baselining.make_baseline_sets_handler',
        role=scope.lambda_execution_role_arn,
        log_group_name=f"/lambda/{function_name}",
        log_retention=logs.RetentionDays.ONE_MONTH,
        runtime='python3.11'
    )
    # outputs=[]

    task = lambda_function.generate_task(
        payload={
            'baseline_file': stepfunctions.JsonPath.string_at(baseline_file_lkp),
            'baseline_pred_file':stepfunctions.JsonPath.string_at(baseline_pred_file_lkp),
            'dq_monitor_dir': dq_monitor_dir,
            'db_monitor_dir': db_monitor_dir,
            'mq_monitor_dir':mq_monitor_dir,
            'mb_monitor_dir': mb_monitor_dir,
            'me_monitor_dir': me_monitor_dir,
            'target_name':target_name,
            'prediction_name': prediction_name,
            'baseline_X_file': stepfunctions.JsonPath.string_at(baseline_X_file_lkp),
            'target_type':target_type
        },
        # result_selector={}
    )

    return [task, lambda_function]


def schedule_dq_task_fn_task(scope, 
        construct_id,
        function_name,
        monitor_name,
        endpoint_name_lkp,
        data_capture_dir,
        monitor_role,
        deploy_type,
        dq_monitor_dir,
        monitor_instance_type_lkp,
        schedule_expression_lkp,
        data_analysis_start_time_lkp,
        data_analysis_end_time_lkp,
        image_uri='156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer', 
        instance_count=1, 
        volume_size_in_gb=20, 
        max_runtime_in_seconds=1800, 
        dataset_format={'Csv': {'Header': True}}
    ):
    lambda_function = CLambdaFunction(
        scope, construct_id,
        use_docker=False,
        function_name=function_name,
        code_path='code/schedule_monitors/',
        handler='schedule_monitors.data_quality_handler',
        role=scope.lambda_execution_role_arn,
        log_group_name=f"/lambda/{function_name}",
        log_retention=logs.RetentionDays.ONE_MONTH,
        runtime='python3.11'
    )
    # outputs=[]

    task = lambda_function.generate_task(
        payload={
            'name': monitor_name,
            'endpoint_name': stepfunctions.JsonPath.string_at(endpoint_name_lkp),
            'data_capture_dir':data_capture_dir,
            'monitor_role': monitor_role.role_arn,
            'deploy_type':deploy_type,
            'monitor_dir': dq_monitor_dir,
            'image_uri': image_uri,
            'instance_count':instance_count,
            'instance_type':stepfunctions.JsonPath.string_at(monitor_instance_type_lkp),
            'volume_size_in_gb': volume_size_in_gb,
            'max_runtime_in_seconds': max_runtime_in_seconds,
            'dataset_format':json.dumps(dataset_format),
            'schedule_expression':stepfunctions.JsonPath.string_at(schedule_expression_lkp),
            'data_analysis_start_time':stepfunctions.JsonPath.string_at(data_analysis_start_time_lkp),
            'data_analysis_end_time':stepfunctions.JsonPath.string_at(data_analysis_end_time_lkp)

        },
        # result_selector={}
    )
    
    return [task, lambda_function]


def schedule_mb_task_fn_task(scope, 
        construct_id,
        function_name,
        monitor_name,
        endpoint_name_lkp,
        data_capture_dir,
        monitor_role,
        deploy_type,
        mb_monitor_dir,
        ground_truth_dir_lkp,
        monitor_instance_type_lkp,
        schedule_expression_lkp,
        data_analysis_start_time_lkp,
        data_analysis_end_time_lkp,
        image_uri='156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer', 
        instance_count=1, 
        volume_size_in_gb=20, 
        max_runtime_in_seconds=1800, 
        dataset_format={'Csv': {'Header': True}}
    ):

    lambda_function = CLambdaFunction(
        scope, construct_id,
        use_docker=False,
        function_name=function_name,
        code_path='code/schedule_monitors/',
        handler='schedule_monitors.model_bias_handler',
        role=scope.lambda_execution_role_arn,
        log_group_name=f"/lambda/{function_name}",
        log_retention=logs.RetentionDays.ONE_MONTH,
        runtime='python3.11'
    )
    # outputs=[]

    task = lambda_function.generate_task(
        payload={
            'name': monitor_name,
            'endpoint_name': stepfunctions.JsonPath.string_at(endpoint_name_lkp),
            'data_capture_dir':data_capture_dir,
            'monitor_role': monitor_role.role_arn,
            'deploy_type':deploy_type,
            'monitor_dir': mb_monitor_dir,
            'ground_truth_dir': stepfunctions.JsonPath.string_at(ground_truth_dir_lkp),
            'image_uri': image_uri,
            'instance_count':instance_count,
            'instance_type':stepfunctions.JsonPath.string_at(monitor_instance_type_lkp),
            'volume_size_in_gb': volume_size_in_gb,
            'max_runtime_in_seconds': max_runtime_in_seconds,
            'dataset_format':json.dumps(dataset_format),
            'schedule_expression':stepfunctions.JsonPath.string_at(schedule_expression_lkp),
            'data_analysis_start_time':stepfunctions.JsonPath.string_at(data_analysis_start_time_lkp),
            'data_analysis_end_time':stepfunctions.JsonPath.string_at(data_analysis_end_time_lkp)

        },
        # result_selector={}
    )
    
    return [task, lambda_function]


def schedule_me_task_fn_task(scope, 
        construct_id,
        function_name,
        monitor_name,
        endpoint_name_lkp,
        data_capture_dir,
        monitor_role,
        deploy_type,
        me_monitor_dir,
        monitor_instance_type_lkp,
        schedule_expression_lkp,
        data_analysis_start_time_lkp,
        data_analysis_end_time_lkp,
        image_uri='156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer', 
        instance_count=1, 
        volume_size_in_gb=20, 
        max_runtime_in_seconds=1800, 
        dataset_format={'Csv': {'Header': True}}
    ):
    
    lambda_function = CLambdaFunction(
        scope, construct_id,
        use_docker=False,
        function_name=function_name,
        code_path='code/schedule_monitors/',
        handler='schedule_monitors.model_explainability_handler',
        role=scope.lambda_execution_role_arn,
        log_group_name=f"/lambda/{function_name}",
        log_retention=logs.RetentionDays.ONE_MONTH,
        runtime='python3.11'
    )
    # outputs=[]

    task = lambda_function.generate_task(
        payload={
            'name': monitor_name,
            'endpoint_name': stepfunctions.JsonPath.string_at(endpoint_name_lkp),
            'data_capture_dir':data_capture_dir,
            'monitor_role': monitor_role.role_arn,
            'deploy_type':deploy_type,
            'monitor_dir': me_monitor_dir,
            'image_uri': image_uri,
            'instance_count':instance_count,
            'instance_type':stepfunctions.JsonPath.string_at(monitor_instance_type_lkp),
            'volume_size_in_gb': volume_size_in_gb,
            'max_runtime_in_seconds': max_runtime_in_seconds,
            'dataset_format':json.dumps(dataset_format),
            'schedule_expression':stepfunctions.JsonPath.string_at(schedule_expression_lkp),
            'data_analysis_start_time':stepfunctions.JsonPath.string_at(data_analysis_start_time_lkp),
            'data_analysis_end_time':stepfunctions.JsonPath.string_at(data_analysis_end_time_lkp)

        },
        # result_selector={}
    )
    
    return [task, lambda_function]


def schedule_mq_task_fn_task(scope, 
        construct_id,
        function_name,
        monitor_name,
        endpoint_name_lkp,
        data_capture_dir,
        monitor_role,
        deploy_type,
        problem_type,
        ground_truth_label,
        ground_truth_dir_lkp,
        mq_monitor_dir,
        monitor_instance_type_lkp,
        schedule_expression_lkp,
        data_analysis_start_time_lkp,
        data_analysis_end_time_lkp,
        image_uri='156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer', 
        instance_count=1, 
        volume_size_in_gb=20, 
        max_runtime_in_seconds=1800, 
        dataset_format={'Csv': {'Header': True}}
    ):
    lambda_function = CLambdaFunction(
        scope, construct_id,
        use_docker=False,
        function_name=function_name,
        code_path='code/schedule_monitors/',
        handler='schedule_monitors.model_quality_handler',
        role=scope.lambda_execution_role_arn,
        log_group_name=f"/lambda/{function_name}",
        log_retention=logs.RetentionDays.ONE_MONTH,
        runtime='python3.11'
    )
    # outputs=[]

    task = lambda_function.generate_task(
        payload={
            'name': monitor_name,
            'endpoint_name': stepfunctions.JsonPath.string_at(endpoint_name_lkp),
            'data_capture_dir':data_capture_dir,
            'monitor_role': monitor_role.role_arn,
            'deploy_type':deploy_type,
            'problem_type':problem_type,
            'ground_truth_label':ground_truth_label,
            'monitor_dir': mq_monitor_dir,
            'ground_truth_dir':stepfunctions.JsonPath.string_at(ground_truth_dir_lkp),
            'image_uri': image_uri,
            'instance_count':instance_count,
            'instance_type':stepfunctions.JsonPath.string_at(monitor_instance_type_lkp),
            'volume_size_in_gb': volume_size_in_gb,
            'max_runtime_in_seconds': max_runtime_in_seconds,
            'dataset_format':json.dumps(dataset_format),
            'schedule_expression':stepfunctions.JsonPath.string_at(schedule_expression_lkp),
            'data_analysis_start_time':stepfunctions.JsonPath.string_at(data_analysis_start_time_lkp),
            'data_analysis_end_time':stepfunctions.JsonPath.string_at(data_analysis_end_time_lkp)

        },
        # result_selector={}
    )
    
    return [task, lambda_function]


def deploy_endpoint_fn_task(scope, construct_id, function_name, model_name_lkp, model_package_group_name, model_package_version_lkp, endpoint_instance_type_lkp, data_capture_dir):
    lambda_function = CLambdaFunction(
        scope, construct_id,
        use_docker=False,
        function_name=function_name,
        code_path='code/deploy_endpoint/',
        handler='deploy_endpoint.handler',
        role=scope.lambda_execution_role_arn,
        log_group_name=f"/lambda/{function_name}",
        log_retention=logs.RetentionDays.ONE_MONTH,
        runtime='python3.11'
    )
    # outputs=[endpoint_name]

    task = lambda_function.generate_task(
        payload={
            'model_name': stepfunctions.JsonPath.string_at(model_name_lkp),
            'model_package_group_name':model_package_group_name,
            'model_package_version': stepfunctions.JsonPath.string_at(model_package_version_lkp),
            'instance_type': stepfunctions.JsonPath.string_at(endpoint_instance_type_lkp),
            'data_capture_dir':data_capture_dir

        },
        # result_selector={}
    )
    
    return [task, lambda_function]


def check_dq_task_fn_task(scope, construct_id, function_name):
    lambda_function = CLambdaFunction(
        scope, construct_id,
        use_docker=False,
        function_name=function_name,
        code_path='code/check_monitors/',
        handler='check_monitors.data_quality_handler',
        role=scope.lambda_execution_role_arn,
        log_group_name=f"/lambda/{function_name}",
        log_retention=logs.RetentionDays.ONE_MONTH,
        runtime='python3.11'
    )
    task = lambda_function.generate_task(
        payload={
        },
        # result_selector={}
    )
    return [task, lambda_function]

def check_mq_task_fn_task(scope, construct_id, function_name):
    lambda_function = CLambdaFunction(
        scope, construct_id,
        use_docker=False,
        function_name=function_name,
        code_path='code/check_monitors/',
        handler='check_monitors.model_quality_handler',
        role=scope.lambda_execution_role_arn,
        log_group_name=f"/lambda/{function_name}",
        log_retention=logs.RetentionDays.ONE_MONTH,
        runtime='python3.11'
    )
    task = lambda_function.generate_task(
        payload={
        },
        # result_selector={}
    )
    return [task, lambda_function]

def check_me_task_fn_task(scope, construct_id, function_name):
    lambda_function = CLambdaFunction(
        scope, construct_id,
        use_docker=False,
        function_name=function_name,
        code_path='code/check_monitors/',
        handler='check_monitors.model_explainability_handler',
        role=scope.lambda_execution_role_arn,
        log_group_name=f"/lambda/{function_name}",
        log_retention=logs.RetentionDays.ONE_MONTH,
        runtime='python3.11'
    )
    task = lambda_function.generate_task(
        payload={
        },
        # result_selector={}
    )
    return [task, lambda_function]

def check_mb_task_fn_task(scope, construct_id, function_name):
    lambda_function = CLambdaFunction(
        scope, construct_id,
        use_docker=False,
        function_name=function_name,
        code_path='code/check_monitors/',
        handler='check_monitors.model_bias_handler',
        role=scope.lambda_execution_role_arn,
        log_group_name=f"/lambda/{function_name}",
        log_retention=logs.RetentionDays.ONE_MONTH,
        runtime='python3.11'
    )
    task = lambda_function.generate_task(
        payload={
        },
        # result_selector={}
    )
    return [task, lambda_function]

