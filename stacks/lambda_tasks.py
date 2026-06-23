import os, pathlib, json
from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_ecs as ecs,
    aws_iam as iam,
    aws_logs as logs,
    aws_stepfunctions as stepfunctions,
    aws_events as events,
    aws_events_targets as targets,
    aws_ecr_assets as ecr_assets,
)
from custom_constructs.CNetwork import CNetwork
from custom_constructs.CLambda import CLambdaFunction
from custom_constructs.CECS import CFargateTaskDefinition
from custom_constructs.utils import get_local_project_root

def get_get_or_create_model_from_registry_fn_task(scope):
    function_name = "get_or_create_model_from_registry",
    lambda_function = CLambdaFunction(
        scope, "GetOrCreateModelFromRegistry",
        use_docker=False,
        function_name=function_name,
        code_path='code/get_or_create_model_from_registry',
        handler='get_or_create_model_from_registry.handler',
        role=scope.lambda_execution_role,
        log_group_name=f"/lambda/{function_name}",
        log_retention=logs.RetentionDays.ONE_MONTH
    )
    # outputs=['model_name', 'model_package_arn']

    task = lambda_function.generate_task(
        payload={
            'model_package_group_name': scope.model_package_group_name,
            'model_package_version': scope.model_package_version_param
        },
        # result_selector={}
    )
    return [task, lambda_function]


def prep_baseline_sets_fn_task(scope):
    function_name = "prep_baseline_sets",
    lambda_function = CLambdaFunction(
        scope, "PrepBaselineSets",
        use_docker=False,
        function_name=function_name,
        code_path='code/baselining',
        handler='baselining.prep_baseline_sets_handler',
        role=scope.lambda_execution_role,
        log_group_name=f"/lambda/{function_name}",
        log_retention=logs.RetentionDays.ONE_MONTH
    )
    # outputs=['baseline_X_dir', 'baseline_X_file', 'baseline_X_filename']

    task = lambda_function.generate_task(
        payload={
            'baseline_file': scope.baseline_file,
            'target_name':scope.target_name,
            'target_type': scope.target_type,
            'baseline_X_file_dest_dir':scope.baseline_dir
        },
        # result_selector={}
    )
    
    return [task, lambda_function]


def get_baseline_preds_fn_task(scope):
    function_name = "get_baseline_preds",
    lambda_function = CLambdaFunction(
        scope, "GetBaselinePreds",
        use_docker=False,
        function_name=function_name,
        code_path='code/baselining',
        handler='baselining.get_baseline_preds_handler',
        role=scope.lambda_execution_role,
        log_group_name=f"/lambda/{function_name}",
        log_retention=logs.RetentionDays.ONE_MONTH
    )
    # outputs=['baseline_pred_file']

    task = lambda_function.generate_task(
        payload={
            'transform_out_dir': scope.baseline_file,
            'baseline_X_filename':scope.target_name,
            'baseline_pred_file_dest': scope.target_type,
        },
        # result_selector={}
    )
    
    return [task, lambda_function]



def make_baseline_sets_fn_task(scope):
    function_name = "make_baseline_sets",
    lambda_function = CLambdaFunction(
        scope, "MakeBaselineSets",
        use_docker=False,
        function_name=function_name,
        code_path='code/baselining',
        handler='baselining.make_baseline_sets_handler',
        role=scope.lambda_execution_role,
        log_group_name=f"/lambda/{function_name}",
        log_retention=logs.RetentionDays.ONE_MONTH
    )
    # outputs=[]

    task = lambda_function.generate_task(
        payload={
            'baseline_file': scope.baseline_file,
            'baseline_pred_file':scope.baseline_pred_file,
            'dq_monitor_dir': scope.dq_monitor_dir,
            'db_monitor_dir': scope.db_monitor_dir,
            'mq_monitor_dir':scope.mq_monitor_dir,
            'mb_monitor_dir': scope.mb_monitor_dir,
            'me_monitor_dir': scope.me_monitor_dir,
            'target_name':scope.target_name,
            'prediction_name': scope.prediction_name,
            'baseline_X_file': scope.baseline_X_file,
            'target_type':scope.target_type
        },
        # result_selector={}
    )
    
    return [task, lambda_function]



def schedule_dq_task_fn_task(scope, name, image_uri='156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer', instance_count=1, volume_size_in_gb=20, max_runtime_in_seconds=1800, dataset_format={'Csv': {'Header': True}}, schedule_expression='cron(0 * ? * * *)', data_analysis_start_time="-PT2H",data_analysis_end_time="-PT1H"):
    function_name = "schedule_data_quality",
    lambda_function = CLambdaFunction(
        scope, "ScheduleDataQuality",
        use_docker=False,
        function_name=function_name,
        code_path='code/schedule_monitors',
        handler='schedule_monitors.data_quality_handler',
        role=scope.lambda_execution_role,
        log_group_name=f"/lambda/{function_name}",
        log_retention=logs.RetentionDays.ONE_MONTH
    )
    # outputs=[]

    task = lambda_function.generate_task(
        payload={
            'endpoint_name': scope.endpoint_name,
            'data_cature_dir':scope.data_cature_dir,
            'name': name,
            'monitor_role': scope.other_execution_role_arn,
            'deploy_type':scope.deploy_type,
            'monitor_dir': scope.dq_monitor_dir,
            'image_uri': image_uri,
            'instance_count':instance_count,
            'instance_type':scope.monitor_instance_type,
            'volume_size_in_gb': volume_size_in_gb,
            'max_runtime_in_seconds': max_runtime_in_seconds,
            'dataset_format':json.dumps(dataset_format),
            'schedule_expression':schedule_expression,
            'data_analysis_start_time':data_analysis_start_time,
            'data_analysis_end_time':data_analysis_end_time,

        },
        # result_selector={}
    )
    
    return [task, lambda_function]


def schedule_mb_task_fn_task(scope, name, image_uri='156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer', instance_count=1, volume_size_in_gb=20, max_runtime_in_seconds=1800, dataset_format={'Csv': {'Header': True}}, schedule_expression='cron(0 * ? * * *)', data_analysis_start_time="-PT2H",data_analysis_end_time="-PT1H"):
    function_name = "schedule_model_bias",
    lambda_function = CLambdaFunction(
        scope, "ScheduleModelBias",
        use_docker=False,
        function_name=function_name,
        code_path='code/schedule_monitors',
        handler='schedule_monitors.model_bias_handler',
        role=scope.lambda_execution_role,
        log_group_name=f"/lambda/{function_name}",
        log_retention=logs.RetentionDays.ONE_MONTH
    )
    # outputs=[]

    task = lambda_function.generate_task(
        payload={
            'endpoint_name': scope.endpoint_name,
            'data_cature_dir':scope.data_cature_dir,
            'name': name,
            'monitor_role': scope.other_execution_role_arn,
            'deploy_type':scope.deploy_type,
            'monitor_dir': scope.mb_monitor_dir,
            'ground_truth_dir': scope.ground_truth_dir,
            'image_uri': image_uri,
            'instance_count':instance_count,
            'instance_type':scope.monitor_instance_type,
            'volume_size_in_gb': volume_size_in_gb,
            'max_runtime_in_seconds': max_runtime_in_seconds,
            'dataset_format':json.dumps(dataset_format),
            'schedule_expression':schedule_expression,
            'data_analysis_start_time':data_analysis_start_time,
            'data_analysis_end_time':data_analysis_end_time,

        },
        # result_selector={}
    )
    
    return [task, lambda_function]


def schedule_me_task_fn_task(scope, name, image_uri='156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer', instance_count=1, volume_size_in_gb=20, max_runtime_in_seconds=1800, dataset_format={'Csv': {'Header': True}}, schedule_expression='cron(0 * ? * * *)', data_analysis_start_time="-PT2H",data_analysis_end_time="-PT1H"):
    function_name = "schedule_model_explainability",
    lambda_function = CLambdaFunction(
        scope, "ScheduleModelExplainability",
        use_docker=False,
        function_name=function_name,
        code_path='code/schedule_monitors',
        handler='schedule_monitors.model_explainability_handler',
        role=scope.lambda_execution_role,
        log_group_name=f"/lambda/{function_name}",
        log_retention=logs.RetentionDays.ONE_MONTH
    )
    # outputs=[]

    task = lambda_function.generate_task(
        payload={
            'endpoint_name': scope.endpoint_name,
            'data_cature_dir':scope.data_cature_dir,
            'name': name,
            'monitor_role': scope.other_execution_role_arn,
            'deploy_type':scope.deploy_type,
            'monitor_dir': scope.me_monitor_dir,
            'image_uri': image_uri,
            'instance_count':instance_count,
            'instance_type':scope.monitor_instance_type,
            'volume_size_in_gb': volume_size_in_gb,
            'max_runtime_in_seconds': max_runtime_in_seconds,
            'dataset_format':json.dumps(dataset_format),
            'schedule_expression':schedule_expression,
            'data_analysis_start_time':data_analysis_start_time,
            'data_analysis_end_time':data_analysis_end_time,

        },
        # result_selector={}
    )
    
    return [task, lambda_function]


def schedule_mq_task_fn_task(scope, name, image_uri='156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer', instance_count=1, volume_size_in_gb=20, max_runtime_in_seconds=1800, dataset_format={'Csv': {'Header': True}}, schedule_expression='cron(0 * ? * * *)', data_analysis_start_time="-PT2H",data_analysis_end_time="-PT1H"):
    function_name = "schedule_model_quality",
    lambda_function = CLambdaFunction(
        scope, "ScheduleModelQuality",
        use_docker=False,
        function_name=function_name,
        code_path='code/schedule_monitors',
        handler='schedule_monitors.model_quality_handler',
        role=scope.lambda_execution_role,
        log_group_name=f"/lambda/{function_name}",
        log_retention=logs.RetentionDays.ONE_MONTH
    )
    # outputs=[]

    task = lambda_function.generate_task(
        payload={
            'endpoint_name': scope.endpoint_name,
            'data_cature_dir':scope.data_cature_dir,
            'name': name,
            'monitor_role': scope.other_execution_role_arn,
            'deploy_type':scope.deploy_type,
            'problem_type':scope.problem_type,
            'ground_truth_label':scope.ground_truth_label,
            'monitor_dir': scope.mq_monitor_dir,
            'ground_truth_dir':scope.ground_truth_dir,
            'image_uri': image_uri,
            'instance_count':instance_count,
            'instance_type':scope.monitor_instance_type,
            'volume_size_in_gb': volume_size_in_gb,
            'max_runtime_in_seconds': max_runtime_in_seconds,
            'dataset_format':json.dumps(dataset_format),
            'schedule_expression':schedule_expression,
            'data_analysis_start_time':data_analysis_start_time,
            'data_analysis_end_time':data_analysis_end_time,

        },
        # result_selector={}
    )
    
    return [task, lambda_function]


def deploy_endpoint_fn_task(scope):
    function_name = "deploy_endpoint",
    lambda_function = CLambdaFunction(
        scope, "DeployEndpoint",
        use_docker=False,
        function_name=function_name,
        code_path='code/deploy_endpoint',
        handler='deploy_endpoint.handler',
        role=scope.lambda_execution_role,
        log_group_name=f"/lambda/{function_name}",
        log_retention=logs.RetentionDays.ONE_MONTH
    )
    # outputs=[endpoint_name]

    task = lambda_function.generate_task(
        payload={
            'model_name': scope.model_name,
            'model_package_group_name':scope.model_package_group_name,
            'model_package_version_param': scope.model_package_version_param,
            'instance_type_param': scope.endpoint_instance_type_param,
            'data_capture_dir':scope.data_capture_dir

        },
        # result_selector={}
    )
    
    return [task, lambda_function]

