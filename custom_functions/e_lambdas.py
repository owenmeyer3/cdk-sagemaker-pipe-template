import json
from aws_cdk import (
    aws_logs as logs,
    aws_lambda as _lambda,
    aws_stepfunctions as stepfunctions,
    aws_stepfunctions_tasks as tasks,
    aws_ecr as ecr,
    aws_iam as iam,
    Duration,
    RemovalPolicy
)
from custom_constructs.CLambda import Lambda

def get_get_or_create_model_from_registry_lambda(scope, construct_id, function_name, role, model_package_group_name, model_package_version_lkp, create_model_role):
    print(scope)
    print(construct_id)
    print(function_name)
    print(model_package_group_name)
    print(model_package_version_lkp)
    print(create_model_role)

    return Lambda(scope, construct_id,
        function_config={
            'type':'Basic',
            'code_path':'code/get_or_create_model_from_registry',
            'handler':'get_or_create_model_from_registry.handler',
            'runtime':'python3.11',
            'function_name':function_name,
            'role':role,
        },
        log_config={
            'log_group_name':f"/lambda/{function_name}"
        },
        task_config={
            'outputs':['MODEL_NAME', 'MODEL_PACKAGE_ARN'],
            'payload':{
                'model_package_group_name': model_package_group_name,
                'model_package_version': stepfunctions.JsonPath.string_at(model_package_version_lkp),
                'create_model_role':create_model_role.role_arn
            }
        }
    )


def prep_baseline_sets_lambda(scope, construct_id, function_name, role, baseline_file_lkp, target_label, target_type, baseline_dir, baseline_cols_lkp = [], layers=[]):

    return Lambda(scope, construct_id,
        function_config={
            'type':'Basic',
            'code_path':'code/baseline_general/',
            'handler':'baseline_general.prep_baseline_sets_handler',
            'runtime':'python3.11',
            'function_name':function_name,
            'role':role,
            'layers':layers
        },
        log_config={
            'log_group_name':f"/lambda/{function_name}"
        },
        task_config={
            'outputs':['BASELINE_HEADERED_FILE', 'BASELINE_X_FILE', 'BASELINE_X_FILENAME'],
            'payload':{
                'baseline_file': stepfunctions.JsonPath.string_at(baseline_file_lkp),
                'target_label':target_label,
                'target_type': target_type,
                'baseline_dir':baseline_dir,
                'columns':stepfunctions.JsonPath.string_at(baseline_cols_lkp)
            }
        }
    )


def get_baseline_preds_lambda(scope, construct_id, function_name, role, transform_out_dir_lkp, baseline_X_filename_lkp, baseline_dir, baseline_headered_file_lkp, predict_label, target_label, target_type, layers=[]):

    return Lambda(scope, construct_id,
        function_config={
            'type':'Basic',
            'code_path':'code/baseline_general/',
            'handler':'baseline_general.get_baseline_preds_handler',
            'runtime':'python3.11',
            'function_name':function_name,
            'role':role,
            'layers':layers
        },
        log_config={
            'log_group_name':f"/lambda/{function_name}"
        },
        task_config={
            'outputs':['BASELINE_PRED_FILE', 'BASELINE_FULL_FILE'],
            'payload':{
                'transform_out_dir': stepfunctions.JsonPath.string_at(transform_out_dir_lkp),
                'baseline_X_filename':stepfunctions.JsonPath.string_at(baseline_X_filename_lkp),
                'baseline_dir': baseline_dir,
                'baseline_headered_file':stepfunctions.JsonPath.string_at(baseline_headered_file_lkp),
                'predict_label':predict_label,
                'target_label':target_label,
                'target_type':target_type

            }
        }
    )

def deploy_endpoint_lambda(scope, construct_id, function_name, role, model_name_lkp, model_package_group_name, model_package_version_lkp, endpoint_instance_type_lkp, data_capture_dir):

    return Lambda(scope, construct_id,
        function_config={
            'type':'Basic',
            'code_path':'code/deploy_endpoint/',
            'handler':'deploy_endpoint.handler',
            'runtime':'python3.11',
            'function_name':function_name,
            'role':role,
        },
        log_config={
            'log_group_name':f"/lambda/{function_name}"
        },
        task_config={
            'outputs':['ENDPOINT_NAME'],
            'payload':{
                'model_name': stepfunctions.JsonPath.string_at(model_name_lkp),
                'model_package_group_name':model_package_group_name,
                'model_package_version': stepfunctions.JsonPath.string_at(model_package_version_lkp),
                'instance_type': stepfunctions.JsonPath.string_at(endpoint_instance_type_lkp),
                'data_capture_dir':data_capture_dir
            }
        }
    )

def schedule_dq_task_lambda(scope, 
        construct_id,
        function_name, 
        role,
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

    return Lambda(scope, construct_id,
        function_config={
            'type':'Basic',
            'code_path':'code/schedule_monitors/',
            'handler':'schedule_monitors.data_quality_handler',
            'runtime':'python3.11',
            'function_name':function_name,
            'role':role,
        },
        log_config={
            'log_group_name':f"/lambda/{function_name}"
        },
        task_config={
            'outputs':[],
            'payload':{
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
            }
        }
    )


def schedule_mb_task_lambda(scope, 
        construct_id,
        function_name, 
        role,
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

    return Lambda(scope, construct_id,
        function_config={
            'type':'Basic',
            'code_path':'code/schedule_monitors/',
            'handler':'schedule_monitors.model_bias_handler',
            'runtime':'python3.11',
            'function_name':function_name,
            'role':role,
        },
        log_config={
            'log_group_name':f"/lambda/{function_name}"
        },
        task_config={
            'outputs':[],
            'payload':{
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
            }
        }
    )


def schedule_me_task_lambda(scope, 
        construct_id,
        function_name, 
        role,
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

    return Lambda(scope, construct_id,
        function_config={
            'type':'Basic',
            'code_path':'code/schedule_monitors/',
            'handler':'schedule_monitors.model_explainability_handler',
            'runtime':'python3.11',
            'function_name':function_name,
            'role':role,
        },
        log_config={
            'log_group_name':f"/lambda/{function_name}"
        },
        task_config={
            'outputs':[],
            'payload':{
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
            }
        }
    )

def schedule_mq_task_lambda(scope, 
        construct_id,
        function_name, 
        role,
        monitor_name,
        endpoint_name_lkp,
        data_capture_dir,
        monitor_role,
        deploy_type,
        problem_type,
        predict_label,
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

    return Lambda(scope, construct_id,
        function_config={
            'type':'Basic',
            'code_path':'code/schedule_monitors/',
            'handler':'schedule_monitors.model_quality_handler',
            'runtime':'python3.11',
            'function_name':function_name,
            'role':role,
        },
        log_config={
            'log_group_name':f"/lambda/{function_name}"
        },
        task_config={
            'outputs':[],
            'payload':{
                'name': monitor_name,
                'endpoint_name': stepfunctions.JsonPath.string_at(endpoint_name_lkp),
                'data_capture_dir':data_capture_dir,
                'monitor_role': monitor_role.role_arn,
                'deploy_type':deploy_type,
                'problem_type':problem_type,
                'predict_label':predict_label,
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
            }
        }
    )


def check_dq_task_lambda(scope, construct_id, function_name, role):

    return Lambda(scope, construct_id,
        function_config={
            'type':'Basic',
            'code_path':'code/check_monitors/',
            'handler':'check_monitors.data_quality_handler',
            'runtime':'python3.11',
            'function_name':function_name,
            'role':role,
        },
        log_config={
            'log_group_name':f"/lambda/{function_name}"
        },
        task_config={
            'outputs':[],
            'payload':{}
        }
    )

def check_mq_task_lambda(scope, construct_id, function_name, role):

    return Lambda(scope, construct_id,
        function_config={
            'type':'Basic',
            'code_path':'code/check_monitors/',
            'handler':'check_monitors.model_quality_handler',
            'runtime':'python3.11',
            'function_name':function_name,
            'role':role,
        },
        log_config={
            'log_group_name':f"/lambda/{function_name}"
        },
        task_config={
            'outputs':[],
            'payload':{}
        }
    )

def check_me_task_lambda(scope, construct_id, function_name, role):

    return Lambda(scope, construct_id,
        function_config={
            'type':'Basic',
            'code_path':'code/check_monitors/',
            'handler':'check_monitors.model_explainability_handler',
            'runtime':'python3.11',
            'function_name':function_name,
            'role':role,
        },
        log_config={
            'log_group_name':f"/lambda/{function_name}"
        },
        task_config={
            'outputs':[],
            'payload':{}
        }
    )

def check_mb_task_lambda(scope, construct_id, function_name, role):

    return Lambda(scope, construct_id,
        function_config={
            'type':'Basic',
            'code_path':'code/check_monitors/',
            'handler':'check_monitors.model_bias_handler',
            'runtime':'python3.11',
            'function_name':function_name,
            'role':role,
        },
        log_config={
            'log_group_name':f"/lambda/{function_name}"
        },
        task_config={
            'outputs':[],
            'payload':{}
        }
    )