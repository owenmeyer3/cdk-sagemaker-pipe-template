import json
from aws_cdk import (
    aws_logs as logs,
    aws_lambda as _lambda,
    aws_stepfunctions as stepfunctions,
    aws_stepfunctions_tasks as tasks,
    aws_ecs as ecs,
    Duration,
    RemovalPolicy
)
from custom_constructs.CECS import ECSFargate

def get_dq_bl(
    scope, 
    construct_id, 
    family, 
    repo,
    monitor_role,
    monitor_dir,
    cluster,
    network,
    baseline_full_dataset_lkp,
    target_label,
    predict_label,
    instance_count=1,
    instance_type='ml.m5.xlarge',
    volume_size_in_gb=20,
    max_runtime_in_seconds=3600
):
    environment={
        'monitor_type':'DataQuality',
        'role':monitor_role.role_arn,
        'baseline_full_dataset':stepfunctions.JsonPath.string_at(baseline_full_dataset_lkp),
        'output_s3_uri':monitor_dir+'/info',
        'target_label':target_label,
        'predict_label':predict_label,
        'dataset_format':json.dumps({'csv': {'header': True}}),
        'instance_count':str(instance_count),
        'instance_type':instance_type,
        'volume_size_in_gb':str(volume_size_in_gb),
        'max_runtime_in_seconds':str(max_runtime_in_seconds),
        'execution_id':stepfunctions.JsonPath.string_at('$$.Execution.Name')
    }

    return ECSFargate(
        scope, 
        construct_id,
        task_def_config={'family':family, 'execution_role':monitor_role, 'task_role':monitor_role, 'repo':repo, 'command':['python3 create_baseline_jobs/main.py']},
        log_group_config={'log_group_name':f'/ecs/{family}'},
        log_driver_config={},
        task_config={'environment':environment, 'cluster':cluster, 'network':network}
    )


def get_mq_bl(
    scope, 
    construct_id, 
    family,
    repo,
    monitor_role, 
    monitor_dir,
    cluster,
    network,
    baseline_full_dataset_lkp,
    target_label,
    predict_label,
    problem_type,
    probability_attribute={}, # Classification Only,
    probability_threshold_attribute={},  # Classification Only
    instance_count=1,
    instance_type='ml.m5.xlarge',
    volume_size_in_gb=20,
    max_runtime_in_seconds=3600
):

    environment={
        'monitor_type':'ModelQuality',
        'role':monitor_role.role_arn,
        'baseline_full_dataset':stepfunctions.JsonPath.string_at(baseline_full_dataset_lkp),
        'output_s3_uri':monitor_dir+'/info',
        'target_label':target_label,
        'predict_label':predict_label,
        'problem_type':problem_type,
        'dataset_format':json.dumps({'csv': {'header': True}}),
        'instance_count':str(instance_count),
        'instance_type':instance_type,
        'volume_size_in_gb':str(volume_size_in_gb),
        'max_runtime_in_seconds':str(max_runtime_in_seconds),
        'probability_attribute':json.dumps(probability_attribute),
        'probability_threshold_attribute':json.dumps(probability_threshold_attribute),
        'execution_id':stepfunctions.JsonPath.string_at('$$.Execution.Name')
    }

    return ECSFargate(
        scope, 
        construct_id,
        task_def_config={'family':family, 'execution_role':monitor_role, 'task_role':monitor_role, 'repo':repo, 'command':['python3 create_baseline_jobs/main.py']},
        log_group_config={'log_group_name':f'/ecs/{family}'},
        log_driver_config={},
        task_config={'environment':environment, 'cluster':cluster, 'network':network}
    )

def get_mb_bl(
    scope, 
    construct_id, 
    family, 
    repo,
    model_name_lkp,
    monitor_role,
    monitor_dir,
    cluster,
    network,
    baseline_full_dataset_lkp,
    target_label,
    predict_label,
    instance_count=1,
    instance_type='ml.m5.xlarge',
    max_runtime_in_seconds=3600,
    model_predicted_label_config={},
    bias_config={},
    content_type='text/csv',
    probability_attribute={},
    probability_threshold_attribute={}
):
    environment={
        'monitor_type':'ModelBias',
        'role':monitor_role.role_arn,
        'model_name':stepfunctions.JsonPath.string_at(model_name_lkp),
        'baseline_full_dataset':stepfunctions.JsonPath.string_at(baseline_full_dataset_lkp),
        'output_s3_uri':monitor_dir+'/info',
        'target_label':target_label,
        'predict_label':predict_label,
        'bias_config':json.dumps(bias_config),
        'model_predicted_label_config':json.dumps(model_predicted_label_config),
        'content_type':content_type,
        'instance_count':str(instance_count),
        'instance_type':instance_type,
        'max_runtime_in_seconds':str(max_runtime_in_seconds),
        'probability_attribute':json.dumps(probability_attribute),
        'probability_threshold_attribute':json.dumps(probability_threshold_attribute),
        'execution_id':stepfunctions.JsonPath.string_at('$$.Execution.Name')
    }

    return ECSFargate(
        scope, 
        construct_id,
        task_def_config={'family':family, 'execution_role':monitor_role, 'task_role':monitor_role, 'repo':repo, 'command':['python3 create_baseline_jobs/main.py']},
        log_group_config={'log_group_name':f'/ecs/{family}'},
        log_driver_config={},
        task_config={'environment':environment, 'cluster':cluster, 'network':network}
    )


def get_me_bl(
    scope, 
    construct_id, 
    family,
    repo,
    model_name_lkp,
    monitor_role,
    monitor_dir,
    cluster,
    network,
    baseline_full_dataset_lkp,
    target_label,
    predict_label,
    content_type='text/csv',
    instance_count=1,
    instance_type='ml.m5.xlarge',
    max_runtime_in_seconds=3600,
    num_samples=100
):
    environment={
        'monitor_type':'ModelExplainability',
        'role':monitor_role.role_arn,
        'model_name':stepfunctions.JsonPath.string_at(model_name_lkp),
        'baseline_full_dataset':stepfunctions.JsonPath.string_at(baseline_full_dataset_lkp),
        'output_s3_uri':monitor_dir+'/info',
        'target_label':target_label,
        'predict_label':predict_label,
        'num_samples':str(num_samples),
        'agg_method':'mean_sq',
        'content_type':content_type,
        'instance_count':str(instance_count),
        'instance_type':instance_type,
        'max_runtime_in_seconds':str(max_runtime_in_seconds),
        'execution_id':stepfunctions.JsonPath.string_at('$$.Execution.Name')
    }

    return ECSFargate(
        scope, 
        construct_id,
        task_def_config={'family':family, 'execution_role':monitor_role, 'task_role':monitor_role, 'repo':repo, 'command':['python3 create_baseline_jobs/main.py']},
        log_group_config={'log_group_name':f'/ecs/{family}'},
        log_driver_config={},
        task_config={'environment':environment, 'cluster':cluster, 'network':network}
    )