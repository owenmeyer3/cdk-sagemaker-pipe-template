import os, pathlib, json
import aws_cdk
from aws_cdk import (
    aws_logs as logs,
    aws_lambda as _lambda,
    aws_stepfunctions as stepfunctions,
    aws_stepfunctions_tasks as tasks,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    Duration,
    RemovalPolicy
)
from custom_constructs.CNetwork import CNetwork
from custom_constructs.CLambda import CLambdaFunction
from custom_constructs.CECS import CFargateTaskDefinition
from custom_constructs.utils import get_local_project_root
from constructs import Construct

class ETaskDefinition(ecs.FargateTaskDefinition):
    def __init__(self, 
        scope: Construct, 
        construct_id: str, 
        family:str,
        repo=None,
        execution_role=None, 
        task_role=None,
        log_group_name=None,
        log_retention=None,
        command=[],
        environment={},
    ):
        super().__init__(
            scope, 
            construct_id, 
            family=family,
            execution_role=execution_role,
            task_role=task_role
        )
        self.environment=environment

        log_driver=ecs.AwsLogDriver(
            log_group=logs.LogGroup(
                self, 
                f"{self.node.id}Log", 
                log_group_name=log_group_name, 
                retention=log_retention, 
                removal_policy=RemovalPolicy.DESTROY), 
                stream_prefix="ecs", 
                mode=ecs.AwsLogDriverMode.NON_BLOCKING
        )
        
        self.container_definition=self.add_container(
            f"{self.node.id}Cntr",
            container_name=self.family,
            image=ecs.ContainerImage.from_ecr_repository(repo, tag='latest'),
            entry_point=["bash", "-c"],
            command=command,
            logging=log_driver
        )
    def get_task(self, cluster):
        print(self.environment)
        env_list = [tasks.TaskEnvironmentVariable(name=k, value=v) for k, v in self.environment.items()]
        env_list.append(tasks.TaskEnvironmentVariable(name='TASK_TOKEN', value=stepfunctions.JsonPath.task_token))

        # env_list = [
        #     tasks.TaskEnvironmentVariable(
        #         name=k,
        #         value=stepfunctions.JsonPath.json_to_string(v) if aws_cdk.Token.is_unresolved(v)
        #         else json.dumps(v) if isinstance(v, (list, dict))
        #         else str(v)
        #     )
        #     for k, v in self.environment.items()
        # ]
        # env_list.append(tasks.TaskEnvironmentVariable(name='TASK_TOKEN', value=stepfunctions.JsonPath.task_token))

        

        return tasks.EcsRunTask(
            self,
            f"{self.node.id}Task",
            task_definition=self,
            task_timeout=stepfunctions.Timeout.duration(Duration.minutes(10)),
            cluster=cluster,
            launch_target=tasks.EcsFargateLaunchTarget(platform_version=ecs.FargatePlatformVersion(ecs.FargatePlatformVersion.LATEST)),
            integration_pattern=stepfunctions.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            container_overrides=[
                tasks.ContainerOverride(container_definition=self.container_definition, environment=env_list)
            ],
            assign_public_ip=True,
            subnets=self.node.scope.public_subnet_selection
        )

def run_dq_bl_job_fn_task(
    scope, 
    construct_id, 
    family, 
    repo,
    monitor_role,
    monitor_dir,
    execution_id_lkp,
    instance_count=1,
    instance_type='ml.m5.xlarge',
    volume_size_in_gb=20,
    max_runtime_in_seconds=3600
):
    environment={
        'monitor_type':'DataQuality',
        'role':monitor_role.role_arn,
        'baseline_dataset':monitor_dir+'/baseline.csv',
        'output_s3_uri':monitor_dir+'/info',
        'dataset_format':json.dumps({'csv': {'header': True}}),
        'instance_count':str(instance_count),
        'instance_type':instance_type,
        'volume_size_in_gb':str(volume_size_in_gb),
        'max_runtime_in_seconds':str(max_runtime_in_seconds),
        'execution_id':stepfunctions.JsonPath.string_at(execution_id_lkp)
    }
    
    e = ETaskDefinition(
        scope, 
        construct_id, 
        family,
        repo, 
        execution_role=monitor_role, 
        task_role=monitor_role,
        log_group_name=f'/ecs/{family}',
        log_retention=logs.RetentionDays.ONE_MONTH,
        command=['python3', 'baseline_ecs/main.py'],
        environment=environment
    )

    return e


def run_mq_bl_job_fn_task(
    scope, 
    construct_id, 
    family,
    repo,
    monitor_role, 
    monitor_dir,
    execution_id_lkp,
    inference_attribute,
    ground_truth_attribute,
    problem_type,
    probability_attribute=None, # Classification Only,
    probability_threshold_attribute=None,  # Classification Only
    instance_count=1,
    instance_type='ml.m5.xlarge',
    volume_size_in_gb=20,
    max_runtime_in_seconds=3600
):
    environment={
        'monitor_type':'ModelQuality',
        'role':monitor_role.role_arn,
        'baseline_dataset':monitor_dir+'/baseline.csv',
        'output_s3_uri':monitor_dir+'/info',
        'problem_type':problem_type,
        'inference_attribute':inference_attribute,
        'ground_truth_attribute':ground_truth_attribute,
        'dataset_format':json.dumps({'csv': {'header': True}}),
        'instance_count':str(instance_count),
        'instance_type':instance_type,
        'volume_size_in_gb':str(volume_size_in_gb),
        'max_runtime_in_seconds':str(max_runtime_in_seconds),
        'execution_id':stepfunctions.JsonPath.string_at(execution_id_lkp)
    }

    if probability_attribute: environment['probability_attribute'] = json.dumps(probability_attribute)
    if probability_threshold_attribute: environment['probability_threshold_attribute'] = json.dumps(probability_threshold_attribute)

    e = ETaskDefinition(
        scope, 
        construct_id, 
        family,
        repo, 
        execution_role=monitor_role, 
        task_role=monitor_role,
        log_group_name=f'/ecs/{family}',
        log_retention=logs.RetentionDays.ONE_MONTH,
        command=['python3', 'baseline_ecs/main.py'],
        environment=environment
    )

    return e

def run_mb_bl_job_fn_task(
    scope, 
    construct_id, 
    family, 
    repo,
    model_name_lkp,
    monitor_role,
    monitor_dir,
    execution_id_lkp,
    label,
    instance_count=1,
    instance_type='ml.m5.xlarge',
    volume_size_in_gb=20,
    max_runtime_in_seconds=3600,
    model_predicted_label_config=None,
    bias_config=None,
    content_type='text/csv',
    probability_attribute=None,
    probability_threshold_attribute=None
):
    model_predicted_label_config={'probability_threshold':0.8}
    bias_config = {'label_values_or_threshold':[1], 'function':"Account Length", 'facet_values_or_threshold':[100]}

    json.dumps(model_predicted_label_config)
    json.dumps(bias_config)

    environment={
        'monitor_type':'ModelBias',
        'role':monitor_role.role_arn,
        'model_name':stepfunctions.JsonPath.string_at(model_name_lkp),
        'baseline_dataset':monitor_dir+'/baseline.csv',
        'output_s3_uri':monitor_dir+'/info',
        'label':label,
        'bias_config':json.dumps(bias_config),
        'model_predicted_label_config':json.dumps(model_predicted_label_config),
        'content_type':content_type,
        'instance_count':str(instance_count),
        'instance_type':instance_type,
        'volume_size_in_gb':str(volume_size_in_gb),
        'max_runtime_in_seconds':str(max_runtime_in_seconds),
        'execution_id':stepfunctions.JsonPath.string_at(execution_id_lkp)
    }
    if probability_attribute: environment['probability_attribute'] = json.dumps(probability_attribute)
    if probability_threshold_attribute: environment['probability_threshold_attribute'] = json.dumps(probability_threshold_attribute)

    e = ETaskDefinition(
        scope, 
        construct_id, 
        family,
        repo, 
        execution_role=monitor_role, 
        task_role=monitor_role,
        log_group_name=f'/ecs/{family}',
        log_retention=logs.RetentionDays.ONE_MONTH,
        command=['python3', 'baseline_ecs/main.py'],
        environment=environment
    )

    return e


def run_me_bl_job_fn_task(
    scope, 
    construct_id, 
    family,
    repo,
    model_name_lkp,
    monitor_role, 
    monitor_dir,
    execution_id_lkp,
    label,
    baseline_cols_lkp,
    test_X_dataset_lkp,
    instance_count=1,
    instance_type='ml.m5.xlarge',
    volume_size_in_gb=20,
    max_runtime_in_seconds=3600,
    num_samples=100
):
    content_type='text/csv'
    
    environment={
        'monitor_type':'ModelExplainability',
        'role':monitor_role.role_arn,
        'model_name':stepfunctions.JsonPath.string_at(model_name_lkp),
        'baseline_dataset':monitor_dir+'/baseline.csv',
        'output_s3_uri':monitor_dir+'/info',
        'label':label,
        'baseline_cols':stepfunctions.JsonPath.string_at(baseline_cols_lkp),
        'test_X_dataset':stepfunctions.JsonPath.string_at(test_X_dataset_lkp),
        'num_samples':str(num_samples),
        'agg_method':'mean_sq',
        'content_type':content_type,
        'instance_count':str(instance_count),
        'instance_type':instance_type,
        'volume_size_in_gb':str(volume_size_in_gb),
        'max_runtime_in_seconds':str(max_runtime_in_seconds),
        'execution_id':stepfunctions.JsonPath.string_at(execution_id_lkp)
    }

    e = ETaskDefinition(
        scope, 
        construct_id, 
        family,
        repo, 
        execution_role=monitor_role, 
        task_role=monitor_role,
        log_group_name=f'/ecs/{family}',
        log_retention=logs.RetentionDays.ONE_MONTH,
        command=['python3', 'baseline_ecs/main.py'],
        environment=environment
    )

    return e