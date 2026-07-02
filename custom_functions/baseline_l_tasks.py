import os, pathlib, json
from aws_cdk import (
    aws_logs as logs,
    aws_lambda as _lambda,
    aws_stepfunctions as stepfunctions,
    aws_stepfunctions_tasks as tasks,
    aws_ecr as ecr,
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
            execution_role=None, 
            task_role=None,
            repo=None,
            log_group_name=None,
            log_retention=None,
            environment={},
        ):

        super().__init__(
            scope, 
            construct_id, 
            family=family,
            execution_role=execution_role,
            task_role=task_role
        )

        logging=ecs.LogDriver(
            log_group=logs.LogGroup(
                self, 
                f"{self.node.id}Log", 
                log_group_name=log_group_name, 
                retention=log_retention, 
                removal_policy=RemovalPolicy.DESTROY), 
                stream_prefix="ecs", 
                mode=ecs.AwsLogDriverMode.NON_BLOCKING
        )
        
        container_definition=self.add_container(
            f"{self.node.id}Cntr",
            container_name=self.family,
            image=ecs.ContainerImage.from_ecr_repository(repo, tag='latest'),
            entry_point=["bash", "-c"],
            command=[],
            logging=logging,
            environment=environment
        )

class ELambdaFunction(Construct):
    def __init__(
        self, 
        scope: Construct, 
        construct_id: str,
        function_name:str,
        repo:ecr.Repository,
        cmd:list=[],
        outputs=[],
        payload={},
        log_group_name='',
        log_retention='',
        task_timeout=stepfunctions.Timeout.duration(Duration.minutes(10)),
        function_timeout=Duration.minutes(15)
    ):
        print(f'repo {repo}')

        super().__init__(scope, construct_id)

        task_timeout=stepfunctions.Timeout.duration(Duration.minutes(15))
        function_timeout=stepfunctions.Timeout.duration(Duration.minutes(15))
        self.task_log_group = logs.LogGroup(self, f"{self.node.id}Log", log_group_name=log_group_name, retention=log_retention, removal_policy=RemovalPolicy.DESTROY)

        self.fn = _lambda.DockerImageFunction(
            self, f'{construct_id}Lambda',
            code=_lambda.DockerImageCode.from_ecr(
                repository=repo,
                tag_or_digest='latest',
                cmd=cmd
            ),
            role=scope.lambda_execution_role_arn,
            function_name=function_name,
            timeout=function_timeout,
            memory_size=512,
            log_group=self.task_log_group
        )

        print(f"outputs: {outputs}")

        result_selection={}
        for o in outputs:
            result_selection[f'{o}.$'] = f'$.Payload.{o}'
        
        print(f"result_selection: {result_selection}")

        print(payload)

        self.task = tasks.LambdaInvoke(
            self, 
            f"{construct_id}Task", 
            lambda_function=self.fn, 
            payload=stepfunctions.TaskInput.from_object(payload), 
            task_timeout=task_timeout, 
            retry_on_service_exceptions=False,
            result_selector=result_selection,
            result_path=f"$.{self.node.id}Task",
        )

def run_dq_bl_job_fn_task(
    scope, 
    construct_id, 
    function_name, 
    repo,
    monitor_role, 
    monitor_dir,
    execution_id_lkp
):
    print(f'repo {repo}')
    payload={
        'role':monitor_role.role_arn,
        'baseline_dataset':monitor_dir+'/baseline.csv',
        'output_s3_uri':monitor_dir+'/info',
        'execution_id':stepfunctions.JsonPath.string_at(execution_id_lkp)
    }

    e = ELambdaFunction(
        scope, 
        construct_id, 
        function_name, 
        repo, 
        cmd=['baseline_lambda.main.create_dq_baseline_handler'],
        payload=payload, 
        log_group_name=f'/lambda/{function_name}', 
        log_retention=logs.RetentionDays.ONE_MONTH,
        outputs=[]
    )

    return [e.task, e.fn]


def run_mq_bl_job_fn_task(
    scope, 
    construct_id, 
    function_name, 
    repo,
    monitor_role, 
    monitor_dir,
    execution_id_lkp,
    inference_attribute,
    ground_truth_attribute,
    problem_type,
    probability_attribute=None, # Classification Only,
    probability_threshold_attribute=None,  # Classification Only
):
    payload={
        'role':monitor_role.role_arn,
        'baseline_dataset':monitor_dir+'/baseline.csv',
        'output_s3_uri':monitor_dir+'/info',
        'problem_type':problem_type,
        'inference_attribute':inference_attribute,
        'probability_attribute':probability_attribute,
        'ground_truth_attribute':ground_truth_attribute,
        'probability_threshold_attribute':probability_threshold_attribute,
        'execution_id':stepfunctions.JsonPath.string_at(execution_id_lkp)
    }

    e = ELambdaFunction(
        scope, 
        construct_id, 
        function_name, 
        repo, 
        cmd=['baseline_lambda.main.create_mq_baseline_handler'],
        payload=payload, 
        log_group_name=f'/lambda/{function_name}', 
        log_retention=logs.RetentionDays.ONE_MONTH,
        outputs=[]
    )

    return [e.task, e.fn]

def run_mb_bl_job_fn_task(
    scope, 
    construct_id, 
    function_name, 
    model_name_lkp,
    repo,
    monitor_role,
    monitor_dir,
    execution_id_lkp,
    label
):
    payload={
        'role':monitor_role.role_arn,
        'model_name':stepfunctions.JsonPath.string_at(model_name_lkp),
        'baseline_dataset':monitor_dir+'/baseline.csv',
        'output_s3_uri':monitor_dir+'/info',
        'label':label,
        'bias_config':{'label_values_or_threshold':[1], 'function':"sex_M", 'facet_values_or_threshold':[0.5]},
        'model_predicted_label_config':{'probability_threshold':0.8},
        'execution_id':stepfunctions.JsonPath.string_at(execution_id_lkp)
    }

    e = ELambdaFunction(
        scope, 
        construct_id, 
        function_name, 
        repo, 
        cmd=['baseline_lambda.main.create_mb_baseline_handler'],
        payload=payload, 
        log_group_name=f'/lambda/{function_name}', 
        log_retention=logs.RetentionDays.ONE_MONTH,
        outputs=[]
    )

    return [e.task, e.fn]


def run_me_bl_job_fn_task(
    scope, 
    construct_id, 
    function_name,
    repo,
    model_name_lkp,
    monitor_role, 
    monitor_dir,
    execution_id_lkp,
    label,
    baseline_cols_lkp,
    test_X_dataset_lkp
):
    
    payload={
        'role':monitor_role.role_arn,
        'model_name':stepfunctions.JsonPath.string_at(model_name_lkp),
        'baseline_dataset':monitor_dir+'/baseline.csv',
        'output_s3_uri':monitor_dir+'/info',
        'label':label,
        'baseline_cols':stepfunctions.JsonPath.string_at(baseline_cols_lkp),
        'test_X_dataset':stepfunctions.JsonPath.string_at(test_X_dataset_lkp),
        'execution_id':stepfunctions.JsonPath.string_at(execution_id_lkp)
    }

    e = ELambdaFunction(
        scope, 
        construct_id, 
        function_name, 
        repo, 
        cmd=['baseline_lambda.main.create_dq_baseline_handler'],
        payload=payload, 
        log_group_name=f'/lambda/{function_name}', 
        log_retention=logs.RetentionDays.ONE_MONTH,
        outputs=[]
    )

    return [e.task, e.fn]