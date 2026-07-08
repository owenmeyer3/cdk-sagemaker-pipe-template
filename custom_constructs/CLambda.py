from aws_cdk import (
    RemovalPolicy,
    Duration,
    aws_lambda as _lambda,
    aws_logs as logs,
    aws_stepfunctions as stepfunctions,
    aws_stepfunctions_tasks as tasks,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_ecr as ecr
)
from constructs import Construct
from custom_constructs.utils import get_local_project_root

_MISSING = object()
class Lambda(Construct):
    def __init__(
        self, 
        scope: Construct, 
        construct_id: str,
        function_config:dict,
        log_config=_MISSING,
        task_config=_MISSING
    ):
        super().__init__(scope, construct_id)
        
        # LOGS
        if log_config is not _MISSING:
            log_group_name=log_config.pop('log_group_name', None)
            retention=log_config.pop('retention', logs.RetentionDays.ONE_MONTH)
            removal_policy=log_config.pop('removal_policy', RemovalPolicy.DESTROY)
            self.log_group = logs.LogGroup(self, f"{construct_id}Log", log_group_name=log_group_name, retention=retention, removal_policy=removal_policy, **log_config)
        else:
            print('forgoing logging due to missing log_config in ELambda args')
            self.task_log_group = None

        # Function
        assert function_config and function_config['type'] in ['ExistingImage', 'Basic', 'NewImage'], 'function_config[type] must be ExistingImage || Basic || NewImage'
        _type=function_config.pop('type')
        if _type == 'ExistingImage':
            repo=function_config.pop('repo')
            tag=function_config.pop('tag', 'latest')
            function_name=function_config.pop('function_name')
            role=function_config.pop('role')
            cmd=function_config.pop('cmd', [])
            timeout=function_config.pop('timeout', Duration.minutes(15))

            self.fn = _lambda.DockerImageFunction(
                self, f'{construct_id}Fn',
                function_name=function_name,
                code=_lambda.DockerImageCode.from_ecr(repository=repo, tag_or_digest=tag, cmd=cmd),
                role=role,
                log_group=self.log_group,
                timeout=timeout,
                **function_config
            )
        elif _type == 'Basic':
            code_path=function_config.pop('code_path')
            handler=function_config.pop('handler')
            runtime=function_config.pop('runtime')
            function_name=function_config.pop('function_name')
            role=function_config.pop('role')
            timeout=function_config.pop('timeout', Duration.minutes(15))
            self.fn=_lambda.Function(
                self, f"{self.node.id}Fn",
                function_name=function_name,
                role=role,
                handler=handler, 
                runtime=_lambda.Runtime(runtime), 
                code = _lambda.Code.from_asset(code_path), 
                timeout=timeout,
                **function_config
            )
        elif _type == 'NewImage': 
            image_asset=function_config.pop('image_asset')
            tag=function_config.pop('tag', 'latest')
            function_name=function_config.pop('function_name')
            role=function_config.pop('role')
            cmd=function_config.pop('cmd', [])
            timeout=function_config.pop('timeout', Duration.minutes(15))
            entrypoint = function_config.pop('entrypoint', ['python3', '-m', 'awslambdaric'])
            self.fn = _lambda.DockerImageFunction(
                self, f'{construct_id}Fn',
                function_name=function_name,
                code=_lambda.DockerImageCode(repository=image_asset.repository, tag_or_digest=image_asset.image_tag, entrypoint=entrypoint),
                role=role,
                log_group=self.task_log_group,
                timeout=timeout,
                **function_config
            )

        # Task
        if task_config is not _MISSING:
            outputs=task_config.pop('outputs', [])
            payload=task_config.pop('payload', {})
            task_timeout=task_config.pop('timeout', Duration.minutes(15))
            task_timeout=task_config.pop('task_timeout', Duration.minutes(15))
            retry_on_service_exceptions=task_config.pop('retry_on_service_exceptions', False)
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
                task_timeout=stepfunctions.Timeout.duration(task_timeout),
                retry_on_service_exceptions=retry_on_service_exceptions,
                result_selector=result_selection,
                result_path=f"$.{construct_id}Task",
            )
        else:
            print('forgoing task due to missing task_config in ELambda args')
            self.task = None

    def add_invoker_arn(self, invoker_arn, **kwargs):
        # Add policy to lambda to allow stepfunction to invoke
        _lambda.CfnPermission(
            self, f"{self.node.id}InvkPerm",
            action="lambda:InvokeFunction",
            function_name=self.fn.function_name,
            principal="states.amazonaws.com",
            source_arn=invoker_arn,
            **kwargs
        )

# class CLambdaFunction(Construct):
#     def __init__(self, scope: Construct, construct_id: str, use_docker:bool, log_group_name:str, log_retention:str, **kwargs):
#         #### Usage ####
#         # https://docs.aws.amazon.com/cdk/api/v2/python/aws_cdk.aws_lambda/DockerImageFunction.html
#         # https://docs.aws.amazon.com/cdk/api/v2/python/aws_cdk.aws_lambda/Function.html
#         # CLambdaFunction(
#         #     # Required
#         #     scope, construct_id, use_docker,
#         #     # Common
#         #     log_group_name & log_retention || log_group
#         #     # Uncommon - other DockerImageFunction or Function kwargs
#         # )
#         ###############

#         super().__init__(scope, construct_id)

#         # add log group
#         if 'log_group' in kwargs:
#             pass
#         elif 'log_group_name' in kwargs != 'log_retention' in kwargs:
#             raise ValueError("if log_group_name or log_retention are arguments, both must be arguments")
#         elif 'log_group_name' in kwargs:
#             log_group_name=kwargs.pop('log_group_name', None)
#             log_retention=kwargs.pop('log_retention', None)
#             self.task_log_group = logs.LogGroup(self, f"{self.node.id}Log", log_group_name=log_group_name, log_retention=log_retention, removal_policy=RemovalPolicy.DESTROY)
#             kwargs['log_group']=self.task_log_group
        
#         if use_docker:
#             image_code=None
#             if 'dockerfile' not in kwargs == 'image_asset' not in kwargs:
#                 raise ValueError("dockerfile or image_asset argument must be specified (not neither or both) when using a docker lambda function")
#             dockerfile=kwargs.pop('dockerfile', None)
#             image_asset=kwargs.pop('image_asset', None)
#             build_args=kwargs.pop('build_args', {})
#             if dockerfile:
#                 image_code=_lambda.DockerImageCode.from_image_asset(directory=get_local_project_root(), file=dockerfile, build_args=build_args)
#             else:
#                 entrypoint = kwargs.pop('entrypoint', ['python3', '-m', 'awslambdaric'])
#                 image_code=_lambda.DockerImageCode(repository=image_asset.repository, tag_or_digest=image_asset.image_tag, entrypoint=entrypoint)
#             self.fn=_lambda.DockerImageFunction(self, f"{self.node.id}Fn", code=image_code, **kwargs)
#         else:
#             if 'code_path' not in kwargs or 'handler' not in kwargs or 'runtime' not in kwargs:
#                 raise ValueError("if code_path, handler and runtime msut exist for lambda function when not using docker")
#             code_path=kwargs.pop('code_path', None)
#             handler=kwargs.pop('handler', None)
#             runtime=kwargs.pop('runtime', 'python3.11')
#             # self.fn=_lambda.Function(self, f"{self.node.id}Fn",runtime=runtime,handler=handler, code = _lambda.Code.from_asset(code_path), **kwargs)
#             self.fn=_lambda.Function(self, f"{self.node.id}Fn", runtime=_lambda.Runtime(runtime), handler=handler, code = _lambda.Code.from_asset(code_path), **kwargs)

    
    
#     def generate_task(self, payload:dict={}, outputs=[], **kwargs) -> tasks.LambdaInvoke:
#         task_timeout=kwargs.pop('task_timeout', stepfunctions.Timeout.duration(Duration.minutes(10)))

#         print(f"outputs: {outputs}")

#         result_selection={}
#         for o in outputs:
#             result_selection[f'{o}.$'] = f'$.Payload.{o}'
        
#         print(f"result_selection: {result_selection}")

#         print(payload)

#         task = tasks.LambdaInvoke(
#             self, f"{self.node.id}Task", 
#             lambda_function=self.fn, 
#             payload=stepfunctions.TaskInput.from_object(payload), 
#             task_timeout=task_timeout, 
#             retry_on_service_exceptions=False,
#             result_selector=result_selection,
#             result_path=f"$.{self.node.id}Task",
#             **kwargs
#             )

#         return task
    
#     def add_invoker_arn(self, invoker_arn, **kwargs):
#         # Add policy to lambda to allow stepfunction to invoke
#         _lambda.CfnPermission(
#             self, f"{self.node.id}InvkPerm",
#             action="lambda:InvokeFunction",
#             function_name=self.fn.function_name,
#             principal="states.amazonaws.com",
#             source_arn=invoker_arn,
#             **kwargs
#         )