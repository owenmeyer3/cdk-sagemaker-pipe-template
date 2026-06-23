import os, pathlib
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
from constructs import Construct
from custom_constructs.CNetwork import CNetwork
from custom_constructs.CLambda import CLambdaFunction
from custom_constructs.CECS import CFargateTaskDefinition
from custom_constructs.utils import get_local_project_root

class SagemakerPipeTemplateStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, project_config:dict, env_config:dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Build Params
        self.sagemaker_session = sagemaker_session
        self.region_name = project_config['region_name']
        self.name = project_config['name']
        self.deploy_type=project_config['deploy_type']
        self.target_name=project_config['target_name']
        self.target_type = project_config['target_type']
        self.problem_type=project_config['problem_type']
        self.prediction_name=project_config['prediction_name']
        self.ground_truth_label=project_config['ground_truth_label']
        self.model_package_group_name=project_config['model_package_group_name']
        self.lambda_execution_role_arn=env_config['lambda_execution_role_arn']
        self.other_execution_role_arn=env_config['other_execution_role_arn']
        self.pipeline_bucket=env_config['pipeline_bucket']

        # Runtime Params
        # self.model_package_version_param =                  ParameterInteger(name='ModelPackageVersion',                 default_value=1)
        # self.action_param =                                 ParameterString( name='Action',                              default_value='deploy', enum_values=['deploy', 'inference'])
        # self.baseline_file_param =                          ParameterString( name='BaselineFile',                        default_value='aaa')
        # self.monitor_instance_type_param =                  ParameterString( name='MonitorInstanceType',                 default_value='ml.m5.large')
        # self.endpoint_instance_type_param =                 ParameterString( name='EndpointInstanceType',                default_value='ml.m5.large')
        # self.transform_instance_type_param =                ParameterString( name='TransformInstanceType',               default_value='ml.m5.large')
        # self.fail_on_violation_param =                      ParameterBoolean(name='FailOnViolation',                     default_value=False)
        # self.register_new_baseline_param =                  ParameterBoolean(name='RegisterNewBaseline',                 default_value=False)
        # self.schedule_expression_param =                    ParameterString( name='MonitorScheduleExpression',           default_value='cron(0 * ? * * *)')
        # self.enable_data_quality_monitoring_param =         ParameterBoolean(name='EnableDataQualityMonitoring',         default_value=True)
        # self.enable_model_bias_monitoring_param =           ParameterBoolean(name='EnableModelBiasMonitoring',           default_value=True)
        # self.enable_model_explainability_monitoring_param = ParameterBoolean(name='EnableModelExplainabilityMonitoring', default_value=True)
        # self.enable_model_quality_monitoring_param =        ParameterBoolean(name='EnableModelQualityMonitoring',        default_value=True)
        # self.environment_param =                            ParameterString( name='Environment',                         default_value='dev',     enum_values=['prd', 'dev', 'stg'])
        # self.sns_topic_arn_param =                          ParameterString( name='SnsTopicArn',                         default_value='aaa')
        # self.enable_sns_notification_param =                ParameterBoolean(name='EnableSnsNotification',               default_value=False)
        # self.ground_truth_dir_param =                       ParameterString( name='GroundTruthDir',                      default_value=f's3://{pipeline_bucket}/ground-truth/{model_package_group_name}')
        # self.batch_input_dir_param =                        ParameterString( name='BatchInputDir',                       default_value=f's3://{pipeline_bucket}/batch_input/{model_package_group_name}')

        # Import existing resources
        self.execution_role=iam.Role.from_role_arn(self, "ImportedExecutionRole", env_config['EXECUTION_ROLE_ARN'], mutable=False)
        self.network = CNetwork(self, "ImportedNetwork", region=env_config['REGION'], vpc_config=env_config['VPC_CONFIG'])
        self.cluster = ecs.Cluster.from_cluster_attributes(self, "ImportedCluster", cluster_name=env_config['CLUSTER_NAME'], vpc=self.network.get_vpc())

        chain = stepfunctions.Pass(self, 'Start')
        sf_launch_schedule = events.Schedule.rate(Duration.hours(1))
        end_pass = stepfunctions.Pass(self, 'End')

        # CHOICES
        rebaseline_choice = stepfunctions.Choice(self, "RebaselineChoice")
        rebaseline_cond = stepfunctions.Condition.string_equals("$.rebaseline", "TRUE")

        deploy_or_inference_choice = stepfunctions.Choice(self, "DeployOrInferenceChoice")
        deploy_or_inference_cond = stepfunctions.Condition.string_equals("$.deploy_or_inference", "deploy")

        schedule_dq_mon_choice = stepfunctions.Choice(self, "ScheduleDqMonChoice")
        schedule_dq_mon_cond = stepfunctions.Condition.string_equals("$.scheduleDqMonChoice", "TRUE")

        schedule_mq_mon_choice = stepfunctions.Choice(self, "ScheduleMqMonChoice")
        schedule_mq_mon_cond = stepfunctions.Condition.string_equals("$.scheduleMqMonChoice", "TRUE")
        
        schedule_me_mon_choice = stepfunctions.Choice(self, "ScheduleMeMonChoice")
        schedule_me_mon_cond = stepfunctions.Condition.string_equals("$.scheduleMeMonChoice", "TRUE")
        
        schedule_mb_mon_choice = stepfunctions.Choice(self, "ScheduleMbMonChoice")
        schedule_mb_mon_cond = stepfunctions.Condition.string_equals("$.scheduleMbMonChoice", "TRUE")

        get_or_create_model_from_registry_task, get_or_create_model_from_registry_function = self.get_get_or_create_model_from_registry_fn_task()
        prep_baseline_sets_task, prep_baseline_sets_function = self.prep_baseline_sets_fn_task()

        # TASKS

        prep_baseline_step = None
        baseline_transform_step = None
        get_baseline_preds_step = None
        make_baseline_step = None

        schedule_dq_task = None
        schedule_mq_task = None
        schedule_me_task = None
        schedule_mb_task = None

        check_dq_task = None
        check_mq_task = None
        check_me_task = None
        check_mb_task = None

        deploy_endpoint_task = None
        batch_transform_task = None

        # MAPS
        ### SCHEDULE MONITOR MAP ###
        schedule_monitor_map = None
        # schedule_monitor_map_end_pass = stepfunctions.Pass(self, 'ScheduleMonitorMapEnd')
        # schedule_monitor_map = stepfunctions.Map(self, "ScheduleMonitorMap",
        #     max_concurrency=1,
        #     items_path=stepfunctions.JsonPath.string_at("$.monitors_to_schedule"),
        #     item_selector={
        #         "item": stepfunctions.JsonPath.string_at("$.Map.Item.Value")
        #     },
        #     result_path="$.scheduleMonitorMapOutput"
        # )
        # schedule_map_chain = stepfunctions.choice(self, "ScheduleMonitorMapChoice") \
        #     .when(stepfunctions.Condition.string_equals('$.item', 'SCHEDULE_DQ'), \
        #         schedule_dq_task \
        #     ).when(stepfunctions.Condition.string_equals('$.item', 'SCHEDULE_MQ'), \
        #         schedule_mq_task \                                      
        #     ).when(stepfunctions.Condition.string_equals('$.item', 'SCHEDULE_ME'), \
        #         schedule_me_task \   
        #     ).when(stepfunctions.Condition.string_equals('$.item', 'SCHEDULE_MB'), \
        #         schedule_mb_task \                                          
        #     ).afterwards().next(schedule_monitor_map_end_pass)
        # schedule_monitor_map.item_processor(schedule_map_chain)

        ### MONITOR_CHECK_MAP ###
        check_monitor_map = None
        # check_monitor_map_end_pass = stepfunctions.Pass(self, 'MonitorCheckMapEnd')
        # check_monitor_map = stepfunctions.Map(self, "CheckMonitorMap",
        #     max_concurrency=1,
        #     items_path=stepfunctions.JsonPath.string_at("$.monitors_to_check"),
        #     item_selector={
        #         "item": stepfunctions.JsonPath.string_at("$.Map.Item.Value")
        #     },
        #     result_path="$.checkMonitorMapOutput"
        # )
        # check_map_chain = stepfunctions.choice(self, "CheckMonitorMapChoice") \
        #     .when(stepfunctions.Condition.string_equals('$.item', 'CHECK_DQ'), \
        #         check_dq_task \
        #     ).when(stepfunctions.Condition.string_equals('$.item', 'CHECK_MQ'), \
        #         check_mq_task \                                      
        #     ).when(stepfunctions.Condition.string_equals('$.item', 'CHECK_ME'), \
        #         check_me_task \   
        #     ).when(stepfunctions.Condition.string_equals('$.item', 'CHECK_MB'), \
        #         check_mb_task \                                          
        #     ).afterwards().next(schedule_monitor_map_end_pass)
        # check_monitor_map.item_processor(check_map_chain)

        # CHAINS
        ### baseline_chain ###
        baseline_chain = None
        # baseline_chain = prep_baseline_step.next(baseline_transform_step).next(get_baseline_preds_step).next(make_baseline_step)
        ### deploy_chain ###
        deploy_chain = None
        # if(self.deploy_type == 'realtime'):
        #     deploy_chain = deploy_endpoint_task.next(schedule_monitor_map)
        # else:
        #     deploy_chain = schedule_monitor_map
        ### inference_chain ###
        inference_chain = None
        # if(self.deploy_type == 'realtime'):
        #     inference_chain = check_monitor_map
        # else:
        #     inference_chain = batch_transform_task.next(check_monitor_map)



        # Make state machine
        chain.next(get_or_create_model_from_registry_task) \
            .next( \
                rebaseline_choice.when(rebaseline_cond, baseline_chain) \
            ).next( \
                deploy_or_inference_choice.when(deploy_or_inference_cond, \
                    deploy_chain \
                ).otherwise( \
                    inference_chain
                ) \
            ).next(end_pass)                   

        state_machine=stepfunctions.StateMachine(
            self, "SM",
            definition_body=stepfunctions.DefinitionBody.from_chainable(chain),
            role=self.execution_role,
            logs=stepfunctions.LogOptions(
                destination=logs.LogGroup(
                    self, "SMLog",
                    log_group_name=f"/ML/{self.env_config['ENV']}/states/{project_config['NAME']}",
                    removal_policy=RemovalPolicy.DESTROY,
                    retention=logs.RetentiuonDays.ONE_MONTH
                ),
                level=stepfunctions.LogLevel.ALL, 
                include_execution_data=True
            )
        )



        get_or_create_model_from_registry_function.add_invoker_arn(state_machine.state_machine_arn)
        prep_baseline_sets_function.add_invoker_arn(state_machine.state_machine_arn)
        sf_launch_rule = events.Rule(self, "Rule", 
            schedule=sf_launch_schedule,
            targets=[
                targets.SfnStateMachine(
                    state_machine,
                    role=self.execution_role,
                    input=events.RuleTargetInput.from_object({"event":events.EventField.from_path("$")})
                )
            ]
        )

    def get_get_or_create_model_from_registry_fn_task(self):
        function_name = "get_or_create_model_from_registry",
        lambda_function = CLambdaFunction(
            self, "GetOrCreateModelFromRegistry",
            use_docker=False,
            function_name=function_name,
            code_path='code/get_or_create_model_from_registry',
            handler='get_or_create_model_from_registry.handler',
            role=self.execution_role,
            log_group_name=f"/ML/{self.env_config['ENV']}/lambda/{function_name}",
            log_retention=logs.RetentionDays.ONE_MONTH
        )

        task = lambda_function.generate_task(
            payload={
                'model_package_group_name': self.model_package_group_name,
                'model_package_version': self.model_package_version_param
            },
            # result_selector={}
            # outputs=[
            #     LambdaOutput(output_name='model_name', output_type=LambdaOutputTypeEnum.String),
            #     LambdaOutput(output_name='model_package_arn', output_type=LambdaOutputTypeEnum.String)
            # ]
        )
        return [task, lambda_function]


    def prep_baseline_sets_fn_task(self):
        function_name = "prep_baseline_sets",
        lambda_function = CLambdaFunction(
            self, "PrepBaselineSets",
            use_docker=False,
            function_name=function_name,
            code_path='code/baselining',
            handler='baselining.prep_baseline_sets_handler',
            role=self.execution_role,
            log_group_name=f"/ML/{self.env_config['ENV']}/lambda/{function_name}",
            log_retention=logs.RetentionDays.ONE_MONTH
        )

        task = lambda_function.generate_task(
            payload={
                'baseline_file': baseline_file,
                'target_name':target_name,
                'target_type': target_type,
                'baseline_X_file_dest_dir':''
            },
            # result_selector={}
            # outputs=[
            #     LambdaOutput(output_name='baseline_X_dir', output_type=LambdaOutputTypeEnum.String),
            #     LambdaOutput(output_name='baseline_X_file', output_type=LambdaOutputTypeEnum.String)
            #     LambdaOutput(output_name='baseline_X_filename', output_type=LambdaOutputTypeEnum.String)
            # ]
        )
        return [task, lambda_function]

        
