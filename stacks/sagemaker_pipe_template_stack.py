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
import custom_functions.lambda_tasks as lambda_tasks
import custom_functions.sagemaker_tasks as sagemaker_tasks 

class SagemakerPipeTemplateStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, project_config:dict, env_config:dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Build Params
        self.name = project_config['NAME']
        self.deploy_type=project_config['DEPLOY_TYPE']
        self.target_name=project_config['TARGET_NAME']
        self.target_type = project_config['TARGET_TYPE']
        self.problem_type=project_config['PROBLEM_TYPE']
        self.prediction_name=project_config['PREDICTION_NAME']
        self.ground_truth_label=project_config['GROUND_TRUTH_LABEL']
        self.model_package_group_name=project_config['MODEL_PACKAGE_GROUP_NAME']
        self.lambda_execution_role_arn=env_config['LAMBDA_EXECUTION_ROLE_ARN']
        self.other_execution_role_arn=env_config['OTHER_EXECUTION_ROLE_ARN']
        self.pipeline_bucket=env_config['PIPELINE_BUCKET']
        self.region_name = env_config['REGION_NAME']
        assert os.getenv('ACTION') not in ['deploy', 'inference'], 'ACTION must be in [deploy, inference]'

        self.pipeline_dir =   f's3://{self.pipeline_bucket}/pipelines/{self.model_package_group_name}'
        self.baseline_dir =   f'{self.pipeline_dir}/baseline'
        self.monitors_dir=    f'{self.pipeline_dir}/monitors'
        self.batch_out_dir=   f'{self.pipeline_dir}/batch_out'
        self.data_capture_dir=f'{self.pipeline_dir}/capture'
        self.dq_monitor_dir=  f'{self.pipeline_dir}/data-quality'
        self.mq_monitor_dir=  f'{self.pipeline_dir}/model-quality'
        self.mb_monitor_dir=  f'{self.pipeline_dir}/model-bias'
        self.me_monitor_dir=  f'{self.pipeline_dir}/model-explainability'
        self.db_monitor_dir=  f'{self.pipeline_dir}/data-bias'

        # self.model_package_version =                    os.getenv('MODEL_PACKAGE_VERSION')# :1,
        # self.action_type =                              os.getenv('ACTION')# :'deploy',
        # self.baseline_file =                            os.getenv('BASELINE_FILE')# :'aaa',
        # self.monitor_instance_type =                    os.getenv('MONITOR_INSTANCE_TYPE')# :'ml.m5.large',
        # self.endpoint_instance_type =                   os.getenv('ENDPOINT_INSTANCE_TYPE')# :'ml.m5.large',
        # self.transform_instance_type =                  os.getenv('TRANSFORM_INSTANCE_TYPE')# :'ml.m5.large',
        # self.fail_on_violation =                        os.getenv('FAIL_ON_VIOLATION')# :False,
        # self.register_new_baseline =                     os.getenv('REGISTER_NEW_BASELINE')# :False,
        # self.monitor_schedule_expression =              os.getenv('MONITOR_SCHEDULE_EXPRESSION')# :'cron(0 * ? * * *)',
        # self.enable_data_quality_monitoring =           os.getenv('ENABLE_DATA_QUALITY_MONITORING')# :True,
        # self.enable_model_bias_monitoring =             os.getenv('ENABLE_MODEL_BIAS_MONITORING')# :True,
        # self.enable_model_explainability_monitoring =   os.getenv('ENABLE_MODEL_EXPLAINABILITY_MONITORING')# :True,
        # self.enable_model_quality_monitoring =          os.getenv('ENABLE_MODEL_QUALITY_MONITORING')# :True,
        # self.sns_topic_arn =                            os.getenv('SNS_TOPIC_ARN')# :'aaa',
        # self.enable_sns_notification =                  os.getenv('ENABLE_SNS_NOTIFICATION')# :False,
        # self.ground_truth_dir =                         os.getenv('GROUND_TRUTH_DIR')# :f's3://omm-test-bucket/ground-truth/abalone',
        # self.batch_input_dir =                          os.getenv('BATCH_INPUT_DIR')# :f's3://omm-test-bucket/batch_input/abalone',
        self.model_package_version_lkp = stepfunctions.JsonPath.string_at('$.MODEL_PACKAGE_VERSION')
        self.action_type_lkp = stepfunctions.JsonPath.string_at('$.ACTION')
        self.baseline_file_lkp = stepfunctions.JsonPath.string_at('$.BASELINE_FILE')
        self.monitor_instance_type_lkp = stepfunctions.JsonPath.string_at('$.MONITOR_INSTANCE_TYPE')
        self.endpoint_instance_type_lkp = stepfunctions.JsonPath.string_at('$.ENDPOINT_INSTANCE_TYPE')
        self.transform_instance_type_lkp = stepfunctions.JsonPath.string_at('$.TRANSFORM_INSTANCE_TYPE')
        self.fail_on_violation_lkp = stepfunctions.JsonPath.string_at('$.FAIL_ON_VIOLATION')
        self.monitor_schedule_expression_lkp = stepfunctions.JsonPath.string_at('$.MONITOR_SCHEDULE_EXPRESSION')
        self.enable_data_quality_monitoring_lkp = stepfunctions.JsonPath.string_at('$.ENABLE_DATA_QUALITY_MONITORING')
        self.enable_model_bias_monitoring_lkp = stepfunctions.JsonPath.string_at('$.ENABLE_MODEL_BIAS_MONITORING')
        self.enable_model_explainability_monitoring_lkp = stepfunctions.JsonPath.string_at('$.ENABLE_MODEL_EXPLAINABILITY_MONITORING')
        self.enable_model_quality_monitoring_lkp = stepfunctions.JsonPath.string_at('$.ENABLE_MODEL_QUALITY_MONITORING')
        self.sns_topic_arn_lkp = stepfunctions.JsonPath.string_at('$.SNS_TOPIC_ARN')
        self.enable_sns_notification_lkp = stepfunctions.JsonPath.string_at('$.ENABLE_SNS_NOTIFICATION')
        self.ground_truth_dir_lkp = stepfunctions.JsonPath.string_at('$.GROUND_TRUTH_DIR')
        self.batch_input_dir_lkp = stepfunctions.JsonPath.string_at('$.BATCH_INPUT_DIR')

        # Import existing resources
        self.lambda_execution_role_arn=iam.Role.from_role_arn(self, "ImportedLambdaExecutionRole", env_config['LAMBDA_EXECUTION_ROLE_ARN'], mutable=False)
        self.other_execution_role_arn=iam.Role.from_role_arn(self, "ImportedOtherExecutionRole", env_config['OTHER_EXECUTION_ROLE_ARN'], mutable=False)
        self.network = CNetwork(self, "ImportedNetwork", region=env_config['REGION_NAME'], vpc_config=env_config['VPC_CONFIG'])
        # self.cluster = ecs.Cluster.from_cluster_attributes(self, "ImportedCluster", cluster_name=env_config['CLUSTER_NAME'], vpc=self.network.get_vpc())

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

        conditional_schedule_dq_tasks = schedule_dq_mon_choice.when(schedule_dq_mon_cond, deploy_chain).otherwise(inference_chain)



        # TASKS
        get_or_create_model_from_registry_task, get_or_create_model_from_registry_function = lambda_tasks.get_get_or_create_model_from_registry_fn_task(self)
        prep_baseline_sets_task, prep_baseline_sets_function = lambda_tasks.prep_baseline_sets_fn_task(self)
        baseline_transform_task = sagemaker_tasks.get_baseline_transform_task(self, f'{get_or_create_model_from_registry_task._result_path}.model_name')

        # Parallel Monitor Schedule
        get_baseline_preds_task = lambda_tasks.get_baseline_preds_fn_task(self)
        make_baseline_task = lambda_tasks.make_baseline_sets_fn_task(self)

        end_parallel_monitor_scheduler = stepfunctions.Pass(self, 'EndParallelMonitorScheduler')

        schedule_dq_task = lambda_tasks.schedule_dq_task_fn_task(self, 'dq-mon')
        conditional_schedule_dq_tasks = schedule_dq_mon_choice.when(schedule_dq_mon_cond, schedule_dq_task).otherwise(inference_chain)

        schedule_mq_task = lambda_tasks.schedule_mq_task_fn_task(self, 'mq-mon')
        conditional_schedule_mq_tasks = schedule_mq_mon_choice.when(schedule_mq_mon_cond, schedule_mq_task).otherwise(inference_chain)


        schedule_me_task = lambda_tasks.schedule_me_task_fn_task(self, 'me-mon')
        conditional_schedule_me_tasks = schedule_me_mon_choice.when(schedule_me_mon_cond, schedule_me_task).otherwise(inference_chain)


        schedule_mb_task = lambda_tasks.schedule_mb_task_fn_task(self, 'mb-mon')
        conditional_schedule_mb_tasks = schedule_mb_mon_choice.when(schedule_mb_mon_cond, schedule_mb_task).otherwise(inference_chain)

        parallel_monitor_scheduler = stepfunctions.Parallel(
            self, 'ParallelMonitorScheduler',
            result_path=stepfunctions.JsonPath.DISCARD  # discard outputs if not needed
        )
        parallel_monitor_scheduler.branch(conditional_schedule_dq_tasks)
        parallel_monitor_scheduler.branch(conditional_schedule_mq_tasks)
        parallel_monitor_scheduler.branch(conditional_schedule_me_tasks)
        parallel_monitor_scheduler.branch(conditional_schedule_mb_tasks)

        # check_dq_task = None
        # check_mq_task = None
        # check_me_task = None
        # check_mb_task = None

        deploy_endpoint_task = lambda_tasks.deploy_endpoint_fn_task(self)
        batch_transform_task = sagemaker_tasks.get_batch_transform_task(self, f'{get_or_create_model_from_registry_task._result_path}.model_name')

        # Parallel


        ### MONITOR_CHECK_MAP ###
        check_monitor_map = stepfunctions.Pass(self, 'CheckMonitorMapDummy')
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
        baseline_chain = prep_baseline_sets_task.next(baseline_transform_task).next(get_baseline_preds_task).next(make_baseline_task)
        ### deploy_chain ###
        deploy_chain = None
        if(self.deploy_type == 'realtime'):
            deploy_chain = deploy_endpoint_task.next(parallel_monitor_scheduler)
        else:
            deploy_chain = parallel_monitor_scheduler
        ### inference_chain ###
        if(self.deploy_type == 'realtime'):
            inference_chain = check_monitor_map
        else:
            inference_chain = batch_transform_task# .next(check_monitor_map)



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