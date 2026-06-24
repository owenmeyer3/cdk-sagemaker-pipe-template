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

        # Build Configs
        self.name = project_config['NAME']
        self.deploy_type=project_config['DEPLOY_TYPE']
        self.target_name=project_config['TARGET_NAME']
        self.target_type = project_config['TARGET_TYPE']
        self.problem_type=project_config['PROBLEM_TYPE']
        self.prediction_name=project_config['PREDICTION_NAME']
        self.ground_truth_label=project_config['GROUND_TRUTH_LABEL']
        self.model_package_group_name=project_config['MODEL_PACKAGE_GROUP_NAME']
        self.pipeline_bucket=env_config['PIPELINE_BUCKET']
        self.region_name = env_config['REGION_NAME']
        self.state_machine_execution_role=iam.Role.from_role_arn(self, "ImportedSMExecutionRole", env_config['SM_EXECUTION_ROLE_ARN'], mutable=False)
        self.rule_execution_role=iam.Role.from_role_arn(self, "ImportedRuleExecutionRole", env_config['RULE_EXECUTION_ROLE_ARN'], mutable=False)
        self.lambda_execution_role_arn=iam.Role.from_role_arn(self, "ImportedLambdaExecutionRole", env_config['LAMBDA_EXECUTION_ROLE_ARN'], mutable=False)
        self.other_execution_role_arn=iam.Role.from_role_arn(self, "ImportedOtherExecutionRole", env_config['OTHER_EXECUTION_ROLE_ARN'], mutable=False)
        self.network = CNetwork(self, "ImportedNetwork", region=env_config['REGION_NAME'], vpc_config=env_config['VPC_CONFIG'])
        assert os.getenv('ACTION') not in ['deploy', 'inference'], 'ACTION must be in [deploy, inference]'

        # Derived Params
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

        # Runtime Args
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
        self.model_package_version_lkp = '$.MODEL_PACKAGE_VERSION'
        self.action_type_lkp = '$.ACTION'
        self.baseline_file_lkp = '$.BASELINE_FILE'
        self.monitor_instance_type_lkp = '$.MONITOR_INSTANCE_TYPE'
        self.endpoint_instance_type_lkp = '$.ENDPOINT_INSTANCE_TYPE'
        self.transform_instance_type_lkp = '$.TRANSFORM_INSTANCE_TYPE'
        self.fail_on_violation_lkp = '$.FAIL_ON_VIOLATION'
        self.monitor_schedule_expression_lkp = '$.MONITOR_SCHEDULE_EXPRESSION'
        self.data_analysis_start_time_lkp = '$.MONITOR_ANALYSIS_START_TIME'
        self.data_analysis_end_time_lkp = '$.MONITOR_ANALYSIS_END_TIME'
        self.enable_data_quality_monitoring_lkp = '$.ENABLE_DATA_QUALITY_MONITORING'
        self.enable_model_bias_monitoring_lkp = '$.ENABLE_MODEL_BIAS_MONITORING'
        self.enable_model_explainability_monitoring_lkp = '$.ENABLE_MODEL_EXPLAINABILITY_MONITORING'
        self.enable_model_quality_monitoring_lkp = '$.ENABLE_MODEL_QUALITY_MONITORING'
        self.sns_topic_arn_lkp = '$.SNS_TOPIC_ARN'
        self.enable_sns_notification_lkp = '$.ENABLE_SNS_NOTIFICATION'
        self.ground_truth_dir_lkp = '$.GROUND_TRUTH_DIR'
        self.batch_input_dir_lkp = '$.BATCH_INPUT_DIR'

        chain = stepfunctions.Pass(self, 'Start')
        sf_launch_schedule = events.Schedule.rate(Duration.hours(1))
        end_pass = stepfunctions.Pass(self, 'End')

        # CHOICES / CONDITIONS
        rebaseline_choice = stepfunctions.Choice(self, "RebaselineChoice")
        rebaseline_cond =   stepfunctions.Condition.string_equals("$.rebaseline", "TRUE")
        deploy_or_inference_choice = stepfunctions.Choice(self, "DeployOrInferenceChoice")
        deploy_or_inference_cond =   stepfunctions.Condition.string_equals("$.deploy_or_inference", "deploy")
        schedule_dq_mon_choice = stepfunctions.Choice(self, "ScheduleDqMonChoice")
        schedule_dq_mon_cond =   stepfunctions.Condition.string_equals("$.scheduleDqMonChoice", "TRUE")
        schedule_mq_mon_choice =  stepfunctions.Choice(self, "ScheduleMqMonChoice")
        schedule_mq_mon_cond =    stepfunctions.Condition.string_equals("$.scheduleMqMonChoice", "TRUE")
        schedule_me_mon_choice = stepfunctions.Choice(self, "ScheduleMeMonChoice")
        schedule_me_mon_cond =   stepfunctions.Condition.string_equals("$.scheduleMeMonChoice", "TRUE")
        schedule_mb_mon_choice =  stepfunctions.Choice(self, "ScheduleMbMonChoice")
        schedule_mb_mon_cond =    stepfunctions.Condition.string_equals("$.scheduleMbMonChoice", "TRUE")


        # TASKS
        get_or_create_model_from_registry_task, get_or_create_model_from_registry_function = lambda_tasks.get_get_or_create_model_from_registry_fn_task(self, 'GetOrCreateModel', f'{self.name}-get-or-create-model', self.model_package_group_name, self.model_package_version_lkp)
        model_name_lkp = f'{get_or_create_model_from_registry_task._result_path}.model_name'
        model_package_arn_lkp = f'{get_or_create_model_from_registry_task._result_path}.model_package_arn'

        prep_baseline_sets_task, prep_baseline_sets_function = lambda_tasks.prep_baseline_sets_fn_task(self, 'PrepBaselineSets', f'{self.name}-prep-baseline-sets', self.baseline_file_lkp, self.target_name, self.target_type, self.baseline_dir)
        baseline_X_dir_lkp = f'{prep_baseline_sets_task._result_path}.baseline_X_dir'
        baseline_X_file_lkp = f'{prep_baseline_sets_task._result_path}.baseline_X_file'
        baseline_X_filename_lkp = f'{prep_baseline_sets_task._result_path}.baseline_X_filename'

        # baseline_transform_task = sagemaker_tasks.get_baseline_transform_task(self, model_name_lkp, self.baseline_file_lkp, transform_instance_dtl_lkp)
        # transform_out_dir_lkp = f'{baseline_transform_task._result_path}.TransformOutput.S3OutputPath' # "s3://bucket/output/"
        baseline_transform_task, baseline_transform_function = lambda_tasks.sm_transform_fn_task(self, 'BaselineTransform', f'{self.name}-baseline-transform', model_name_lkp, self.transform_instance_type_lkp, s3_data_source_lkp=self.baseline_file_lkp, transform_out_dir=self.baseline_dir)
        baseline_transform_job_arn_lkp = f'{baseline_transform_task._result_path}.TransformJobArn'
        baseline_transform_job_name_lkp = f'{baseline_transform_task._result_path}.JobName'
        baseline_transform_out_dir_lkp = f'{baseline_transform_task._result_path}.OutputPath' # "s3://bucket/output/"
        baseline_transform_status_lkp = f'{baseline_transform_task._result_path}.Status'

        baseline_transform_job_arn_lkp = f'{baseline_transform_task._result_path}.TransformJobArn'
        baseline_transform_job_name_lkp = f'{baseline_transform_task._result_path}.JobName'
        baseline_transform_out_dir_lkp = f'{baseline_transform_task._result_path}.OutputPath' # "s3://bucket/output/"
        baseline_transform_status_lkp = f'{baseline_transform_task._result_path}.Status'
        
        get_baseline_preds_task, get_baseline_preds_function = lambda_tasks.get_baseline_preds_fn_task(self, 'GetBaselinePreds', f'{self.name}-get-baseline-preds', baseline_transform_out_dir_lkp, self.target_name, self.target_type)

        baseline_pred_file_lkp = f'{get_baseline_preds_task._result_path}.baseline_pred_file'

        make_baseline_task, make_baseline_function = lambda_tasks.make_baseline_sets_fn_task(
            self, 
            'MakeBaselineSets', f'{self.name}-make-baseline-sets',
            self.baseline_file_lkp, 
            baseline_pred_file_lkp, 
            self.dq_monitor_dir, 
            self.db_monitor_dir, 
            self.mq_monitor_dir, 
            self.mb_monitor_dir, 
            self.me_monitor_dir, 
            self.target_name, 
            self.prediction_name, 
            baseline_X_file_lkp, 
            self.target_type
        )

        deploy_endpoint_task, deploy_endpoint_function = lambda_tasks.deploy_endpoint_fn_task(
            self,
            'DeployEndpoint', f'{self.name}-deploy-endpoint',
            model_name_lkp, 
            self.model_package_group_name, 
            self.model_package_version_lkp, 
            self.endpoint_instance_type_lkp, 
            self.data_capture_dir
        )
        endpoint_name_lkp = f'{deploy_endpoint_task._result_path}.endpoint_name'

        # Parallel Monitor Schedule
        end_parallel_monitor_scheduler = stepfunctions.Pass(self, 'EndParallelMonitorScheduler')

        schedule_dq_task, schedule_dq_function = lambda_tasks.schedule_dq_task_fn_task(self, 
            'ScheduleDQ', f'{self.name}-schedule-dq', 'dq-mon', 
            endpoint_name_lkp,
            self.data_capture_dir,
            self.other_execution_role_arn,
            self.deploy_type,
            self.dq_monitor_dir,
            self.monitor_instance_type_lkp,
            self.monitor_schedule_expression_lkp,
            self.data_analysis_start_time_lkp,
            self.data_analysis_end_time_lkp,
        )
        conditional_schedule_dq_tasks = schedule_dq_mon_choice.when(schedule_dq_mon_cond, schedule_dq_task).afterwards()

        schedule_mq_task, schedule_mq_function = lambda_tasks.schedule_mq_task_fn_task(
            self, 
            'ScheduleMQ', f'{self.name}-schedule-mq', 'mq-mon',
            endpoint_name_lkp,
            self.data_capture_dir,
            self.other_execution_role_arn,
            self.deploy_type,
            self.problem_type,
            self.ground_truth_label,
            self.ground_truth_dir_lkp,
            self.mq_monitor_dir,
            self.monitor_instance_type_lkp,
            self.monitor_schedule_expression_lkp,
            self.data_analysis_start_time_lkp,
            self.data_analysis_end_time_lkp,
        )
        conditional_schedule_mq_tasks = schedule_mq_mon_choice.when(schedule_mq_mon_cond, schedule_mq_task).afterwards()

        schedule_me_task, schedule_me_function = lambda_tasks.schedule_me_task_fn_task(
            self, 
            'ScheduleME', f'{self.name}-schedule-me', 'me-mon', 
            endpoint_name_lkp,
            self.data_capture_dir,
            self.other_execution_role_arn,
            self.deploy_type,
            self.me_monitor_dir,
            self.monitor_instance_type_lkp,
            self.monitor_schedule_expression_lkp,
            self.data_analysis_start_time_lkp,
            self.data_analysis_end_time_lkp,
        )
        conditional_schedule_me_tasks = schedule_me_mon_choice.when(schedule_me_mon_cond, schedule_me_task).afterwards()

        schedule_mb_task, schedule_mb_function = lambda_tasks.schedule_mb_task_fn_task(
            self, 
            'ScheduleMB', f'{self.name}-schedule-mb', 'mb-mon', 
            endpoint_name_lkp,
            self.data_capture_dir,
            self.other_execution_role_arn,
            self.deploy_type,
            self.mb_monitor_dir,
            self.ground_truth_dir_lkp,
            self.monitor_instance_type_lkp,
            self.monitor_schedule_expression_lkp,
            self.data_analysis_start_time_lkp,
            self.data_analysis_end_time_lkp,
        )
        conditional_schedule_mb_tasks = schedule_mb_mon_choice.when(schedule_mb_mon_cond, schedule_mb_task).afterwards()

        parallel_monitor_scheduler = stepfunctions.Parallel(
            self, 'ParallelMonitorScheduler',
            result_path=stepfunctions.JsonPath.DISCARD  # discard outputs if not needed
        )
        parallel_monitor_scheduler.branch(conditional_schedule_dq_tasks)
        parallel_monitor_scheduler.branch(conditional_schedule_mq_tasks)
        parallel_monitor_scheduler.branch(conditional_schedule_me_tasks)
        parallel_monitor_scheduler.branch(conditional_schedule_mb_tasks)
        parallel_monitor_scheduler.next(end_parallel_monitor_scheduler)

        # check_dq_task = None
        # check_mq_task = None
        # check_me_task = None
        # check_mb_task = None

        # batch_transform_task = sagemaker_tasks.get_batch_transform_task(self, model_name_lkp, self.batch_input_dir_lkp, transform_instance_dtl_lkp)
        batch_transform_task, batch_transform_function = lambda_tasks.sm_transform_fn_task(self, 'BatchTransform', f'{self.name}-batch-transform', model_name_lkp, self.transform_instance_type_lkp, s3_data_source_lkp=self.batch_input_dir_lkp, transform_out_dir=self.batch_out_dir)
        batch_transform_job_arn_lkp = f'{baseline_transform_task._result_path}.TransformJobArn'
        batch_transform_job_name_lkp = f'{baseline_transform_task._result_path}.JobName'
        batch_transform_out_dir_lkp = f'{baseline_transform_task._result_path}.OutputPath' # "s3://bucket/output/"
        batch_transform_status_lkp = f'{baseline_transform_task._result_path}.Status'

        # Parallel


        ### MONITOR_CHECK_MAP ###
        # end_parallel_monitor_checker = stepfunctions.Pass(self, 'EndParallelMonitorChecker')

        # check_dq_task, check_dq_function = lambda_tasks.check_dq_task_fn_task(self, 'dq-mon')
        # conditional_check_dq_tasks = check_dq_mon_choice.when(check_dq_mon_cond, check_dq_task).otherwise(inference_chain)

        # check_mq_task, check_mq_function = lambda_tasks.check_mq_task_fn_task(self, 'mq-mon')
        # conditional_check_mq_tasks = check_mq_mon_choice.when(check_mq_mon_cond, check_mq_task).otherwise(inference_chain)


        # check_me_task, check_me_function = lambda_tasks.check_me_task_fn_task(self, 'me-mon')
        # conditional_check_me_tasks = check_me_mon_choice.when(check_me_mon_cond, check_me_task).otherwise(inference_chain)


        # check_mb_task, check_mb_function = lambda_tasks.check_mb_task_fn_task(self, 'mb-mon')
        # conditional_check_mb_tasks = check_mb_mon_choice.when(check_mb_mon_cond, check_mb_task).otherwise(inference_chain)

        # parallel_monitor_checker = stepfunctions.Parallel(
        #     self, 'ParallelMonitorChecker',
        #     result_path=stepfunctions.JsonPath.DISCARD  # discard outputs if not needed
        # )
        # parallel_monitor_checker.branch(conditional_check_dq_tasks)
        # parallel_monitor_checker.branch(conditional_check_mq_tasks)
        # parallel_monitor_checker.branch(conditional_check_me_tasks)
        # parallel_monitor_checker.branch(conditional_check_mb_tasks)
        # parallel_monitor_checker.next(end_parallel_monitor_checker)

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
            inference_chain = None # parallel_monitor_checker
        else:
            inference_chain = batch_transform_task# .next(parallel_monitor_checker)


        print("MAKE CHAIN")
        print(chain.to_string())

        # Make state machine
        chain.next(get_or_create_model_from_registry_task) \
            .next( \
                rebaseline_choice.when(rebaseline_cond, baseline_chain).afterwards()  \
            ).next( \
                deploy_or_inference_choice.when(deploy_or_inference_cond, \
                    deploy_chain \
                ).otherwise( \
                    inference_chain
                ).afterwards()   \
            ).next(end_pass)                   
        
        print("MADE CHAIN")
        state_machine=stepfunctions.StateMachine(
            self, "SM",
            definition_body=stepfunctions.DefinitionBody.from_chainable(chain),
            role=self.state_machine_execution_role,
            logs=stepfunctions.LogOptions(
                destination=logs.LogGroup(
                    self, "SMLog",
                    log_group_name=f"/ML/{env_config['ENV']}/states/{project_config['NAME']}",
                    removal_policy=RemovalPolicy.DESTROY,
                    retention=logs.RetentionDays.ONE_MONTH
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
                    role=self.rule_execution_role,
                    input=events.RuleTargetInput.from_object({"event":events.EventField.from_path("$")})
                )
            ]
        )