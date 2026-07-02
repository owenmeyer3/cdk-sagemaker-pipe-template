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
    aws_ecr as ecr,
    aws_ec2 as ec2,
    aws_lambda as _lambda
)
from constructs import Construct
from custom_constructs.CNetwork import CNetwork
from custom_constructs.CLambda import CLambdaFunction
from custom_constructs.CECS import CFargateTaskDefinition
from custom_constructs.utils import get_local_project_root
import custom_functions.lambda_tasks as lambda_tasks
import custom_functions.baseline_l_tasks as baseline_l_tasks
import custom_functions.baseline_e_tasks as baseline_e_tasks
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
        self.cluster = ecs.Cluster.from_cluster_attributes(self, "ImportedCluster", cluster_name=env_config['CLUSTER_NAME'], vpc=self.network.get_vpc())
        self.pandas_layer_version=_lambda.LayerVersion.from_layer_version_arn(self, 'ExistingPandasLayer', 'arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python311:33')
        # self.public_subnet_selection=ec2.SubnetSelection(subnets=[ec2.Subnet.from_subnet_id(self, 'ExistingPublicSubnet', 'subnet-00d66021', route_table_id='rtb-xxxxxxxx')])
        self.public_subnet_selection=ec2.SubnetSelection(subnets=[ec2.Subnet.from_subnet_attributes(self, 'ExistingPublicSubnet',subnet_id='subnet-00d66021',route_table_id='rtb-a787c4d9')])
        assert os.getenv('ACTION') not in ['deploy', 'inference'], 'ACTION must be in [deploy, inference]'



        print(f"ROLE: {self.state_machine_execution_role.role_arn}")
        



        self.baseline_image_repo = ecr.Repository.from_repository_name(
            self, 
            'BaselineImageRepo', 
            'baseline-image'
        )

        # Derived Params
        self.pipeline_dir =   f's3://{self.pipeline_bucket}/pipelines/{self.name}'
        self.baseline_dir =   f'{self.pipeline_dir}/baseline'
        self.monitors_dir=    f'{self.pipeline_dir}/monitors'
        self.batch_out_dir=   f'{self.pipeline_dir}/batch-out'
        self.data_capture_dir=f'{self.pipeline_dir}/capture'
        self.dq_monitor_dir=  f'{self.pipeline_dir}/data-quality'
        self.mq_monitor_dir=  f'{self.pipeline_dir}/model-quality'
        self.mb_monitor_dir=  f'{self.pipeline_dir}/model-bias'
        self.me_monitor_dir=  f'{self.pipeline_dir}/model-explainability'
        self.db_monitor_dir=  f'{self.pipeline_dir}/data-bias'

        # Runtime Args
        self.model_package_version_lkp = '$.MODEL_PACKAGE_VERSION'# :1,
        self.action_type_lkp = '$.ACTION'# :'deploy',
        self.baseline_file_lkp = '$.BASELINE_FILE'# :'aaa',
        self.baseline_cols_lkp = '$.BASELINE_COLS'
        self.monitor_instance_type_lkp = '$.MONITOR_INSTANCE_TYPE'# :'ml.m5.large',
        self.endpoint_instance_type_lkp = '$.ENDPOINT_INSTANCE_TYPE'# :'ml.m5.large',
        self.transform_instance_type_lkp = '$.TRANSFORM_INSTANCE_TYPE'# :'ml.m5.large',
        self.fail_on_violation_lkp = '$.FAIL_ON_VIOLATION'# :False,
        self.monitor_schedule_expression_lkp = '$.MONITOR_SCHEDULE_EXPRESSION'# :'cron(0 * ? * * *)',
        self.data_analysis_start_time_lkp = '$.MONITOR_ANALYSIS_START_TIME'# :,
        self.data_analysis_end_time_lkp = '$.MONITOR_ANALYSIS_END_TIME'# :,
        self.rebaseline_lkp = '$.REBASELINE'# :True,
        self.enable_data_quality_monitoring_lkp = '$.ENABLE_DATA_QUALITY_MONITORING'# :True,
        self.enable_model_bias_monitoring_lkp = '$.ENABLE_MODEL_BIAS_MONITORING'# :True,
        self.enable_model_explainability_monitoring_lkp = '$.ENABLE_MODEL_EXPLAINABILITY_MONITORING'# :True,
        self.enable_model_quality_monitoring_lkp = '$.ENABLE_MODEL_QUALITY_MONITORING'# :True,
        self.enable_data_quality_check_lkp = '$.ENABLE_DATA_QUALITY_CHECK'# :True,
        self.enable_model_bias_check_lkp = '$.ENABLE_MODEL_BIAS_CHECK'# :True,
        self.enable_model_explainability_check_lkp = '$.ENABLE_MODEL_EXPLAINABILITY_CHECK'# :True,
        self.enable_model_quality_check_lkp = '$.ENABLE_MODEL_QUALITY_CHECK'# :True,
        self.sns_topic_arn_lkp = '$.SNS_TOPIC_ARN'# :,
        self.enable_sns_notification_lkp = '$.ENABLE_SNS_NOTIFICATION' # :False,
        self.ground_truth_dir_lkp = '$.GROUND_TRUTH_DIR'# :f's3://omm-test-bucket/ground-truth/abalone',
        self.batch_input_dir_lkp = '$.BATCH_INPUT_DIR'# :f's3://omm-test-bucket/ground-truth/abalone',
        self.execution_id_lkp = ('$$.Execution.Name')

        # '%Y-%m-%d-%H-%M-%S'
        # stepfunctions.JsonPath.format(f'{name}-{{}}', stepfunctions.JsonPath.string_at(execution_id_lkp))
        # execution_id=event['execution_id']

        state_machine_start = stepfunctions.Pass(self, 'Start')
        sf_launch_schedule = events.Schedule.rate(Duration.hours(1))
        statemachine_end = stepfunctions.Pass(self, 'End')

        # CHOICES / CONDITIONS
        rebaseline_choice = stepfunctions.Choice(self, "RebaselineChoice")
        rebaseline_cond =   stepfunctions.Condition.string_equals(self.rebaseline_lkp, "TRUE")
        action_choice = stepfunctions.Choice(self, "ActionChoice")
        action_cond =   stepfunctions.Condition.string_equals(self.action_type_lkp, "deploy")
        schedule_dq_mon_choice = stepfunctions.Choice(self, "ScheduleDqMonChoice")
        schedule_dq_mon_cond =   stepfunctions.Condition.string_equals(self.enable_data_quality_monitoring_lkp, "TRUE")
        schedule_mq_mon_choice =  stepfunctions.Choice(self, "ScheduleMqMonChoice")
        schedule_mq_mon_cond =    stepfunctions.Condition.string_equals(self.enable_model_quality_monitoring_lkp, "TRUE")
        schedule_me_mon_choice = stepfunctions.Choice(self, "ScheduleMeMonChoice")
        schedule_me_mon_cond =   stepfunctions.Condition.string_equals(self.enable_model_explainability_monitoring_lkp, "TRUE")
        schedule_mb_mon_choice =  stepfunctions.Choice(self, "ScheduleMbMonChoice")
        schedule_mb_mon_cond =    stepfunctions.Condition.string_equals(self.enable_model_bias_monitoring_lkp, "TRUE")
        check_dq_mon_choice = stepfunctions.Choice(self, "CheckDqMonChoice")
        check_dq_mon_cond =   stepfunctions.Condition.string_equals(self.enable_data_quality_check_lkp, "TRUE")
        check_mq_mon_choice =  stepfunctions.Choice(self, "CheckMqMonChoice")
        check_mq_mon_cond =    stepfunctions.Condition.string_equals(self.enable_model_quality_check_lkp, "TRUE")
        check_me_mon_choice = stepfunctions.Choice(self, "CheckMeMonChoice")
        check_me_mon_cond =   stepfunctions.Condition.string_equals(self.enable_model_explainability_check_lkp, "TRUE")
        check_mb_mon_choice =  stepfunctions.Choice(self, "CheckMbMonChoice")
        check_mb_mon_cond =    stepfunctions.Condition.string_equals(self.enable_model_bias_check_lkp, "TRUE")

        # CREATE
        get_or_create_model_from_registry_task, get_or_create_model_from_registry_function = lambda_tasks.get_get_or_create_model_from_registry_fn_task(self, 'GetOrCreateModel', f'{self.name}-get-or-create-model', self.model_package_group_name, self.model_package_version_lkp, self.other_execution_role_arn)
        model_name_lkp = f'{get_or_create_model_from_registry_task._result_path}.MODEL_NAME'
        model_package_arn_lkp = f'{get_or_create_model_from_registry_task._result_path}.MODEL_PACKAGE_ARN'

        # BASELINE
        prep_baseline_sets_task, prep_baseline_sets_function = lambda_tasks.prep_baseline_sets_fn_task(self, 'PrepBaselineSets', f'{self.name}-prep-baseline-sets', self.baseline_file_lkp, self.target_name, self.target_type, self.baseline_dir, baseline_cols_lkp=self.baseline_cols_lkp, layers=[self.pandas_layer_version])
        baseline_headered_file_lkp = f'{prep_baseline_sets_task._result_path}.BASELINE_HEADERED_FILE'
        baseline_X_file_lkp = f'{prep_baseline_sets_task._result_path}.BASELINE_X_FILE'
        baseline_X_filename_lkp = f'{prep_baseline_sets_task._result_path}.BASELINE_X_FILENAME'

        baseline_transform_chain, baseline_transform_end, baseline_transform_out_dir_lkp = sagemaker_tasks.get_transform_task(
            scope, 
            'BaselineTransform', 
            f'{self.name}-bl-transform-job', 
            model_name_lkp,
            self.execution_id_lkp, 
            self.transform_instance_type_lkp, 
            s3_data_source_lkp=baseline_X_file_lkp, 
            transform_out_dir=self.baseline_dir
        ) 

        get_baseline_preds_task, get_baseline_preds_function = lambda_tasks.get_baseline_preds_fn_task(self, 'GetBaselinePreds', f'{self.name}-get-baseline-preds', baseline_transform_out_dir_lkp, baseline_X_filename_lkp, self.baseline_dir, layers=[self.pandas_layer_version])
        baseline_pred_file_lkp = f'{get_baseline_preds_task._result_path}.BASELINE_PRED_FILE'

        make_baseline_task, make_baseline_function = lambda_tasks.make_baseline_sets_fn_task(
            self, 
            'MakeBaselineSets', f'{self.name}-make-baseline-sets',
            baseline_headered_file_lkp, 
            baseline_pred_file_lkp, 
            self.dq_monitor_dir, 
            self.db_monitor_dir, 
            self.mq_monitor_dir, 
            self.mb_monitor_dir, 
            self.me_monitor_dir, 
            self.target_name, 
            self.prediction_name, 
            baseline_X_file_lkp, 
            self.target_type, 
            layers=[self.pandas_layer_version]
        )

        # dq_baseline_task, dq_baseline_function = baseline_l_tasks.run_dq_bl_job_fn_task(
        #     self, 
        #     'DQBaselineJob', 
        #     f'{self.name}-dq-baseline-fn', 
        #     self.baseline_image_repo,
        #     self.other_execution_role_arn, 
        #     self.dq_monitor_dir,
        #     self.execution_id_lkp,
        # )
        dq_baseline_task=baseline_e_tasks.run_dq_bl_job_fn_task(
            self, 
            'DQBaselineJob', 
            f'{self.name}-dq-baseline', 
            self.baseline_image_repo,
            self.other_execution_role_arn,
            self.dq_monitor_dir,
            self.execution_id_lkp
        ).get_task(self.cluster)

        # mq_baseline_task, mq_baseline_function = baseline_l_tasks.run_mq_bl_job_fn_task(
        #     self, 
        #     'MQBaselineJob', 
        #     f'{self.name}-mq-baseline-fn', 
        #     self.baseline_image_repo,
        #     self.other_execution_role_arn,
        #     self.mq_monitor_dir, 
        #     self.execution_id_lkp,
        #     self.prediction_name,
        #     self.target_name,
        #     self.problem_type,
        #     probability_attribute=None, # Classification Only,
        #     probability_threshold_attribute=None,  # Classification Only
        # )
        mq_baseline_task=baseline_e_tasks.run_mq_bl_job_fn_task(
            self, 
            'MQBaselineJob', 
            f'{self.name}-mq-baseline-fn',
            self.baseline_image_repo,
            self.other_execution_role_arn,
            self.mq_monitor_dir, 
            self.execution_id_lkp,
            self.prediction_name,
            self.target_name,
            self.problem_type,
            probability_attribute=None, # Classification Only,
            probability_threshold_attribute=None,  # Classification Only
        ).get_task(self.cluster)

        # mb_baseline_task, mb_baseline_function = baseline_l_tasks.run_mb_bl_job_fn_task(
        #     self, 
        #     'MBBaselineJob', 
        #     f'{self.name}-mb-baseline-fn',
        #     model_name_lkp,
        #     self.baseline_image_repo,
        #     self.other_execution_role_arn, 
        #     self.mb_monitor_dir,
        #     self.execution_id_lkp,
        #     self.target_name
        # )
        mb_baseline_task = baseline_e_tasks.run_mb_bl_job_fn_task(
            self, 
            'MBBaselineJob', 
            f'{self.name}-mb-baseline',
            self.baseline_image_repo,
            model_name_lkp,
            self.other_execution_role_arn, 
            self.mb_monitor_dir,
            self.execution_id_lkp,
            self.target_name
        ).get_task(self.cluster)

        # me_baseline_task, me_baseline_function = baseline_l_tasks.run_me_bl_job_fn_task(
        #     self, 
        #     'MEBaselineJob', 
        #     f'{self.name}-me-baseline-fn', 
        #     self.baseline_image_repo,
        #     model_name_lkp,
        #     self.other_execution_role_arn, 
        #     self.me_monitor_dir, 
        #     self.execution_id_lkp,
        #     self.target_name,
        #     self.baseline_cols_lkp,
        #     baseline_X_file_lkp
        # )
        me_baseline_task = baseline_e_tasks.run_me_bl_job_fn_task(
            self, 
            'MEBaselineJob', 
            f'{self.name}-me-baseline',
            self.baseline_image_repo,
            model_name_lkp,
            self.other_execution_role_arn, 
            self.me_monitor_dir, 
            self.execution_id_lkp,
            self.target_name,
            self.baseline_cols_lkp,
            baseline_X_file_lkp
        ).get_task(self.cluster)
        
        parallel_baseline_jobs=stepfunctions.Parallel(self, 'ParallelBaselineJobs') \
            .branch(mb_baseline_task) \
            .branch(me_baseline_task)
            # .branch(dq_baseline_task) \
            # .branch(mq_baseline_task) \


        # RT DEPLOY
        deploy_endpoint_task, deploy_endpoint_function = lambda_tasks.deploy_endpoint_fn_task(
            self,
            'DeployEndpoint', f'{self.name}-deploy-endpoint',
            model_name_lkp, 
            self.model_package_group_name, 
            self.model_package_version_lkp, 
            self.endpoint_instance_type_lkp, 
            self.data_capture_dir
        )
        endpoint_name_lkp = f'{deploy_endpoint_task._result_path}.ENDPOINT_NAME'
        
        # MONITOR SCHEDULES
        schedule_dq_task, schedule_dq_function = lambda_tasks.schedule_dq_task_fn_task(
            self, 
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
        conditional_schedule_dq_tasks = schedule_dq_mon_choice.when(schedule_dq_mon_cond, schedule_dq_task)

        schedule_mq_task, schedule_mq_function = lambda_tasks.schedule_mq_task_fn_task(
            self, 
            'ScheduleMQ', f'{self.name}-schedule-mq', 'mq-mon',
            endpoint_name_lkp,
            self.data_capture_dir,
            self.other_execution_role_arn,
            self.deploy_type,
            self.problem_type,
            self.prediction_name,
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

        parallel_monitor_scheduler=stepfunctions.Parallel(self, 'ParallelMonitorScheduler') \
            .branch(conditional_schedule_dq_tasks) \
            .branch(conditional_schedule_mq_tasks) \
            .branch(conditional_schedule_me_tasks) \
            .branch(conditional_schedule_mb_tasks)

        # INF TRANSFORM
        # batch_transform_task, batch_transform_function = lambda_tasks.sm_transform_fn_task(self, 'BatchTransform', f'{self.name}-batch-transform', model_name_lkp, self.transform_instance_type_lkp, s3_data_source_lkp=self.batch_input_dir_lkp, transform_out_dir=self.batch_out_dir)
        # batch_transform_job_arn_lkp = f'{baseline_transform_task._result_path}.TRANSFORM_JOB_ARN'
        # batch_transform_job_name_lkp = f'{baseline_transform_task._result_path}.JOB_NAME'
        # batch_transform_out_dir_lkp = f'{baseline_transform_task._result_path}.OUTPUT_PATH' # "s3://bucket/output/"
        # batch_transform_status_lkp = f'{baseline_transform_task._result_path}.STATUS'
        batch_transform_chain, batch_transform_end, batch_transform_out_dir_lkp = sagemaker_tasks.get_transform_task(
            self, 
            'BatchTransform', 
            f'{self.name}-batch-transform-job', 
            model_name_lkp, 
            self.execution_id_lkp,
            self.transform_instance_type_lkp, 
            s3_data_source_lkp=self.batch_input_dir_lkp, 
            transform_out_dir=self.batch_out_dir
        ) 

        check_dq_task, check_dq_function = lambda_tasks.check_dq_task_fn_task(self, 'CheckDQ', f'{self.name}-check-dq')
        conditional_check_dq_tasks = check_dq_mon_choice.when(check_dq_mon_cond, check_dq_task).afterwards()
        check_mq_task, check_mq_function = lambda_tasks.check_mq_task_fn_task(self, 'CheckMQ', f'{self.name}-check-mq')
        conditional_check_mq_tasks = check_mq_mon_choice.when(check_mq_mon_cond, check_mq_task).afterwards()
        check_me_task, check_me_function = lambda_tasks.check_me_task_fn_task(self, 'CheckME', f'{self.name}-check-me')
        conditional_check_me_tasks = check_me_mon_choice.when(check_me_mon_cond, check_me_task).afterwards()
        check_mb_task, check_mb_function = lambda_tasks.check_mb_task_fn_task(self, 'CheckMB', f'{self.name}-check-mb')
        conditional_check_mb_tasks = check_mb_mon_choice.when(check_mb_mon_cond, check_mb_task).afterwards()

        pre_transform_parallel_monitor_checker = stepfunctions.Parallel(self, 'PreTransformMonitorChecker').branch(conditional_check_me_tasks).branch(conditional_check_dq_tasks)
        post_transform_parallel_monitor_checker = stepfunctions.Parallel(self, 'PostTransformMonitorChecker').branch(conditional_check_mq_tasks).branch(conditional_check_mb_tasks)


        # SUB CHAINS

        ### baseline_chain ###
        # Wire all exits
        baseline_transform_end.next(get_baseline_preds_task).next(make_baseline_task).next(parallel_baseline_jobs)
        baseline_chain = prep_baseline_sets_task.next(baseline_transform_chain)

        ### deploy_chain ###
        deploy_chain = None
        if(self.deploy_type == 'realtime'):
            deploy_chain = deploy_endpoint_task.next(parallel_monitor_scheduler)
        else:
            deploy_chain = parallel_monitor_scheduler

        ### inference_chain ###
        if(self.deploy_type == 'realtime'):
            inference_chain = pre_transform_parallel_monitor_checker.next(post_transform_parallel_monitor_checker)
        else:
            batch_transform_end.next(post_transform_parallel_monitor_checker)
            inference_chain = pre_transform_parallel_monitor_checker.next(batch_transform_chain)

        # FULL CHAIN
        chain=state_machine_start.next(get_or_create_model_from_registry_task) \
            .next( \
                rebaseline_choice.when(rebaseline_cond, baseline_chain).otherwise(action_choice).afterwards() \
            ).next( \
                action_choice.when(action_cond, \
                    deploy_chain \
                ).otherwise( \
                    inference_chain \
                ).afterwards() \
            ).next(statemachine_end)     
        
        # STATE MACHINE
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

        # ALLOW SM TO CAL LAMBDAS
        get_or_create_model_from_registry_function.add_invoker_arn(state_machine.state_machine_arn)
        prep_baseline_sets_function.add_invoker_arn(state_machine.state_machine_arn)
        get_baseline_preds_function.add_invoker_arn(state_machine.state_machine_arn)
        make_baseline_function.add_invoker_arn(state_machine.state_machine_arn)
        deploy_endpoint_function.add_invoker_arn(state_machine.state_machine_arn)
        schedule_dq_function.add_invoker_arn(state_machine.state_machine_arn)
        schedule_mq_function.add_invoker_arn(state_machine.state_machine_arn)
        schedule_me_function.add_invoker_arn(state_machine.state_machine_arn)
        schedule_mb_function.add_invoker_arn(state_machine.state_machine_arn)
        check_dq_function.add_invoker_arn(state_machine.state_machine_arn)
        check_mq_function.add_invoker_arn(state_machine.state_machine_arn)
        check_me_function.add_invoker_arn(state_machine.state_machine_arn)
        check_mb_function.add_invoker_arn(state_machine.state_machine_arn)
        # dq_baseline_function.grant_invoke(state_machine)
        # mq_baseline_function.grant_invoke(state_machine)
        # mb_baseline_function.grant_invoke(state_machine)
        # me_baseline_function.grant_invoke(state_machine)


        # RULE
        sf_launch_rule = events.Rule(self, "Rule", 
            schedule=sf_launch_schedule,
            enabled=False,
            targets=[
                targets.SfnStateMachine(
                    state_machine,
                    role=self.rule_execution_role,
                    input=events.RuleTargetInput.from_object(events.EventField.from_path("$"))
                )
            ]
        )