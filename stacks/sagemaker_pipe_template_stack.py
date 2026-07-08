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
from custom_constructs.CNetwork import Network
import custom_functions.c_lambda as c_lambda
import custom_functions.c_ecs as c_ecs
import custom_functions.c_sm as c_sm 

class SagemakerPipeTemplateStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, project_config:dict, env_config:dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Build Configs
        self.name = project_config['NAME']
        self.deploy_type=project_config['DEPLOY_TYPE']
        self.target_label=project_config['TARGET_NAME']
        self.target_type = project_config['TARGET_TYPE']
        self.problem_type=project_config['PROBLEM_TYPE']
        self.predict_label=project_config['PREDICTION_NAME']
        self.ground_truth_label=project_config['GROUND_TRUTH_LABEL']
        self.model_package_group_name=project_config['MODEL_PACKAGE_GROUP_NAME']
        self.pipeline_bucket=env_config['PIPELINE_BUCKET']
        self.region_name = env_config['REGION_NAME']
        self.state_machine_execution_role=iam.Role.from_role_arn(self, "ImportedSMExecutionRole", env_config['SM_EXECUTION_ROLE_ARN'], mutable=False)
        self.rule_execution_role=iam.Role.from_role_arn(self, "ImportedRuleExecutionRole", env_config['RULE_EXECUTION_ROLE_ARN'], mutable=False)
        self.lambda_execution_role=iam.Role.from_role_arn(self, "ImportedLambdaExecutionRole", env_config['LAMBDA_EXECUTION_ROLE_ARN'], mutable=False)
        self.monitor_role=iam.Role.from_role_arn(self, "ImportedOtherExecutionRole", env_config['MONITOR_EXECUTION_ROLE_ARN'], mutable=False)
        self.network = Network(self, "ImportedNetwork", region=env_config['REGION_NAME'], vpc_config=env_config['VPC_CONFIG'])
        self.cluster = ecs.Cluster.from_cluster_attributes(self, "ImportedCluster", cluster_name=env_config['CLUSTER_NAME'], vpc=self.network.get_vpc())
        self.pandas_layer_version=_lambda.LayerVersion.from_layer_version_arn(self, 'ExistingPandasLayer', 'arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python311:33')
        assert os.getenv('ACTION') not in ['deploy', 'inference'], 'ACTION must be in [deploy, inference]'

        print(f"ROLE: {self.state_machine_execution_role.role_arn}")
        
        self.baseline_image_repo = ecr.Repository.from_repository_name(self, 'BaselineImageRepo', 'baseline-image')

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

        state_machine_start = stepfunctions.Pass(self, 'Start')
        sf_launch_schedule = events.Schedule.rate(Duration.hours(1))
        statemachine_end = stepfunctions.Pass(self, 'End')

        # CHOICES / CONDITIONS
        rebaseline_choice = stepfunctions.Choice(self, "RebaselineChoice")
        rebaseline_cond =   stepfunctions.Condition.string_equals(self.rebaseline_lkp, "TRUE")
        action_choice =     stepfunctions.Choice(self, "ActionChoice")
        action_cond =            stepfunctions.Condition.string_equals(self.action_type_lkp, "deploy")
        schedule_dq_mon_choice = stepfunctions.Choice(self, "ScheduleDqMonChoice")
        schedule_dq_mon_cond =   stepfunctions.Condition.string_equals(self.enable_data_quality_monitoring_lkp, "TRUE")
        schedule_mq_mon_choice = stepfunctions.Choice(self, "ScheduleMqMonChoice")
        schedule_mq_mon_cond =   stepfunctions.Condition.string_equals(self.enable_model_quality_monitoring_lkp, "TRUE")
        schedule_me_mon_choice = stepfunctions.Choice(self, "ScheduleMeMonChoice")
        schedule_me_mon_cond =   stepfunctions.Condition.string_equals(self.enable_model_explainability_monitoring_lkp, "TRUE")
        schedule_mb_mon_choice = stepfunctions.Choice(self, "ScheduleMbMonChoice")
        schedule_mb_mon_cond =   stepfunctions.Condition.string_equals(self.enable_model_bias_monitoring_lkp, "TRUE")
        check_dq_mon_choice = stepfunctions.Choice(self, "CheckDqMonChoice")
        check_dq_mon_cond =   stepfunctions.Condition.string_equals(self.enable_data_quality_check_lkp, "TRUE")
        check_mq_mon_choice = stepfunctions.Choice(self, "CheckMqMonChoice")
        check_mq_mon_cond =   stepfunctions.Condition.string_equals(self.enable_model_quality_check_lkp, "TRUE")
        check_me_mon_choice = stepfunctions.Choice(self, "CheckMeMonChoice")
        check_me_mon_cond =   stepfunctions.Condition.string_equals(self.enable_model_explainability_check_lkp, "TRUE")
        check_mb_mon_choice = stepfunctions.Choice(self, "CheckMbMonChoice")
        check_mb_mon_cond =   stepfunctions.Condition.string_equals(self.enable_model_bias_check_lkp, "TRUE")

        # CREATE
        get_or_create_model_from_registry_lambda = c_lambda.get_get_or_create_model_from_registry_lambda(self, 'GetOrCreateModel', f'{self.name}-get-or-create-model', self.lambda_execution_role, self.model_package_group_name, self.model_package_version_lkp, self.lambda_execution_role)
        model_name_lkp =        f'{get_or_create_model_from_registry_lambda.task._result_path}.MODEL_NAME'
        model_package_arn_lkp = f'{get_or_create_model_from_registry_lambda.task._result_path}.MODEL_PACKAGE_ARN'

        # BASELINE
        prep_baseline_sets_lambda = c_lambda.prep_baseline_sets_lambda(self, 'PrepBaselineSets', f'{self.name}-prep-baseline-sets', self.lambda_execution_role, self.baseline_file_lkp, self.target_label, self.target_type, self.baseline_dir, baseline_cols_lkp=self.baseline_cols_lkp, layers=[self.pandas_layer_version])
        baseline_headered_file_lkp = f'{prep_baseline_sets_lambda.task._result_path}.BASELINE_HEADERED_FILE'
        baseline_X_file_lkp = f'{prep_baseline_sets_lambda.task._result_path}.BASELINE_X_FILE'
        baseline_X_filename_lkp = f'{prep_baseline_sets_lambda.task._result_path}.BASELINE_X_FILENAME'

        # baseline_transform_chain, baseline_transform_end, baseline_transform_out_dir_lkp = c_sm.get_transform_task(
        #     scope, 
        #     'BaselineTransform', 
        #     f'{self.name}-bl-transform-job', 
        #     model_name_lkp,
        #     self.transform_instance_type_lkp, 
        #     s3_data_source_lkp=baseline_X_file_lkp, 
        #     transform_out_dir=self.baseline_dir
        # ) 
        baseline_transform_task = c_sm.get_transform_task(
            scope, 
            'BaselineTransform', 
            f'{self.name}-bl-transform-job', 
            model_name_lkp,
            self.transform_instance_type_lkp, 
            s3_data_source_lkp=baseline_X_file_lkp, 
            transform_out_dir=self.baseline_dir
        ) 
        print("HEEERRREEE")
        print(baseline_transform_task.to_state_json())
        baseline_transform_out_dir_lkp=f'{baseline_transform_task.to_state_json()["ResultPath"]}.TransformOutput.S3OutputPath'

        get_baseline_preds_lambda = c_lambda.get_baseline_preds_lambda(
            self, 
            'GetBaselinePreds', 
            f'{self.name}-get-baseline-preds', 
            self.lambda_execution_role, 
            baseline_transform_out_dir_lkp, 
            baseline_X_filename_lkp, 
            self.baseline_dir,
            baseline_headered_file_lkp, 
            self.predict_label, 
            self.target_label, 
            self.target_type,
            layers=[self.pandas_layer_version]
        )
        baseline_pred_file_lkp = f'{get_baseline_preds_lambda.task._result_path}.BASELINE_PRED_FILE'
        baseline_full_dataset_lkp = f'{get_baseline_preds_lambda.task._result_path}.BASELINE_FULL_FILE'

        dq_baseline_ecs=c_ecs.get_dq_bl(
            self, 
            'DQBaselineJob', 
            f'{self.name}-dq-baseline', 
            self.baseline_image_repo,
            self.monitor_role,
            self.dq_monitor_dir,
            self.cluster,
            self.network,
            baseline_full_dataset_lkp,
            self.target_label,
            self.predict_label,
        )
        dq_baseline_job_name_lkp = f'{dq_baseline_ecs.task._result_path}.BASELINING_JOB_NAME'

        mq_baseline_ecs=c_ecs.get_mq_bl(
            self, 
            'MQBaselineJob', 
            f'{self.name}-mq-baseline-fn',
            self.baseline_image_repo,
            self.monitor_role,
            self.mq_monitor_dir, 
            self.cluster,
            self.network,
            baseline_full_dataset_lkp,
            self.target_label,
            self.predict_label,
            self.problem_type
        )
        mq_baseline_job_name_lkp = f'{mq_baseline_ecs.task._result_path}.BASELINING_JOB_NAME'

        mb_baseline_ecs = c_ecs.get_mb_bl(
            self, 
            'MBBaselineJob', 
            f'{self.name}-mb-baseline',
            self.baseline_image_repo,
            model_name_lkp,
            self.monitor_role, 
            self.mb_monitor_dir,
            self.cluster,
            self.network,
            baseline_full_dataset_lkp,
            self.target_label,
            self.predict_label,
            model_predicted_label_config={'label':None, 'probability':None, 'probability_threshold':None, 'label_headers':None},
            bias_config = {'label_values_or_threshold':[1], 'facet_name':'sex_F', 'facet_values_or_threshold':None, 'group_name':None}
        )
        mb_baseline_job_name_lkp = f'{mb_baseline_ecs.task._result_path}.BASELINING_JOB_NAME'

        me_baseline_ecs = c_ecs.get_me_bl(
            self, 
            'MEBaselineJob', 
            f'{self.name}-me-baseline',
            self.baseline_image_repo,
            model_name_lkp,
            self.monitor_role, 
            self.me_monitor_dir, 
            self.cluster,
            self.network,
            baseline_full_dataset_lkp,
            self.target_label,
            self.predict_label,
        )
        me_baseline_job_name_lkp = f'{me_baseline_ecs.task._result_path}.BASELINING_JOB_NAME'

        mon_baseline_jobs=      stepfunctions.Parallel(self, 'MonitorBaselineJobs', result_path='$.MonitorBaselineJobs').branch(dq_baseline_ecs.task).branch(mq_baseline_ecs.task)
        clarify_baseline_jobs=  stepfunctions.Parallel(self, 'ClarifyBaselineJobs', result_path='$.ClarifyBaselineJobs').branch(mb_baseline_ecs.task).branch(me_baseline_ecs.task)
        parallel_baseline_jobs= mon_baseline_jobs.next(clarify_baseline_jobs)



        # RT DEPLOY
        deploy_endpoint_lambda = c_lambda.deploy_endpoint_lambda(
            self,
            'DeployEndpoint', f'{self.name}-deploy-endpoint', self.lambda_execution_role,
            model_name_lkp, 
            self.model_package_group_name, 
            self.model_package_version_lkp, 
            self.endpoint_instance_type_lkp, 
            self.data_capture_dir
        )
        endpoint_name_lkp = f'{deploy_endpoint_lambda.task._result_path}.ENDPOINT_NAME'
        
        # MONITOR SCHEDULES
        schedule_dq_lambda = c_lambda.schedule_dq_task_lambda(
            self, 
            'ScheduleDQ', f'{self.name}-schedule-dq', self.lambda_execution_role, 'dq-mon', 
            endpoint_name_lkp,
            self.data_capture_dir,
            self.monitor_role,
            self.deploy_type,
            self.dq_monitor_dir,
            self.monitor_instance_type_lkp,
            self.monitor_schedule_expression_lkp,
            self.data_analysis_start_time_lkp,
            self.data_analysis_end_time_lkp,
        )
        conditional_schedule_dq_tasks = schedule_dq_mon_choice.when(schedule_dq_mon_cond, schedule_dq_lambda.task)

        schedule_mq_lambda = c_lambda.schedule_mq_task_lambda(
            self, 
            'ScheduleMQ', f'{self.name}-schedule-mq', self.lambda_execution_role, 'mq-mon',
            endpoint_name_lkp,
            self.data_capture_dir,
            self.monitor_role,
            self.deploy_type,
            self.problem_type,
            self.predict_label,
            self.ground_truth_dir_lkp,
            self.mq_monitor_dir,
            self.monitor_instance_type_lkp,
            self.monitor_schedule_expression_lkp,
            self.data_analysis_start_time_lkp,
            self.data_analysis_end_time_lkp,
        )
        conditional_schedule_mq_tasks = schedule_mq_mon_choice.when(schedule_mq_mon_cond, schedule_mq_lambda.task).afterwards()

        schedule_me_lambda = c_lambda.schedule_me_task_lambda(
            self, 
            'ScheduleME', f'{self.name}-schedule-me', self.lambda_execution_role, 'me-mon', 
            endpoint_name_lkp,
            self.data_capture_dir,
            self.lambda_execution_role,
            self.deploy_type,
            self.me_monitor_dir,
            self.monitor_instance_type_lkp,
            self.monitor_schedule_expression_lkp,
            self.data_analysis_start_time_lkp,
            self.data_analysis_end_time_lkp,
        )
        conditional_schedule_me_tasks = schedule_me_mon_choice.when(schedule_me_mon_cond, schedule_me_lambda.task).afterwards()

        schedule_mb_lambda = c_lambda.schedule_mb_task_lambda(
            self, 
            'ScheduleMB', f'{self.name}-schedule-mb', self.lambda_execution_role, 'mb-mon', 
            endpoint_name_lkp,
            self.data_capture_dir,
            self.lambda_execution_role,
            self.deploy_type,
            self.mb_monitor_dir,
            self.ground_truth_dir_lkp,
            self.monitor_instance_type_lkp,
            self.monitor_schedule_expression_lkp,
            self.data_analysis_start_time_lkp,
            self.data_analysis_end_time_lkp,
        )
        conditional_schedule_mb_tasks = schedule_mb_mon_choice.when(schedule_mb_mon_cond, schedule_mb_lambda.task).afterwards()

        parallel_monitor_scheduler=stepfunctions.Parallel(self, 'ParallelMonitorScheduler', result_path='$.ParallelMonitorScheduler') \
            .branch(conditional_schedule_dq_tasks) \
            .branch(conditional_schedule_mq_tasks) \
            .branch(conditional_schedule_me_tasks) \
            .branch(conditional_schedule_mb_tasks)

        # INF TRANSFORM
        # batch_transform_lambda = lambda_tasks.sm_transform_lambda(self, 'BatchTransform', f'{self.name}-batch-transform', model_name_lkp, self.transform_instance_type_lkp, s3_data_source_lkp=self.batch_input_dir_lkp, transform_out_dir=self.batch_out_dir)
        # batch_transform_job_arn_lkp = f'{baseline_transform_lambda.task._result_path}.TRANSFORM_JOB_ARN'
        # batch_transform_job_name_lkp = f'{baseline_transform_lambda.task._result_path}.JOB_NAME'
        # batch_transform_out_dir_lkp = f'{baseline_transform_lambda.task._result_path}.OUTPUT_PATH' # "s3://bucket/output/"
        # batch_transform_status_lkp = f'{baseline_transform_lambda.task._result_path}.STATUS'
        # batch_transform_chain, batch_transform_end, batch_transform_out_dir_lkp = c_sm.get_transform_task(
        #     self, 
        #     'BatchTransform', 
        #     f'{self.name}-batch-transform-job', 
        #     model_name_lkp, 
        #     self.transform_instance_type_lkp, 
        #     s3_data_source_lkp=self.batch_input_dir_lkp, 
        #     transform_out_dir=self.batch_out_dir
        # ) 
        batch_transform_task = c_sm.get_transform_task(
            self, 
            'BatchTransform', 
            f'{self.name}-batch-transform-job', 
            model_name_lkp, 
            self.transform_instance_type_lkp, 
            s3_data_source_lkp=self.batch_input_dir_lkp, 
            transform_out_dir=self.batch_out_dir
        ) 
        batch_transform_out_dir_lkp=f'{batch_transform_task.to_state_json()["ResultPath"]}.TransformOutput.S3OutputPath'

        check_dq_lambda = c_lambda.check_dq_task_lambda(self, 'CheckDQ', f'{self.name}-check-dq', self.lambda_execution_role)
        conditional_check_dq_tasks = check_dq_mon_choice.when(check_dq_mon_cond, check_dq_lambda.task).afterwards()
        check_mq_lambda = c_lambda.check_mq_task_lambda(self, 'CheckMQ', f'{self.name}-check-mq', self.lambda_execution_role)
        conditional_check_mq_tasks = check_mq_mon_choice.when(check_mq_mon_cond, check_mq_lambda.task).afterwards()
        check_me_lambda = c_lambda.check_me_task_lambda(self, 'CheckME', f'{self.name}-check-me', self.lambda_execution_role)
        conditional_check_me_tasks = check_me_mon_choice.when(check_me_mon_cond, check_me_lambda.task).afterwards()
        check_mb_lambda = c_lambda.check_mb_task_lambda(self, 'CheckMB', f'{self.name}-check-mb', self.lambda_execution_role)
        conditional_check_mb_tasks = check_mb_mon_choice.when(check_mb_mon_cond, check_mb_lambda.task).afterwards()

        pre_transform_parallel_monitor_checker=conditional_check_dq_tasks
        post_transform_parallel_monitor_checker = stepfunctions.Parallel(self, 'PostTransformMonitorChecker', result_path='$.PostTransformMonitorChecker').branch(conditional_check_mq_tasks).branch(conditional_check_mb_tasks).branch(conditional_check_me_tasks)

        # SUB CHAINS

        ### baseline_chain ###
        # Wire all exits
        # baseline_transform_end.next(get_baseline_preds_lambda.task).next(parallel_baseline_jobs)
        # baseline_chain = prep_baseline_sets_lambda.task.next(baseline_transform_chain)
        baseline_chain = prep_baseline_sets_lambda.task.next(baseline_transform_task).next(get_baseline_preds_lambda.task).next(parallel_baseline_jobs)

        ### deploy_chain ###
        deploy_chain = None
        if(self.deploy_type == 'realtime'):
            deploy_chain = deploy_endpoint_lambda.task.next(parallel_monitor_scheduler)
        else:
            deploy_chain = parallel_monitor_scheduler

        ### inference_chain ###
        if(self.deploy_type == 'realtime'):
            inference_chain = pre_transform_parallel_monitor_checker.next(post_transform_parallel_monitor_checker)
        else:
            # batch_transform_end.next(post_transform_parallel_monitor_checker)
            # inference_chain = pre_transform_parallel_monitor_checker.next(batch_transform_chain)
            inference_chain = pre_transform_parallel_monitor_checker.next(batch_transform_task).next(post_transform_parallel_monitor_checker)

        # # FULL CHAIN
        # chain=state_machine_start.next(get_or_create_model_from_registry_lambda.task) \
        #     .next( \
        #         rebaseline_choice.when(rebaseline_cond, baseline_chain).otherwise(action_choice).afterwards() \
        #     ).next( \
        #         action_choice.when(action_cond, \
        #             deploy_chain \
        #         ).otherwise( \
        #             inference_chain \
        #         ).afterwards() \
        #     ).next(statemachine_end)     
        
        # FULL CHAIN
        chain=state_machine_start.next(get_or_create_model_from_registry_lambda.task) \
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
        get_or_create_model_from_registry_lambda.add_invoker_arn(state_machine.state_machine_arn)
        prep_baseline_sets_lambda.add_invoker_arn(state_machine.state_machine_arn)
        get_baseline_preds_lambda.add_invoker_arn(state_machine.state_machine_arn)
        deploy_endpoint_lambda.add_invoker_arn(state_machine.state_machine_arn)
        schedule_dq_lambda.add_invoker_arn(state_machine.state_machine_arn)
        schedule_mq_lambda.add_invoker_arn(state_machine.state_machine_arn)
        schedule_me_lambda.add_invoker_arn(state_machine.state_machine_arn)
        schedule_mb_lambda.add_invoker_arn(state_machine.state_machine_arn)
        check_dq_lambda.add_invoker_arn(state_machine.state_machine_arn)
        check_mq_lambda.add_invoker_arn(state_machine.state_machine_arn)
        check_me_lambda.add_invoker_arn(state_machine.state_machine_arn)
        check_mb_lambda.add_invoker_arn(state_machine.state_machine_arn)
        # dq_baseline_lambda.grant_invoke(state_machine)
        # mq_baseline_lambda.grant_invoke(state_machine)
        # mb_baseline_lambda.grant_invoke(state_machine)
        # me_baseline_lambda.grant_invoke(state_machine)


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