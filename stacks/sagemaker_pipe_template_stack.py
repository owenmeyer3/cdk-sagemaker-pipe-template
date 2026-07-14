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
import custom_constructs.clambdas as clambdas
import custom_constructs.cecs as cecs
import custom_constructs.csms as csms 

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
        self.transform_role=iam.Role.from_role_arn(self, "ImportedTransformRole", env_config['TRANSFORM_ROLE_ARN'], mutable=False)
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
        #self.db_monitor_dir=  f'{self.pipeline_dir}/data-bias'

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
        self.agg_method_lkp = '$.BL_AGG_METHOD'

        state_machine_start = stepfunctions.Pass(self, 'Start')
        sf_launch_schedule = events.Schedule.rate(Duration.hours(1))
        statemachine_end = stepfunctions.Pass(self, 'End')

        # CHOICES / CONDITIONS
        rebaseline_choice = stepfunctions.Choice(self, "RebaselineChoice")
        rebaseline_cond =   stepfunctions.Condition.string_equals(self.rebaseline_lkp, "TRUE")
        action_choice =     stepfunctions.Choice(self, "ActionChoice")
        action_cond =            stepfunctions.Condition.string_equals(self.action_type_lkp, "deploy")
        # schedule_dq_mon_choice = stepfunctions.Choice(self, "ScheduleDqMonChoice")
        # schedule_dq_mon_cond =   stepfunctions.Condition.string_equals(self.enable_data_quality_monitoring_lkp, "TRUE")
        # schedule_mq_mon_choice = stepfunctions.Choice(self, "ScheduleMqMonChoice")
        # schedule_mq_mon_cond =   stepfunctions.Condition.string_equals(self.enable_model_quality_monitoring_lkp, "TRUE")
        # schedule_me_mon_choice = stepfunctions.Choice(self, "ScheduleMeMonChoice")
        # schedule_me_mon_cond =   stepfunctions.Condition.string_equals(self.enable_model_explainability_monitoring_lkp, "TRUE")
        # schedule_mb_mon_choice = stepfunctions.Choice(self, "ScheduleMbMonChoice")
        # schedule_mb_mon_cond =   stepfunctions.Condition.string_equals(self.enable_model_bias_monitoring_lkp, "TRUE")
        check_dq_mon_choice = stepfunctions.Choice(self, "CheckDqMonChoice")
        check_dq_mon_cond =   stepfunctions.Condition.string_equals(self.enable_data_quality_check_lkp, "TRUE")
        check_mq_mon_choice = stepfunctions.Choice(self, "CheckMqMonChoice")
        check_mq_mon_cond =   stepfunctions.Condition.string_equals(self.enable_model_quality_check_lkp, "TRUE")
        check_me_mon_choice = stepfunctions.Choice(self, "CheckMeMonChoice")
        check_me_mon_cond =   stepfunctions.Condition.string_equals(self.enable_model_explainability_check_lkp, "TRUE")
        check_mb_mon_choice = stepfunctions.Choice(self, "CheckMbMonChoice")
        check_mb_mon_cond =   stepfunctions.Condition.string_equals(self.enable_model_bias_check_lkp, "TRUE")

        # CREATE
        get_or_create_model_from_registry_lambda = clambdas.GetCreateModelLambda(self, 'GetOrCreateModel', f'{self.name}-get-or-create-model', self.lambda_execution_role, self.model_package_group_name, self.model_package_version_lkp, self.lambda_execution_role)
        model_name_lkp =        f'{get_or_create_model_from_registry_lambda.task._result_path}.MODEL_NAME'
        model_package_arn_lkp = f'{get_or_create_model_from_registry_lambda.task._result_path}.MODEL_PACKAGE_ARN'

        # BASELINE
        prep_baseline_lambda = clambdas.PrepBaselineLambda(self, 'PrepBaselineSets', f'{self.name}-prep-baseline-sets', self.lambda_execution_role, self.baseline_file_lkp, self.target_label, self.target_type, self.baseline_dir, baseline_cols_lkp=self.baseline_cols_lkp, layers=[self.pandas_layer_version])
        baseline_headered_file_lkp = f'{prep_baseline_lambda.task._result_path}.BASELINE_HEADERED_FILE'
        baseline_X_file_lkp = f'{prep_baseline_lambda.task._result_path}.BASELINE_X_FILE'

        baseline_transform_task = csms.TransformTask( 
            self, 
            'BaselineTransform', 
            f'bl-tr', 
            model_name_lkp=model_name_lkp,
            instance_type_lkp=self.transform_instance_type_lkp, 
            s3_data_source_lkp=baseline_X_file_lkp, 
            s3_out_dir=self.baseline_dir
        )
        baseline_transform_in_file_lkp=f'{baseline_transform_task.to_state_json()["ResultPath"]}.TransformInput.DataSource.S3DataSource.S3Uri'
        baseline_transform_out_dir_lkp=f'{baseline_transform_task.to_state_json()["ResultPath"]}.TransformOutput.S3OutputPath'
        baseline_transform_job_name_lkp=f'{baseline_transform_task.to_state_json()["ResultPath"]}.TransformJobName'

        process_baseline_lambda = clambdas.ProcessBaselinePredsLambda(
            self, 
            'GetBaselinePreds', 
            f'{self.name}-get-baseline-preds', 
            self.lambda_execution_role, 
            model_name_lkp,
            baseline_transform_out_dir_lkp, 
            self.baseline_dir,
            self.dq_monitor_dir, self.mq_monitor_dir, self.mb_monitor_dir, self.me_monitor_dir, self.agg_method_lkp,
            baseline_headered_file_lkp, 
            self.predict_label, 
            self.target_label, 
            self.target_type,
            layers=[self.pandas_layer_version]
        )
        # baseline_fs_p_file_lkp = f'{process_baseline_lambda.task._result_path}.BASELINE_FS_P_FILE'
        # baseline_full_dataset_lkp = f'{process_baseline_lambda.task._result_path}.BASELINE_FULL_FILE'
        baseline_dq_dataset_lkp = f'{process_baseline_lambda.task._result_path}.BASELINE_DQ_FILE'
        baseline_mq_dataset_lkp = f'{process_baseline_lambda.task._result_path}.BASELINE_MQ_FILE'
        baseline_mb_dataset_lkp = f'{process_baseline_lambda.task._result_path}.BASELINE_MB_FILE'
        baseline_me_dataset_lkp = f'{process_baseline_lambda.task._result_path}.BASELINE_ME_FILE'
        # mb_analysis_config_file_lkp = f'{process_baseline_lambda.task._result_path}.MB_ANALYSIS_CONFIG_FILE'
        # me_analysis_config_file_lkp = f'{process_baseline_lambda.task._result_path}.ME_ANALYSIS_CONFIG_FILE'

        dq_baseline_task=csms.DataQualityCheckTask(
            self, 'DQBaseline', f'{self.name}-dq-baseline',                                                  
            role=self.monitor_role, 
            dataset_lkp=baseline_dq_dataset_lkp, 
            s3_out_dir=f'{self.dq_monitor_dir}/info', 
            instance_type_lkp=self.monitor_instance_type_lkp
        )
        dq_bl_out_dir_lkp=f'{dq_baseline_task.to_state_json()["ResultPath"]}.ProcessingOutputConfig.Outputs[0].S3Output.S3Uri'

        mq_baseline_task=csms.ModelQualityCheckTask(
            self, 'MQBaseline', f'{self.name}-mq-baseline', 
            problem_type=self.problem_type,                  
            role=self.monitor_role, 
            dataset_lkp=baseline_mq_dataset_lkp, 
            s3_out_dir=f'{self.mq_monitor_dir}/info', 
            instance_type_lkp=self.monitor_instance_type_lkp,
            inference_attribute=self.predict_label,
            ground_truth_attribute=self.target_label
        )
        mq_bl_out_dir_lkp=f'{mq_baseline_task.to_state_json()["ResultPath"]}.ProcessingOutputConfig.Outputs[0].S3Output.S3Uri'

        mb_baseline_task=csms.ClarifyCheckTask(      
            self, 'MBBaseline', f'{self.name}-mb-baseline', 
            analysis_config_dir=f'{self.mb_monitor_dir}/info', 
            role=self.monitor_role, 
            dataset_lkp=baseline_mb_dataset_lkp, 
            s3_out_dir=f'{self.mb_monitor_dir}/info', 
            instance_type_lkp=self.monitor_instance_type_lkp
        )
        mb_bl_out_dir_lkp=f'{mb_baseline_task.to_state_json()["ResultPath"]}.ProcessingOutputConfig.Outputs[0].S3Output.S3Uri'

        me_baseline_task=csms.ClarifyCheckTask(      
            self, 'MEBaseline', f'{self.name}-me-baseline', 
            analysis_config_dir=f'{self.me_monitor_dir}/info', 
            role=self.monitor_role, 
            dataset_lkp=baseline_me_dataset_lkp, 
            s3_out_dir=f'{self.me_monitor_dir}/info', 
            instance_type_lkp=self.monitor_instance_type_lkp
        )
        me_bl_out_dir_lkp=f'{me_baseline_task.to_state_json()["ResultPath"]}.ProcessingOutputConfig.Outputs[0].S3Output.S3Uri'

        process_baseline_results_lambda=clambdas.ProcessBaselineResultsLambda(
            self, 
            'ProcessBLResults', 
            f'{self.name}-process_bl_results', 
            role=self.monitor_role,
            dq_bl_out_dir_lkp=dq_bl_out_dir_lkp, mq_bl_out_dir_lkp=mq_bl_out_dir_lkp, mb_bl_out_dir_lkp=mb_bl_out_dir_lkp, me_bl_out_dir_lkp=me_bl_out_dir_lkp,
            dq_monitor_dir=self.dq_monitor_dir, mq_monitor_dir=self.mq_monitor_dir, mb_monitor_dir=self.mb_monitor_dir, me_monitor_dir=self.me_monitor_dir,
        )
        
        # mon_baseline_jobs=      stepfunctions.Parallel(self, 'MonitorBaselineJobs', result_path='$.MonitorBaselineJobs').branch(dq_baseline_task).branch(mq_baseline_task)
        # clarify_baseline_jobs=  stepfunctions.Parallel(self, 'ClarifyBaselineJobs', result_path='$.ClarifyBaselineJobs').branch(mb_baseline_task).branch(me_baseline_task)
        # parallel_baseline_jobs= mon_baseline_jobs.next(clarify_baseline_jobs)
        parallel_baseline_jobs=stepfunctions.Parallel(self, 'BaselineJobs', result_path='$.BaselineJobs').branch(dq_baseline_task).branch(mq_baseline_task).branch(mb_baseline_task).branch(me_baseline_task)

        # RT DEPLOY
        deploy_endpoint_lambda = clambdas.DeployEndpointLambda(
            self,
            'DeployEndpoint', f'{self.name}-deploy-endpoint', self.lambda_execution_role,
            model_name_lkp, 
            self.model_package_group_name, 
            self.model_package_version_lkp, 
            self.endpoint_instance_type_lkp, 
            self.data_capture_dir
        )
        endpoint_name_lkp = f'{deploy_endpoint_lambda.task._result_path}.ENDPOINT_NAME'

        # INF TRANSFORM
        batch_transform_task = csms.TransformTask(
            self, 
            'BatchTransform', 
            f'btch-tr', 
            model_name_lkp=model_name_lkp,
            instance_type_lkp=self.transform_instance_type_lkp, 
            s3_data_source_lkp=self.batch_input_dir_lkp, 
            s3_out_dir=self.batch_out_dir
        ) 
        batch_transform_in_file_lkp= f'{batch_transform_task.to_state_json()["ResultPath"]}.TransformInput.DataSource.S3DataSource.S3Uri'
        batch_transform_out_dir_lkp= f'{batch_transform_task.to_state_json()["ResultPath"]}.TransformOutput.S3OutputPath'
        batch_transform_job_name_lkp=f'{batch_transform_task.to_state_json()["ResultPath"]}.TransformJobName'

        batch_capture_task = clambdas.TransformOutToDataCaptureLambda(
            self, 
            'BatchCapture', 
            f'{self.name}-btch-capture',
            self.lambda_execution_role, 
            batch_transform_job_name_lkp, 
            batch_transform_out_dir_lkp,
            self.baseline_dir,
            input_s3_file_lkp=batch_transform_in_file_lkp,
            variant_name_alias="AllTraffic",
            content_type="text/csv",
            encoding="CSV"
        )
        batch_capture_files_lkp = f'{batch_capture_task.task._result_path}.CAPTURE_FILES_WRITTEN'
        batch_capture_dir_lkp = f'{batch_capture_task.task._result_path}.CAPTURE_DESTINATION_PREFIX'

        # CHECKS
        dq_check_task=csms.DataQualityCheckTask(
            self, 'DQCheck', f'{self.name}-dq-check',                                                  
            role=self.monitor_role, 
            dataset_lkp=baseline_dq_dataset_lkp, 
            s3_out_dir=f'{self.dq_monitor_dir}/info', 
            instance_type_lkp=self.monitor_instance_type_lkp
        )
        
        mq_check_task=csms.ModelQualityCheckTask(
            self, 'MQCheck', f'{self.name}-mq-check', 
            problem_type=self.problem_type,                  
            role=self.monitor_role, 
            dataset_lkp=baseline_mq_dataset_lkp, 
            s3_out_dir=f'{self.mq_monitor_dir}/info', 
            instance_type_lkp=self.monitor_instance_type_lkp,
            inference_attribute=self.predict_label,
            ground_truth_attribute=self.target_label
        )

        mb_check_task=csms.ClarifyCheckTask(      
            self, 'MBCheck', f'{self.name}-mb-check', 
            analysis_config_dir=f'{self.mb_monitor_dir}/info', 
            role=self.monitor_role, 
            dataset_lkp=baseline_mb_dataset_lkp, 
            s3_out_dir=f'{self.mb_monitor_dir}/info', 
            instance_type_lkp=self.monitor_instance_type_lkp
        )

        me_check_task=csms.ClarifyCheckTask(      
            self, 'MECheck', f'{self.name}-me-check', 
            analysis_config_dir=f'{self.me_monitor_dir}/info', 
            role=self.monitor_role, 
            dataset_lkp=baseline_me_dataset_lkp, 
            s3_out_dir=f'{self.me_monitor_dir}/info', 
            instance_type_lkp=self.monitor_instance_type_lkp
        )

        current_constraints_uri=''
        current_statistics_uri=''
        dq_val_lambda=clambdas.ValidateDataQualityLambda(
            self, 'DQVal', f'{self.name}-dq-val', 
            self.monitor_role, current_constraints_uri, f'{self.dq_monitor_dir}/info/constraints.json', current_statistics_uri, f'{self.dq_monitor_dir}/info/statistics.json', comparison_threshold=0.1, fail_on_violation = True
        )
        dq_should_fail = f'{dq_val_lambda.task._result_path}.SHOULD_FAIL_PIPELINE'
        current_constraints_uri=''
        mq_val_lambda=clambdas.ValidateModelQualityLambda(
            self, 'MQVal', f'{self.name}-mq-val', 
            self.monitor_role, current_constraints_uri, f'{self.mq_monitor_dir}/info/constraints.json', fail_on_violation = True, problem_type="Regression"
        )
        mb_val_taskq_should_fail = f'{mq_val_lambda.task._result_path}.SHOULD_FAIL_PIPELINE'
        current_analysis_uri=''
        mb_val_lambda=clambdas.ValidateModelBiasQualityLambda(
            self, 'MBVal', f'{self.name}-mb-val', 
            self.monitor_role, current_analysis_uri, f'{self.mb_monitor_dir}/info/analysis_config.json', fail_on_violation = True
        )
        mb_should_fail = f'{mb_val_lambda.task._result_path}.SHOULD_FAIL_PIPELINE'
        current_analysis_uri=''
        me_val_lambda=clambdas.ValidateModelExplainabilityLambda(
            self, 'MEVal', f'{self.name}-me-val', 
            self.monitor_role, current_analysis_uri, f'{self.me_monitor_dir}/info/analysis_config.json', ndcg_violation_threshold=0.9, fail_on_violation = True
        )
        me_should_fail = f'{me_val_lambda.task._result_path}.SHOULD_FAIL_PIPELINE'
        conditional_check_dq_tasks = check_dq_mon_choice.when(check_dq_mon_cond, dq_check_task.next(dq_val_lambda.task)).otherwise(stepfunctions.Pass(self, 'NoDqCheck')).afterwards()
        conditional_check_mq_tasks = check_mq_mon_choice.when(check_mq_mon_cond, mq_check_task.next(mq_val_lambda.task)).otherwise(stepfunctions.Pass(self, 'NoMqCheck')).afterwards()
        conditional_check_mb_tasks = check_mb_mon_choice.when(check_mb_mon_cond, mb_check_task.next(mb_val_lambda.task)).otherwise(stepfunctions.Pass(self, 'NoMbCheck')).afterwards()
        conditional_check_me_tasks = check_me_mon_choice.when(check_me_mon_cond, me_check_task.next(me_val_lambda.task)).otherwise(stepfunctions.Pass(self, 'NoMeCheck')).afterwards()
        
        pre_transform_parallel_monitor_checker=conditional_check_dq_tasks
        post_transform_parallel_monitor_checker = stepfunctions.Parallel(self, 'PostTransformMonitorChecker', result_path='$.PostTransformMonitorChecker').branch(conditional_check_mq_tasks).branch(conditional_check_mb_tasks).branch(conditional_check_me_tasks)

        # SUB CHAINS

        ### baseline_chain ###
        baseline_chain = prep_baseline_lambda.task.next(baseline_transform_task).next(process_baseline_lambda.task).next(parallel_baseline_jobs).next(process_baseline_results_lambda.task)

        ### deploy_chain ###
        deploy_chain = None
        if(self.deploy_type == 'realtime'):
            deploy_chain = deploy_endpoint_lambda.task#.next(parallel_monitor_scheduler)
        else:
            deploy_chain = stepfunctions.Pass(self, 'PassBatchDeploy')

        ### inference_chain ###
        if(self.deploy_type == 'realtime'):
            inference_chain = pre_transform_parallel_monitor_checker.next(post_transform_parallel_monitor_checker)
        else:
            inference_chain = pre_transform_parallel_monitor_checker.next(batch_transform_task).next(batch_capture_task.task).next(post_transform_parallel_monitor_checker)

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
        prep_baseline_lambda.add_invoker_arn(state_machine.state_machine_arn)
        process_baseline_lambda.add_invoker_arn(state_machine.state_machine_arn)
        deploy_endpoint_lambda.add_invoker_arn(state_machine.state_machine_arn)
        dq_val_lambda.add_invoker_arn(state_machine.state_machine_arn)
        mq_val_lambda.add_invoker_arn(state_machine.state_machine_arn)
        mb_val_lambda.add_invoker_arn(state_machine.state_machine_arn)
        me_val_lambda.add_invoker_arn(state_machine.state_machine_arn)
        # schedule_dq_lambda.add_invoker_arn(state_machine.state_machine_arn)
        # schedule_mq_lambda.add_invoker_arn(state_machine.state_machine_arn)
        # schedule_me_lambda.add_invoker_arn(state_machine.state_machine_arn)
        # schedule_mb_lambda.add_invoker_arn(state_machine.state_machine_arn)
        # check_dq_lambda.add_invoker_arn(state_machine.state_machine_arn)
        # check_mq_lambda.add_invoker_arn(state_machine.state_machine_arn)
        # check_me_lambda.add_invoker_arn(state_machine.state_machine_arn)
        # check_mb_lambda.add_invoker_arn(state_machine.state_machine_arn)
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