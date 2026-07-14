import json
from aws_cdk import (
    aws_logs as logs,
    aws_lambda as _lambda,
    aws_stepfunctions as stepfunctions,
    aws_stepfunctions_tasks as tasks,
    aws_ecr as ecr,
    aws_iam as iam,
    Duration,
    RemovalPolicy
)
from custom_constructs.CLambda import Lambda


class DeployEndpointLambda(Lambda):
    def __init__(
        self,
        scope, 
        construct_id, 
        function_name, 
        role, 
        model_name_lkp, 
        model_package_group_name, 
        model_package_version_lkp, 
        endpoint_instance_type_lkp, 
        data_capture_dir
    ):
        super().__init__(
            scope, 
            construct_id,
            function_config={
                'type':'Basic',
                'code_path':'code/deploy_endpoint/',
                'handler':'deploy_endpoint.handler',
                'runtime':'python3.11',
                'function_name':function_name,
                'role':role,
            },
            log_config={
                'log_group_name':f"/lambda/{function_name}"
            },
            task_config={
                'outputs':['ENDPOINT_NAME'],
                'payload':{
                    'model_name': stepfunctions.JsonPath.string_at(model_name_lkp),
                    'model_package_group_name':model_package_group_name,
                    'model_package_version': stepfunctions.JsonPath.string_at(model_package_version_lkp),
                    'instance_type': stepfunctions.JsonPath.string_at(endpoint_instance_type_lkp),
                    'data_capture_dir':data_capture_dir
                }
            }
        )


class GetCreateModelLambda(Lambda):
    def __init__(
        self,
        scope, 
        construct_id, 
        function_name, 
        role, 
        model_package_group_name, 
        model_package_version_lkp, 
        create_model_role
    ):
        super().__init__(
            scope, 
            construct_id,
            function_config={
                'type':'Basic',
                'code_path':'code/get_or_create_model_from_registry',
                'handler':'get_or_create_model_from_registry.handler',
                'runtime':'python3.11',
                'function_name':function_name,
                'role':role,
            },
            log_config={
                'log_group_name':f"/lambda/{function_name}"
            },
            task_config={
                'outputs':['MODEL_NAME', 'MODEL_PACKAGE_ARN'],
                'payload':{
                    'model_package_group_name': model_package_group_name,
                    'model_package_version': stepfunctions.JsonPath.string_at(model_package_version_lkp),
                    'create_model_role':create_model_role.role_arn
                }
            }
        )


class PrepBaselineLambda(Lambda):
    def __init__(
        self,
        scope, 
        construct_id, 
        function_name, 
        role, 
        baseline_file_lkp, 
        target_label, 
        target_type, 
        baseline_dir, 
        baseline_cols_lkp = [], 
        layers=[]
    ):
        super().__init__(
            scope, 
            construct_id,
            function_config={
                'type':'Basic',
                'code_path':'code/baseline_general/',
                'handler':'baseline_general.prep_baseline_sets_handler',
                'runtime':'python3.11',
                'function_name':function_name,
                'role':role,
                'layers':layers
            },
            log_config={
                'log_group_name':f"/lambda/{function_name}"
            },
            task_config={
                'outputs':['BASELINE_HEADERED_FILE', 'BASELINE_X_FILE'],
                'payload':{
                    'baseline_file': stepfunctions.JsonPath.string_at(baseline_file_lkp),
                    'target_label':target_label,
                    'target_type': target_type,
                    'baseline_dir':baseline_dir,
                    'columns':stepfunctions.JsonPath.string_at(baseline_cols_lkp)
                }
            }
        )


class ProcessBaselinePredsLambda(Lambda):
    def __init__(
        self,
        scope, 
        construct_id, 
        function_name, 
        role, 
        model_name_lkp,
        transform_out_dir_lkp, 
        baseline_dir, 
        dq_monitor_dir, 
        mq_monitor_dir, 
        mb_monitor_dir, 
        me_monitor_dir, 
        agg_method_lkp, 
        baseline_headered_file_lkp, 
        predict_label, 
        target_label, 
        target_type, 
        layers=[]
    ):
        super().__init__(
            scope, 
            construct_id,
            function_config={
                'type':'Basic',
                'code_path':'code/baseline_general/',
                'handler':'baseline_general.get_baseline_preds_handler',
                'runtime':'python3.11',
                'function_name':function_name,
                'role':role,
                'layers':layers
            },
            log_config={
                'log_group_name':f"/lambda/{function_name}"
            },
            task_config={
                'outputs':['BASELINE_FS_P_FILE', 'BASELINE_FULL_FILE', 'BASELINE_DQ_FILE', 'BASELINE_MQ_FILE', 'BASELINE_MB_FILE', 'BASELINE_ME_FILE', 'MB_ANALYSIS_CONFIG_FILE', 'ME_ANALYSIS_CONFIG_FILE'],
                'payload':{
                    'model_name': stepfunctions.JsonPath.string_at(model_name_lkp),
                    'transform_out_dir': stepfunctions.JsonPath.string_at(transform_out_dir_lkp),
                    'baseline_dir': baseline_dir,
                    'dq_monitor_dir': dq_monitor_dir,
                    'mq_monitor_dir': mq_monitor_dir,
                    'mb_monitor_dir': mb_monitor_dir,
                    'me_monitor_dir': me_monitor_dir,
                    'agg_method': stepfunctions.JsonPath.string_at(agg_method_lkp),
                    'baseline_headered_file':stepfunctions.JsonPath.string_at(baseline_headered_file_lkp),
                    'predict_label':predict_label,
                    'target_label':target_label,
                    'target_type':target_type

                }
            }
        )


class ValidateDataQualityLambda(Lambda):
    def __init__(
        self,
        scope, 
        construct_id, 
        function_name, 
        role, 
        current_constraints_uri, 
        baseline_constraints_uri, 
        current_statistics_uri, 
        baseline_statistics_uri, 
        comparison_threshold=0.1, 
        fail_on_violation = True
    ):
        super().__init__(
            scope, 
            construct_id,
            function_config={
                'type':'Basic',
                'code_path':'code/violation_checks/',
                'handler':'main.dq_handler',
                'runtime':'python3.11',
                'function_name':function_name,
                'role':role,
            },
            log_config={
                'log_group_name':f"/lambda/{function_name}"
            },
            task_config={
                'outputs':["PASSED", "VIOLATION_COUNT", "VIOLATIONS", "FAIL_ON_VIOLATION", "SHOULD_FAIL_PIPELINE"],
                'payload': {
                    "current_constraints_uri": current_constraints_uri,
                    "baseline_constraints_uri": baseline_constraints_uri,
                    "current_statistics_uri": current_statistics_uri,
                    "baseline_statistics_uri": baseline_statistics_uri,
                    "fail_on_violation": fail_on_violation,
                    "comparison_threshold":comparison_threshold
                }
            }
        )


class ValidateModelQualityLambda(Lambda):
    def __init__(
        self,
        scope, 
        construct_id, 
        function_name, 
        role, 
        current_constraints_uri, 
        baseline_constraints_uri, 
        fail_on_violation = True, 
        problem_type="Regression"
    ):

        super().__init__(
            scope, 
            construct_id,
            function_config={
                'type':'Basic',
                'code_path':'code/violation_checks/',
                'handler':'main.mq_handler',
                'runtime':'python3.11',
                'function_name':function_name,
                'role':role,
            },
            log_config={
                'log_group_name':f"/lambda/{function_name}"
            },
            task_config={
                'outputs':["PASSED", "VIOLATION_COUNT", "VIOLATIONS", "FAIL_ON_VIOLATION", "SHOULD_FAIL_PIPELINE"],
                'payload':{
                    "problem_type": problem_type, # "BinaryClassification" | "MulticlassClassification" | "Regression"
                    "current_constraints_uri": current_constraints_uri,
                    "baseline_constraints_uri": baseline_constraints_uri,
                    "fail_on_violation": fail_on_violation
                }
            }
        )


class ValidateModelBiasQualityLambda(Lambda):
    def __init__(
        self, 
        scope, 
        construct_id, 
        function_name, 
        role, 
        current_analysis_uri, 
        baseline_analysis_uri, 
        fail_on_violation = True
    ):
        super().__init__(
            scope, 
            construct_id,
            function_config={
                'type':'Basic',
                'code_path':'code/violation_checks/',
                'handler':'main.mb_handler',
                'runtime':'python3.11',
                'function_name':function_name,
                'role':role,
            },
            log_config={
                'log_group_name':f"/lambda/{function_name}"
            },
            task_config={
                'outputs':["PASSED", "VIOLATION_COUNT", "VIOLATIONS", "FAIL_ON_VIOLATION", "SHOULD_FAIL_PIPELINE"],
                'payload':{
                    "current_analysis_uri": current_analysis_uri,
                    "baseline_analysis_uri": baseline_analysis_uri,
                    "fail_on_violation": fail_on_violation
                }
            }
        )

class ValidateModelExplainabilityLambda(Lambda):
    def __init__(
        self, 
        scope, 
        construct_id, 
        function_name, 
        role, 
        current_analysis_uri, 
        baseline_analysis_uri,
        ndcg_violation_threshold=0.9, 
        fail_on_violation = True
    ):

        super().__init__(scope, construct_id,
            function_config={
                'type':'Basic',
                'code_path':'code/violation_checks/',
                'handler':'main.me_handler',
                'runtime':'python3.11',
                'function_name':function_name,
                'role':role,
            },
            log_config={
                'log_group_name':f"/lambda/{function_name}"
            },
            task_config={
                'outputs':["PASSED", "VIOLATION_COUNT", "VIOLATIONS", "FAIL_ON_VIOLATION", "SHOULD_FAIL_PIPELINE"],
                'payload':{
                    "current_analysis_uri": current_analysis_uri,
                    "baseline_analysis_uri": baseline_analysis_uri,
                    "fail_on_violation": fail_on_violation,
                    "ndcg_violation_threshold": ndcg_violation_threshold
                }
            }
        )


class TransformOutToDataCaptureLambda(Lambda):
    def __init__(
        self, 
        scope, 
        construct_id, 
        function_name, 
        role, 
        transform_job_name_lkp, 
        output_s3_dir_lkp,
        capture_dir,
        input_s3_file_lkp=None, # alternative to num_input_columns
        num_input_columns=None,
        variant_name_alias="AllTraffic",
        content_type="text/csv",
        encoding="CSV"
    ):
        super().__init__(scope, construct_id,
            function_config={
                'type':'Basic',
                'code_path':'code/transform/',
                'handler':'main.transform_out_to_data_capture_handler',
                'runtime':'python3.11',
                'function_name':function_name,
                'role':role,
            },
            log_config={
                'log_group_name':f"/lambda/{function_name}"
            },
            task_config={
                'outputs':["STATUS_CODE", "TRANSFORM_JOB_NAME", "NUM_INPUT_COLUMNS_USED", "CAPTURE_FILES_WRITTEN", "CAPTURE_DESTINATION_PREFIX"],
                'payload':{
                    "transform_job_name": stepfunctions.JsonPath.string_at(transform_job_name_lkp),
                    "output_s3_dir": stepfunctions.JsonPath.string_at(output_s3_dir_lkp),
                    "capture_dir": capture_dir,
                    "endpoint_name_alias": stepfunctions.JsonPath.string_at(transform_job_name_lkp),
                    "variant_name_alias": variant_name_alias,
                    "content_type": content_type,
                    "encoding": encoding,
                    "num_input_columns":num_input_columns, 
                    "input_s3_file": stepfunctions.JsonPath.string_at(input_s3_file_lkp),
                    "execution_start_time": stepfunctions.JsonPath.string_at("$$.Execution.StartTime")
                }
            }
        )

class ProcessBaselineResultsLambda(Lambda):
    def __init__(
        self, 
        scope, 
        construct_id, 
        function_name, 
        role, 
        dq_bl_out_dir_lkp=None,
        mq_bl_out_dir_lkp=None,
        mb_bl_out_dir_lkp=None,
        me_bl_out_dir_lkp=None, # alternative to num_input_columns
        dq_monitor_dir=None,
        mq_monitor_dir=None,
        mb_monitor_dir=None,
        me_monitor_dir=None,
    ):
        super().__init__(scope, construct_id,
            function_config={
                'type':'Basic',
                'code_path':'code/baseline_general/',
                'handler':'baseline_general.process_baseline_results_handler',
                'runtime':'python3.11',
                'function_name':function_name,
                'role':role,
            },
            log_config={
                'log_group_name':f"/lambda/{function_name}"
            },
            task_config={
                'outputs':[],
                'payload':{
                    "dq_bl_out_dir": stepfunctions.JsonPath.string_at(dq_bl_out_dir_lkp),
                    "mq_bl_out_dir": stepfunctions.JsonPath.string_at(mq_bl_out_dir_lkp),
                    "mb_bl_out_dir": stepfunctions.JsonPath.string_at(mb_bl_out_dir_lkp),
                    "me_bl_out_dir": stepfunctions.JsonPath.string_at(me_bl_out_dir_lkp),
                    "dq_monitor_dir": dq_monitor_dir,
                    "mq_monitor_dir": mq_monitor_dir,
                    "mb_monitor_dir": mb_monitor_dir,
                    "me_monitor_dir": me_monitor_dir,
                }
            }
        )

# def schedule_dq_task_lambda(scope, 
#         construct_id,
#         function_name, 
#         role,
#         monitor_name,
#         endpoint_name_lkp,
#         data_capture_dir,
#         monitor_role,
#         deploy_type,
#         dq_monitor_dir,
#         monitor_instance_type_lkp,
#         schedule_expression_lkp,
#         data_analysis_start_time_lkp,
#         data_analysis_end_time_lkp,
#         image_uri='156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer', 
#         instance_count=1, 
#         volume_size_in_gb=20, 
#         max_runtime_in_seconds=1800, 
#         dataset_format={'Csv': {'Header': True}}
#     ):

#     return Lambda(scope, construct_id,
#         function_config={
#             'type':'Basic',
#             'code_path':'code/schedule_monitors/',
#             'handler':'schedule_monitors.data_quality_handler',
#             'runtime':'python3.11',
#             'function_name':function_name,
#             'role':role,
#         },
#         log_config={
#             'log_group_name':f"/lambda/{function_name}"
#         },
#         task_config={
#             'outputs':[],
#             'payload':{
#                 'name': monitor_name,
#                 'endpoint_name': stepfunctions.JsonPath.string_at(endpoint_name_lkp),
#                 'data_capture_dir':data_capture_dir,
#                 'monitor_role': monitor_role.role_arn,
#                 'deploy_type':deploy_type,
#                 'monitor_dir': dq_monitor_dir,
#                 'image_uri': image_uri,
#                 'instance_count':instance_count,
#                 'instance_type':stepfunctions.JsonPath.string_at(monitor_instance_type_lkp),
#                 'volume_size_in_gb': volume_size_in_gb,
#                 'max_runtime_in_seconds': max_runtime_in_seconds,
#                 'dataset_format':json.dumps(dataset_format),
#                 'schedule_expression':stepfunctions.JsonPath.string_at(schedule_expression_lkp),
#                 'data_analysis_start_time':stepfunctions.JsonPath.string_at(data_analysis_start_time_lkp),
#                 'data_analysis_end_time':stepfunctions.JsonPath.string_at(data_analysis_end_time_lkp)
#             }
#         }
#     )


# def schedule_mb_task_lambda(scope, 
#         construct_id,
#         function_name, 
#         role,
#         monitor_name,
#         endpoint_name_lkp,
#         data_capture_dir,
#         monitor_role,
#         deploy_type,
#         mb_monitor_dir,
#         ground_truth_dir_lkp,
#         monitor_instance_type_lkp,
#         schedule_expression_lkp,
#         data_analysis_start_time_lkp,
#         data_analysis_end_time_lkp,
#         image_uri='156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer', 
#         instance_count=1, 
#         volume_size_in_gb=20, 
#         max_runtime_in_seconds=1800, 
#         dataset_format={'Csv': {'Header': True}}
#     ):

#     return Lambda(scope, construct_id,
#         function_config={
#             'type':'Basic',
#             'code_path':'code/schedule_monitors/',
#             'handler':'schedule_monitors.model_bias_handler',
#             'runtime':'python3.11',
#             'function_name':function_name,
#             'role':role,
#         },
#         log_config={
#             'log_group_name':f"/lambda/{function_name}"
#         },
#         task_config={
#             'outputs':[],
#             'payload':{
#                 'name': monitor_name,
#                 'endpoint_name': stepfunctions.JsonPath.string_at(endpoint_name_lkp),
#                 'data_capture_dir':data_capture_dir,
#                 'monitor_role': monitor_role.role_arn,
#                 'deploy_type':deploy_type,
#                 'monitor_dir': mb_monitor_dir,
#                 'ground_truth_dir': stepfunctions.JsonPath.string_at(ground_truth_dir_lkp),
#                 'image_uri': image_uri,
#                 'instance_count':instance_count,
#                 'instance_type':stepfunctions.JsonPath.string_at(monitor_instance_type_lkp),
#                 'volume_size_in_gb': volume_size_in_gb,
#                 'max_runtime_in_seconds': max_runtime_in_seconds,
#                 'dataset_format':json.dumps(dataset_format),
#                 'schedule_expression':stepfunctions.JsonPath.string_at(schedule_expression_lkp),
#                 'data_analysis_start_time':stepfunctions.JsonPath.string_at(data_analysis_start_time_lkp),
#                 'data_analysis_end_time':stepfunctions.JsonPath.string_at(data_analysis_end_time_lkp)
#             }
#         }
#     )


# def schedule_me_task_lambda(scope, 
#         construct_id,
#         function_name, 
#         role,
#         monitor_name,
#         endpoint_name_lkp,
#         data_capture_dir,
#         monitor_role,
#         deploy_type,
#         me_monitor_dir,
#         monitor_instance_type_lkp,
#         schedule_expression_lkp,
#         data_analysis_start_time_lkp,
#         data_analysis_end_time_lkp,
#         image_uri='156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer', 
#         instance_count=1, 
#         volume_size_in_gb=20, 
#         max_runtime_in_seconds=1800, 
#         dataset_format={'Csv': {'Header': True}}
#     ):

#     return Lambda(scope, construct_id,
#         function_config={
#             'type':'Basic',
#             'code_path':'code/schedule_monitors/',
#             'handler':'schedule_monitors.model_explainability_handler',
#             'runtime':'python3.11',
#             'function_name':function_name,
#             'role':role,
#         },
#         log_config={
#             'log_group_name':f"/lambda/{function_name}"
#         },
#         task_config={
#             'outputs':[],
#             'payload':{
#                 'name': monitor_name,
#                 'endpoint_name': stepfunctions.JsonPath.string_at(endpoint_name_lkp),
#                 'data_capture_dir':data_capture_dir,
#                 'monitor_role': monitor_role.role_arn,
#                 'deploy_type':deploy_type,
#                 'monitor_dir': me_monitor_dir,
#                 'image_uri': image_uri,
#                 'instance_count':instance_count,
#                 'instance_type':stepfunctions.JsonPath.string_at(monitor_instance_type_lkp),
#                 'volume_size_in_gb': volume_size_in_gb,
#                 'max_runtime_in_seconds': max_runtime_in_seconds,
#                 'dataset_format':json.dumps(dataset_format),
#                 'schedule_expression':stepfunctions.JsonPath.string_at(schedule_expression_lkp),
#                 'data_analysis_start_time':stepfunctions.JsonPath.string_at(data_analysis_start_time_lkp),
#                 'data_analysis_end_time':stepfunctions.JsonPath.string_at(data_analysis_end_time_lkp)
#             }
#         }
#     )

# def schedule_mq_task_lambda(scope, 
#         construct_id,
#         function_name, 
#         role,
#         monitor_name,
#         endpoint_name_lkp,
#         data_capture_dir,
#         monitor_role,
#         deploy_type,
#         problem_type,
#         predict_label,
#         ground_truth_dir_lkp,
#         mq_monitor_dir,
#         monitor_instance_type_lkp,
#         schedule_expression_lkp,
#         data_analysis_start_time_lkp,
#         data_analysis_end_time_lkp,
#         image_uri='156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer', 
#         instance_count=1, 
#         volume_size_in_gb=20, 
#         max_runtime_in_seconds=1800, 
#         dataset_format={'Csv': {'Header': True}}
#     ):

#     return Lambda(scope, construct_id,
#         function_config={
#             'type':'Basic',
#             'code_path':'code/schedule_monitors/',
#             'handler':'schedule_monitors.model_quality_handler',
#             'runtime':'python3.11',
#             'function_name':function_name,
#             'role':role,
#         },
#         log_config={
#             'log_group_name':f"/lambda/{function_name}"
#         },
#         task_config={
#             'outputs':[],
#             'payload':{
#                 'name': monitor_name,
#                 'endpoint_name': stepfunctions.JsonPath.string_at(endpoint_name_lkp),
#                 'data_capture_dir':data_capture_dir,
#                 'monitor_role': monitor_role.role_arn,
#                 'deploy_type':deploy_type,
#                 'problem_type':problem_type,
#                 'predict_label':predict_label,
#                 'monitor_dir': mq_monitor_dir,
#                 'ground_truth_dir':stepfunctions.JsonPath.string_at(ground_truth_dir_lkp),
#                 'image_uri': image_uri,
#                 'instance_count':instance_count,
#                 'instance_type':stepfunctions.JsonPath.string_at(monitor_instance_type_lkp),
#                 'volume_size_in_gb': volume_size_in_gb,
#                 'max_runtime_in_seconds': max_runtime_in_seconds,
#                 'dataset_format':json.dumps(dataset_format),
#                 'schedule_expression':stepfunctions.JsonPath.string_at(schedule_expression_lkp),
#                 'data_analysis_start_time':stepfunctions.JsonPath.string_at(data_analysis_start_time_lkp),
#                 'data_analysis_end_time':stepfunctions.JsonPath.string_at(data_analysis_end_time_lkp)
#             }
#         }
#     )


# def check_dq_task_lambda(scope, construct_id, function_name, role):

#     return Lambda(scope, construct_id,
#         function_config={
#             'type':'Basic',
#             'code_path':'code/check_monitors/',
#             'handler':'check_monitors.data_quality_handler',
#             'runtime':'python3.11',
#             'function_name':function_name,
#             'role':role,
#         },
#         log_config={
#             'log_group_name':f"/lambda/{function_name}"
#         },
#         task_config={
#             'outputs':[],
#             'payload':{}
#         }
#     )

# def check_mq_task_lambda(scope, construct_id, function_name, role):

#     return Lambda(scope, construct_id,
#         function_config={
#             'type':'Basic',
#             'code_path':'code/check_monitors/',
#             'handler':'check_monitors.model_quality_handler',
#             'runtime':'python3.11',
#             'function_name':function_name,
#             'role':role,
#         },
#         log_config={
#             'log_group_name':f"/lambda/{function_name}"
#         },
#         task_config={
#             'outputs':[],
#             'payload':{}
#         }
#     )

# def check_me_task_lambda(scope, construct_id, function_name, role):

#     return Lambda(scope, construct_id,
#         function_config={
#             'type':'Basic',
#             'code_path':'code/check_monitors/',
#             'handler':'check_monitors.model_explainability_handler',
#             'runtime':'python3.11',
#             'function_name':function_name,
#             'role':role,
#         },
#         log_config={
#             'log_group_name':f"/lambda/{function_name}"
#         },
#         task_config={
#             'outputs':[],
#             'payload':{}
#         }
#     )

# def check_mb_task_lambda(scope, construct_id, function_name, role):

#     return Lambda(scope, construct_id,
#         function_config={
#             'type':'Basic',
#             'code_path':'code/check_monitors/',
#             'handler':'check_monitors.model_bias_handler',
#             'runtime':'python3.11',
#             'function_name':function_name,
#             'role':role,
#         },
#         log_config={
#             'log_group_name':f"/lambda/{function_name}"
#         },
#         task_config={
#             'outputs':[],
#             'payload':{}
#         }
#     )

# def analysis_config_build_lambda(scope, construct_id, function_name, role, bl_config_file, mb_monitor_dir, me_monitor_dir, agg_method):

#     return Lambda(scope, construct_id,
#         function_config={
#             'type':'Basic',
#             'code_path':'code/check_monitors/',
#             'handler':'check_monitors.analysis_config_build_handler',
#             'runtime':'python3.11',
#             'function_name':function_name,
#             'role':role
#         },
#         log_config={
#             'log_group_name':f"/lambda/{function_name}"
#         },
#         task_config={
#             'outputs':['MB_ANALYSIS_CONFIG_FILE', 'ME_ANALYSIS_CONFIG_FILE'],
#             'payload':{
#                 'bl_config_file': bl_config_file,
#                 'mb_monitor_dir': mb_monitor_dir,
#                 'me_monitor_dir': me_monitor_dir,
#                 'agg_method': agg_method

#             }
#         }
#     )