# response = sm_client.create_processing_job(
#     ProcessingJobName='inspect-model-monitor-container-10',
#     AppSpecification={
#         'ImageUri': '156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer',
#         'ContainerEntrypoint': ['/bin/bash', '-c'],
#         'ContainerArguments': [
#             'cat /opt/program/analysis/analytics_input.py  2>/dev/null'
#         ]
#     },
#     ProcessingResources={
#         'ClusterConfig': {
#             'InstanceCount': 1,
#             'InstanceType': 'ml.m5.large',
#             'VolumeSizeInGB': 20
#         }
#     },
#     ProcessingOutputConfig={
#         'Outputs': [{
#             'OutputName': 'inspection_output',
#             'S3Output': {
#                 'S3Uri': 's3://omm-test-bucket/pipelines/abalone/container-inspect',
#                 'LocalPath': '/opt/ml/processing/output',
#                 'S3UploadMode': 'EndOfJob'
#             }
#         }]
#     },
#     RoleArn="arn:aws:iam::088461143167:role/SageMakerExecutionRole-1",
#     StoppingCondition={'MaxRuntimeInSeconds': 300}
# )

import json
class AnalyticsInput:
    """
    Input class for data analyzer
    """
    def __init__(self,
                 dataset_source, dataset_format, output_path,
                 analysis_type, problem_type,
                 inference_attribute = None, probability_attribute = None, ground_truth_attribute = None, probability_threshold_attribute = None,
                 positive_label = None, cloudwatch_metrics_directory = None, output_message_file = None,
                 start_time = None, end_time = None, metric_time = None,
                 record_preprocessor_script = None,
                 post_analytics_processor_script = None, baseline_constraints = None,
                 baseline_statistics = None,
                 publish_cloudwatch_metrics = None, sagemaker_endpoint_name = None,
                 sagemaker_monitoring_schedule_name = None,
                 detect_outliers = None, detect_drift = None, image_data = None,
                 report_enabled = False, auto_ml_job_detail = None, monitoring_input_type = None,
                 data_quality_monitoring_config = None, exclude_features_attribute=None):
        self.dataset_source = dataset_source
        self.dataset_format = dataset_format
        self.output_path = output_path
        self.monitoring_input_type = monitoring_input_type
        self.analysis_type = analysis_type
        self.problem_type = problem_type
        self.inference_attribute = inference_attribute
        self.probability_attribute = probability_attribute
        self.ground_truth_attribute = ground_truth_attribute
        self.probability_threshold_attribute = probability_threshold_attribute
        self.positive_label = positive_label
        self.exclude_features_attribute = exclude_features_attribute
        self.record_preprocessor_script = record_preprocessor_script
        self.post_analytics_processor_script = post_analytics_processor_script
        self.baseline_constraints = baseline_constraints
        self.baseline_statistics = baseline_statistics
        self.data_quality_monitoring_config = data_quality_monitoring_config
        self.start_time = start_time
        self.end_time = end_time
        self.metric_time = metric_time
        self.cloudwatch_metrics_directory = cloudwatch_metrics_directory
        self.publish_cloudwatch_metrics = publish_cloudwatch_metrics
        self.sagemaker_endpoint_name = sagemaker_endpoint_name
        self.sagemaker_monitoring_schedule_name = sagemaker_monitoring_schedule_name
        self.output_message_file = output_message_file
        # Image analysis inputs
        self.detect_outliers = detect_outliers
        self.detect_drift = detect_drift
        self.image_data = image_data
        self.report_enabled = report_enabled
        self.auto_ml_job_detail = auto_ml_job_detail
    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__)
class DatasetFormat:
    def __init__(self, csv=None, json=None, captured=None, parquet=None):
        if csv is None and json is None and captured is None and parquet is None:
            raise ValueError('Dataset format is missing')
        self.csv = csv
        self.json = json
        self.captured = captured
        self.parquet = parquet
class CSV:
    default_header = False
    allowed_positions = ['START', 'END']
    default_record_delimiter = '\n'
    default_field_delimiter = ','
    default_output_columns_position = 'START'
    def __init__(self, header = None, output_columns_position = None, record_delimiter = None, field_delimiter = None, quote_character = None,
                 quote_escape_character = None):
        if output_columns_position is not None and output_columns_position not in self.allowed_positions:
            raise ValueError('Invalid value for output_columns_position')
        self.header = self.default_header if header is None else header
        self.output_columns_position = self.default_output_columns_position if output_columns_position is None else output_columns_position
        self.record_delimiter = self.default_record_delimiter if record_delimiter is None else record_delimiter
        self.field_delimiter = self.default_field_delimiter if field_delimiter is None else field_delimiter
        self.quote_character = quote_character
        self.quote_escape_character = quote_escape_character
class JSON:
    default_record_delimiter = '\n'
    def __init__(self, record_delimiter = None):
        self.record_delimiter = self.default_record_delimiter if record_delimiter is None else record_delimiter
class Captured:
    default_captured_index_names = ['endpointInput', 'endpointOutput']
    def __init__(self, captured_index_names = None):
        self.captured_index_names = self.default_captured_index_names if captured_index_names is None else captured_index_names
class DataQualityDistributionConstraints:
    """Class to define the distribution_constraints that is used to override the
    distribution_constraint object values in DataQualityMonitoringConfig object.
    """
    default_perform_comparison = 'Enabled'
    default_comparison_threshold = 0.1
    default_comparison_method = 'Robust'
    default_categorical_comparison_threshold = 0.1
    default_categorical_drift_method = 'LInfinity'
    def __init__(self, perform_comparison=None, comparison_threshold=None, comparison_method=None,
                 categorical_comparison_threshold=None, categorical_drift_method=None):
        self.perform_comparison = self.default_perform_comparison if perform_comparison is None else perform_comparison
        self.comparison_threshold = self.default_comparison_threshold if comparison_threshold is None else comparison_threshold
        self.comparison_method = self.default_comparison_method if comparison_method is None else comparison_method
        self.categorical_comparison_threshold = self.default_categorical_comparison_threshold if categorical_comparison_threshold is None else categorical_comparison_threshold
        self.categorical_drift_method = self.default_categorical_drift_method if categorical_drift_method is None else categorical_drift_method
class DataQualityMonitoringConfig:
    """Class to define the monitoring_config object that is used to override the default
    global monitoring_config values in constraints suggested by service.
    """
    default_evaluate_constraints = 'Enabled'
    default_emit_metrics = 'Enabled'
    default_datatype_check_threshold = 1.0
    default_domain_content_threshold = 1.0
    default_distribution_constraints = DataQualityDistributionConstraints()
    def __init__(self, evaluate_constraints=None, emit_metrics=None, datatype_check_threshold=None,
                 domain_content_threshold=None, distribution_constraints=None):
        self.evaluate_constraints = self.default_evaluate_constraints if evaluate_constraints is None else evaluate_constraints
        self.emit_metrics = self.default_emit_metrics if emit_metrics is None else emit_metrics
        self.datatype_check_threshold = self.default_datatype_check_threshold if datatype_check_threshold is None else datatype_check_threshold
        self.domain_content_threshold = self.default_domain_content_threshold if domain_content_threshold is None else domain_content_threshold
        self.distribution_constraints = self.default_distribution_constraints if distribution_constraints is None else distribution_constraints
