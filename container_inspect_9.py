#!/usr/bin/env python3

# response = sm_client.create_processing_job(
#     ProcessingJobName='inspect-model-monitor-container-9',
#     AppSpecification={
#         'ImageUri': '156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer',
#         'ContainerEntrypoint': ['/bin/bash', '-c'],
#         'ContainerArguments': [
#             'cat /opt/program/analyze'
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
import logging
import os
import sys
from analysis.analytics_input import AnalyticsInput, DataQualityDistributionConstraints, DataQualityMonitoringConfig
from analysis.report import AutoMlJobDetail, ReportEnvParam
from analysis.default_data_analyzer import DefaultDataAnalyzer
from analysis.image_data_analyzer import ImageDataAnalyzer
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)
OUTPUT_MESSAGE_FILE = '/opt/ml/output/message'
# This is the location where AnalyticsJob writes the whole of CreateAnalyticsJob input
# we will pull the Environment map from there
ANALYTICS_JOB_CONFIG_FILE = '/opt/ml/config/processingjobconfig.json'
def get_environment_params():
    with open(ANALYTICS_JOB_CONFIG_FILE, 'r') as ajc:
        params = json.load(ajc)
        log.info('All params:{}'.format(params))
        environment = params["Environment"]
        log.info('Current Environment:{}'.format(environment))
        return environment
def generate_analytics_input(environment):
    dataset_source = environment["dataset_source"]
    output_path = environment["output_path"]
    dataset_format = json.loads(environment["dataset_format"])
    cloudwatch_metrics_directory = environment.get('CLOUDWATCH_METRICS_DIRECTORY', "/opt/ml/output/metrics/cloudwatch")
    start_time = environment.get("start_time")
    end_time = environment.get("end_time")
    metric_time = environment.get("metric_time")
    analysis_type = environment.get("analysis_type")
    problem_type = environment.get("problem_type")
    # automl_problem_name is added to display the name of the model trained on the report. problem_type values can
    # only be BinaryClassificaiton/MulticlassClassification/Regression, so in order to display ImageClassification/
    # TextClassification the automl_problem_name env variable will be used to pass the problem name.
    automl_problem_name = environment.get("automl_problem_name")
    inference_attribute = environment.get("inference_attribute")
    probability_attribute = environment.get("probability_attribute")
    ground_truth_attribute = environment.get("ground_truth_attribute")
    probability_threshold_attribute = environment.get("probability_threshold_attribute")
    positive_label = environment.get("positive_label")
    exclude_features_attribute = environment.get("exclude_features_attribute")
    baseline_constraints = environment.get("baseline_constraints")
    baseline_statistics = environment.get("baseline_statistics")
    publish_cloudwatch_metrics = environment.get("publish_cloudwatch_metrics")
    record_preprocessor_script = environment.get("record_preprocessor_script")
    post_analytics_processor_script = environment.get("post_analytics_processor_script")
    sagemaker_endpoint_name = environment.get("sagemaker_endpoint_name")
    sagemaker_monitoring_schedule_name = environment.get("sagemaker_monitoring_schedule_name")
    categorical_drift_method = environment.get("categorical_drift_method")
    log.info('categorical_drift_method:{}'.format(categorical_drift_method))
    # Global monitoring_config used by customer to override the default global monitoring_config
    # values in constraints file suggested by service.
    data_quality_monitoring_config = DataQualityMonitoringConfig(
        distribution_constraints=DataQualityDistributionConstraints(
            categorical_drift_method=categorical_drift_method
        )
    )
    # Image data analysis environment variables
    detect_outliers = environment.get("detect_outliers")
    detect_drift = environment.get("detect_drift")
    image_data = environment.get("image_data")
    # To ensure backward compatibility
    monitoring_input_type = environment.get("monitoring_input_type", "ENDPOINT_INPUT") \
        if sagemaker_monitoring_schedule_name else None
    report_enabled = bool(environment.get("report_enabled"))
    auto_ml_job_detail = None
    if report_enabled:
        auto_ml_job_detail = AutoMlJobDetail(
            candidate_name=environment.get(ReportEnvParam.AUTO_ML_CANDIDATE_NAME.value),
            job_name=environment.get(ReportEnvParam.AUTO_ML_JOB_NAME.value),
            objective_metric_name=environment.get(ReportEnvParam.AUTO_ML_OBJECTIVE_METRIC_NAME.value),
            objective_type=environment.get(ReportEnvParam.AUTO_ML_JOB_OBJECTIVE_TYPE.value),
            problem_type=problem_type,
            automl_problem_name=automl_problem_name,
        )
    return AnalyticsInput(dataset_source, dataset_format, output_path,
                          analysis_type, problem_type,
                          inference_attribute, probability_attribute, ground_truth_attribute,
                          probability_threshold_attribute,
                          positive_label, cloudwatch_metrics_directory, OUTPUT_MESSAGE_FILE,
                          start_time, end_time, metric_time,
                          record_preprocessor_script,
                          post_analytics_processor_script,
                          baseline_constraints, baseline_statistics,
                          publish_cloudwatch_metrics, sagemaker_endpoint_name, sagemaker_monitoring_schedule_name,
                          detect_outliers, detect_drift, image_data, report_enabled, auto_ml_job_detail,
                          monitoring_input_type, data_quality_monitoring_config, exclude_features_attribute)
ANALYTICS_INPUT_ENVIRONMENT_FIELDS = {
    "dataset_source", "output_path", "dataset_format" "cloudwatch_metrics_directory", "start_time",
    "end_time", "metric_time", "analysis_type", "problem_type", "automl_problem_name", "inference_attribute",
    "probability_attribute", "ground_truth_attribute", "probability_threshold_attribute", "positive_label",
    "baseline_constraints", "baseline_statistics", "publish_cloudwatch_metrics", "record_preprocessor_script",
    "post_analytics_processor_script", "sagemaker_endpoint_name", "sagemaker_monitoring_schedule_name",
    "detect_outliers", "detect_drift", "image_data", "monitoring_input_type", "report_enabled",
    "categorical_drift_method", ReportEnvParam.AUTO_ML_CANDIDATE_NAME.value, ReportEnvParam.AUTO_ML_JOB_NAME.value,
    ReportEnvParam.AUTO_ML_OBJECTIVE_METRIC_NAME.value, ReportEnvParam.AUTO_ML_JOB_OBJECTIVE_TYPE.value,
}
# We set the processing environment variables as shell environment variables to allow
# pre and post processing scripts to access them. A few considerations went into this
# approach.
#
# 1. This is the only common place where we can set the environment variables so that they are
#    accessible to both the pre and post processing script.
# 2. While we don't need to filter these as they are any way available to the user via the processing
#    job details we still only let user defined environment variables through as the limit on an
#    environment block size is 32767 characters.
def set_environment_variables(environment):
    for k, v in environment.items():
        if k and v and k not in ANALYTICS_INPUT_ENVIRONMENT_FIELDS:
            os.environ[k] = v
if __name__ == "__main__":
    try:
        environment_params = get_environment_params()
        analytics_input = generate_analytics_input(environment_params)
        set_environment_variables(environment_params)
        if analytics_input.image_data:
            MODELS_FILE_PATH = "/opt/ml/models/drift_detector"
            image_analyzer = ImageDataAnalyzer()
            image_analyzer.perform_analysis(analytics_input, MODELS_FILE_PATH)
        else:
            default_analyzer = DefaultDataAnalyzer()
            default_analyzer.perform_analysis(analytics_input)
    except Exception as e:
        # Printing this causes the exception to be in the ProcessingJob logs, as well.
        log.error("Exception performing analysis: " + str(e))
        # Write out an error file. This will be returned as the failureReason in the
        # DescribeAnalyticsJob result.
        # if message file not exists, write error messages to it.
        if not os.path.exists(OUTPUT_MESSAGE_FILE):
            with open(os.path.join(OUTPUT_MESSAGE_FILE), 'w+') as s:
                s.write(str(e))
        # A non-zero exit code causes the analytics job to be marked as Failed.
        # TODO: Should we consider different exit codes for different failure modes?
        sys.exit(255)
