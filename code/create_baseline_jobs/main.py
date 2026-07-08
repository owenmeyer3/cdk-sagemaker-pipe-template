from sagemaker.core.model_monitor import DefaultModelMonitor
from sagemaker.core.model_monitor.dataset_format import DatasetFormat
from sagemaker.core.helper.session_helper import Session
from sagemaker.core.model_monitor import ModelQualityMonitor
from sagemaker.core.model_monitor.clarify_model_monitoring import ModelBiasMonitor, ModelExplainabilityMonitor
from sagemaker.core.clarify import DataConfig as CfyDataConfig, BiasConfig as CfyBiasConfig, ModelConfig as CfyModelConfig, ModelPredictedLabelConfig as CfyModelPredictedLabelConfig, SHAPConfig as CfySHAPConfig
import pandas as pd 
from urllib.parse import urlparse
import boto3, os, json, sys, io

stepfunctions = boto3.client('stepfunctions')
s3_client = boto3.client('s3')

def report_success(stepfunctions, state):
    stepfunctions.send_task_success(taskToken=os.getenv('TASK_TOKEN'))
    sys.exit(0)

def report_failure(stepfunctions, error_message):
    print(f'MANUAL ERROR: {error_message}')
    stepfunctions.send_task_failure(taskToken=os.getenv('TASK_TOKEN'), error="ECSFailure", cause=error_message)

def get_dataset_format(dataset_format):
    if 'csv' in list(dataset_format.keys()):
        return DatasetFormat.csv(header = dataset_format['csv']['header'], output_columns_position="START")
    elif 'json' in list(dataset_format.keys()):
        return DatasetFormat.json(lines = dataset_format['json']['lines'])
    else:
        return None

def get_bkt_key(s3_uri):
    parsed = urlparse(s3_uri)
    return [parsed.netloc, parsed.path.lstrip('/')]

def load_csv_from_s3(s3_uri, header=None):
    bucket, key = get_bkt_key(s3_uri)
    s3_client.download_file(bucket, key, '/tmp/tmp.csv')
    return pd.read_csv('/tmp/tmp.csv', header=header)

def write_csv_to_s3(df, s3_uri, index=False, header=None, drop_cols=[]):
    bucket, key = get_bkt_key(s3_uri)
    if drop_cols: df = df.drop(columns=drop_cols)
    buffer = io.StringIO()
    df.to_csv(buffer, index=index, header=header)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())


def parse_csv_and_headers_s3(s3_uri, sfx='out', drop_cols=[], target_to_shift=None):

    df = load_csv_from_s3(s3_uri, header=0)
    if target_to_shift: df = df[[target_to_shift] + [c for c in df.columns if c != target_to_shift]]
    if drop_cols: df = df.drop(columns=drop_cols)
    headers = list(df.columns)

    s3_uri_out=s3_uri.split('.')[0] + '_' + sfx + '.' + s3_uri.split('.')[1]
    write_csv_to_s3(df, s3_uri_out, index=False, header=False)

    return [s3_uri_out, headers]


def create_dq_baseline_handler(            
    role,
    baseline_full_dataset, # [features] must be compliant with Spark. For column names, use only lowercase characters, and _ as the only special character
    output_s3_uri,
    target_label,
    predict_label,
    dataset_format, # include headers
    instance_count,
    instance_type,
    volume_size_in_gb,
    max_runtime_in_seconds,
    execution_id
):
    baseline_df=load_csv_from_s3(baseline_full_dataset, header=0)
    baseline_df = baseline_df.drops(columns=[target_label, predict_label])
    baseline_df.to_csv('/tmp/dq.csv', index=False, header=True)

    my_default_monitor = DefaultModelMonitor(
        role=role,
        instance_count=instance_count,
        instance_type=instance_type,
        volume_size_in_gb=volume_size_in_gb,
        max_runtime_in_seconds=max_runtime_in_seconds,
    )

    my_default_monitor.suggest_baseline(
        job_name=f"dq-baseline-{execution_id}",
        baseline_dataset='/tmp/dq.csv',
        dataset_format=get_dataset_format(dataset_format),
        output_s3_uri=output_s3_uri,
        wait=True
    )
    print(f"ModelBiasMonitor baselining job: {my_default_monitor.latest_baselining_job_name}")
    return my_default_monitor.latest_baselining_job_name


def create_mq_baseline_handler(
    role,
    baseline_full_dataset, # target + pred
    output_s3_uri,
    target_label,
    predict_label,
    problem_type,
    dataset_format,
    instance_count,
    instance_type,
    volume_size_in_gb,
    max_runtime_in_seconds,
    probability_attribute, # Classification Only,
    probability_threshold_attribute,  # Classification Only
    execution_id,
):
    baseline_df=load_csv_from_s3(baseline_full_dataset, header=0)
    baseline_df = baseline_df[[target_label, predict_label]]
    baseline_df.to_csv('/tmp/mq.csv', index=False, header=True)

    model_quality_monitor = ModelQualityMonitor(
        role=role,
        instance_count=instance_count,
        instance_type=instance_type,
        volume_size_in_gb=volume_size_in_gb,
        max_runtime_in_seconds=max_runtime_in_seconds,
        sagemaker_session=Session()
    )    
    model_quality_monitor.suggest_baseline(
        job_name=f"mq-baseline-{execution_id}",
        baseline_dataset='/tmp/mq.csv', # The location of the validation dataset.
        dataset_format=get_dataset_format(dataset_format),
        output_s3_uri = output_s3_uri, # The S3 location to store the results.
        problem_type=problem_type,
        inference_attribute= predict_label, # The column in the dataset that contains predictions.
        ground_truth_attribute= target_label, # The column in the dataset that contains ground truth labels.
        probability_attribute= probability_attribute, # The column in the dataset that contains probabilities.
        probability_threshold_attribute=probability_threshold_attribute,

        wait=True
    )
    print(f"ModelBiasMonitor baselining job: {model_quality_monitor.latest_baselining_job_name}")
    return model_quality_monitor.latest_baselining_job_name


def create_mb_baseline_handler(
    role,
    model_name,
    baseline_full_dataset, # pred + target + [features]
    output_s3_uri,
    target_label,
    predict_label,
    bias_config,
    model_predicted_label_config,
    content_type,
    instance_count,
    instance_type,
    max_runtime_in_seconds,
    execution_id
):
    s3_data_input_path, headers = parse_csv_and_headers_s3(baseline_full_dataset, target_to_shift=target_label)

    model_bias_monitor = ModelBiasMonitor(
        role=role,
        sagemaker_session=Session(),
        max_runtime_in_seconds=max_runtime_in_seconds,
    )

    model_bias_data_config = CfyDataConfig(
        s3_data_input_path=s3_data_input_path,
        s3_output_path=output_s3_uri,
        headers=headers,
        label=target_label,
        predicted_label=predict_label,
        features=",".join([c for c in headers if c != target_label and c != predict_label]),
        dataset_type=content_type,
    )

    if bias_config:
        bias_config_obj = CfyBiasConfig(
            label_values_or_threshold=bias_config['label_values_or_threshold'],
            facet_name=bias_config['function'],
            facet_values_or_threshold=bias_config['facet_values_or_threshold'],
        )
    else:
        bias_config_obj=None

    if model_predicted_label_config:
        model_predicted_label_config_obj = CfyModelPredictedLabelConfig(
            probability_threshold=model_predicted_label_config['probability_threshold'],
        )
    else:
        model_predicted_label_config_obj=None

    model_config_obj = CfyModelConfig(
        model_name=model_name,
        instance_count=instance_count,
        instance_type=instance_type,
        content_type=content_type,
        accept_type=content_type,
    )

    model_bias_monitor.suggest_baseline(
        job_name=f"mb-baseline-{execution_id}",
        model_config=model_config_obj,
        data_config=model_bias_data_config,
        bias_config=bias_config_obj,
        model_predicted_label_config=model_predicted_label_config_obj,
        wait=True
    )
    print(f"ModelBiasMonitor baselining job: {model_bias_monitor.latest_baselining_job_name}")
    return model_bias_monitor.latest_baselining_job_name


def create_me_baseline_handler(
    role,
    model_name,
    baseline_full_dataset,  # pred + [features]
    output_s3_uri,
    target_label,
    predict_label,
    num_samples,
    agg_method,
    content_type,
    instance_count,
    instance_type,
    max_runtime_in_seconds,
    execution_id
):
    model_explainability_monitor = ModelExplainabilityMonitor(
        role=role,
        sagemaker_session=Session(),
        max_runtime_in_seconds=max_runtime_in_seconds,
    )

    s3_data_input_path, headers = parse_csv_and_headers_s3(baseline_full_dataset, target_to_shift=target_label)
    model_explainability_data_config = CfyDataConfig(
        s3_data_input_path=s3_data_input_path,
        s3_output_path=output_s3_uri, 
        headers=headers,
        label=target_label,
        predicted_label=predict_label,
        features=",".join([c for c in headers if c != target_label and c != predict_label]), 
        dataset_type=content_type,
    )

    model_config_obj = CfyModelConfig(
        model_name=model_name,
        instance_count=instance_count,
        instance_type=instance_type,
        content_type=content_type,
        accept_type=content_type,
    )

    # Here use the mean value of test dataset as SHAP baseline
    baseline_X_dataframe=load_csv_from_s3(baseline_full_dataset, header=0)
    baseline_X_dataframe = baseline_X_dataframe.drop(columns=[target_label, predict_label])
    shap_baseline = [list(baseline_X_dataframe.mean())]

    shap_config = CfySHAPConfig(
        baseline=shap_baseline,
        num_samples=num_samples,
        agg_method=agg_method,
        save_local_shap_values=False,
    )

    model_explainability_monitor.suggest_baseline(
        job_name=f"me-baseline-{execution_id}",
        data_config=model_explainability_data_config,
        model_config=model_config_obj,
        explainability_config=shap_config,
        wait=True
    )
    print(f"ModelExplainabilityMonitor baselining job: {model_explainability_monitor.latest_baselining_job_name}")
    return model_explainability_monitor.latest_baselining_job_name


if __name__ == '__main__':
    print('START')

    state={}

    monitor_type = os.getenv('monitor_type')
    print('monitor_type ' + monitor_type)

    role = os.getenv('role')
    baseline_full_dataset = os.getenv('baseline_full_dataset', '')
    output_s3_uri = os.getenv('output_s3_uri', '')
    dataset_format = json.loads(os.getenv('dataset_format', '{"csv": {"header": true}}'))
    instance_count = int(os.getenv('instance_count', '1'))
    instance_type = os.getenv('instance_type', 'ml.m5.xlarge')
    volume_size_in_gb = int(os.getenv('volume_size_in_gb', '20'))
    max_runtime_in_seconds = int(os.getenv('max_runtime_in_seconds', '3600'))
    execution_id = os.getenv('execution_id')
    problem_type = os.getenv('problem_type', '')
    predict_label = os.getenv('predict_label', '') # The column in the dataset that contains predictions.
    target_label = os.getenv('target_label', '') # The column in the dataset that contains ground truth labels.
    model_name = os.getenv('model_name', '')
    bias_config = json.loads(os.getenv('bias_config','{}')) or None # {'label_values_or_threshold':[1], 'function':"Account Length", 'facet_values_or_threshold':[100]}
    model_predicted_label_config = json.loads(os.getenv('model_predicted_label_config','{}')) or None # {'probability_threshold':0.8}
    content_type = os.getenv('content_type', 'text/csv')
    num_samples = int(os.getenv('num_samples', '100'))
    agg_method = os.getenv('agg_method', 'mean_sq') # "mean_abs", "median", "mean_sq"
    probability_attribute = json.loads(os.getenv('probability_attribute','{}')) or None # The column in the dataset that contains probabilities.
    probability_threshold_attribute=json.loads(os.getenv('probability_threshold_attribute','{}')) or None
    
    if monitor_type == 'DataQuality':
        baselining_job_name = create_dq_baseline_handler(
            role,
            baseline_full_dataset, # [features] must be compliant with Spark. For column names, use only lowercase characters, and _ as the only special character
            output_s3_uri,
            target_label,
            predict_label,
            dataset_format, # include headers
            instance_count,
            instance_type,
            volume_size_in_gb,
            max_runtime_in_seconds,
            execution_id
        )
    elif monitor_type == 'ModelQuality':
        baselining_job_name = create_mq_baseline_handler(
            role,
            baseline_full_dataset, # target + pred
            output_s3_uri,
            target_label,
            predict_label,
            problem_type,
            dataset_format,
            instance_count,
            instance_type,
            volume_size_in_gb,
            max_runtime_in_seconds,
            probability_attribute, # Classification Only,
            probability_threshold_attribute,  # Classification Only
            execution_id,
        )
    elif monitor_type == 'ModelBias':
        baselining_job_name = create_mb_baseline_handler(
            role,
            model_name,
            baseline_full_dataset, # pred + target + [features]
            output_s3_uri,
            target_label,
            predict_label,
            bias_config,
            model_predicted_label_config,
            content_type,
            instance_count,
            instance_type,
            max_runtime_in_seconds,
            execution_id
        )
    elif monitor_type == 'ModelExplainability':
        baselining_job_name = create_me_baseline_handler(
            role,
            model_name,
            baseline_full_dataset,  # pred + [features]
            output_s3_uri,
            target_label,
            predict_label,
            num_samples,
            agg_method,
            content_type,
            instance_count,
            instance_type,
            max_runtime_in_seconds,
            execution_id
        )
    else:
        baselining_job_name = 'None'

    state['BASELINING_JOB_NAME'] = baselining_job_name
    report_success(stepfunctions, state)