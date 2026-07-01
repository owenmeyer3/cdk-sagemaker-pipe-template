from sagemaker.core.model_monitor import DefaultModelMonitor
from sagemaker.core.model_monitor.dataset_format import DatasetFormat
from sagemaker.core.helper.session_helper import Session
from sagemaker.core.model_monitor import ModelQualityMonitor
from sagemaker.core.model_monitor.clarify_model_monitoring import ModelBiasMonitor, ModelExplainabilityMonitor
from sagemaker.core.clarify import DataConfig as CfyDataConfig, BiasConfig as CfyBiasConfig, ModelConfig as CfyModelConfig, ModelPredictedLabelConfig as CfyModelPredictedLabelConfig, SHAPConfig as CfySHAPConfig
import pandas as pd 
from urllib.parse import urlparse
import datetime, boto3

def get_dataset_format(dataset_format):
    if 'csv' in list(dataset_format.keys()):
        return DatasetFormat.csv(header = dataset_format['csv']['header'], output_columns_position="START")
    elif 'json' in list(dataset_format.keys()):
        return DatasetFormat.json(lines = dataset_format['json']['lines'])
    else:
        return None

def load_csv_from_s3(s3_uri, header=None):
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')
    
    local_path = '/tmp/' + key.split('/')[-1]
    
    s3 = boto3.client('s3')
    s3.download_file(bucket, key, local_path)
    
    return pd.read_csv(local_path, header=header)

def create_dq_baseline_handler(event, context):
    role = event['role']
    baseline_dataset = event['baseline_dataset']
    output_s3_uri = event['output_s3_uri']
    dataset_format = event['dataset_format'] if 'dataset_format' in event else {'csv': {'header': True}}
    instance_count = event['instance_count'] if 'instance_count' in event else 1
    instance_type = event['instance_type'] if 'instance_type' in event else 'ml.m5.xlarge'
    volume_size_in_gb = event['volume_size_in_gb'] if 'volume_size_in_gb' in event else 20
    max_runtime_in_seconds = event['max_runtime_in_seconds'] if 'max_runtime_in_seconds' in event else 3600
    execution_id=event['execution_id']

    my_default_monitor = DefaultModelMonitor(
        role=role,
        instance_count=instance_count,
        instance_type=instance_type,
        volume_size_in_gb=volume_size_in_gb,
        max_runtime_in_seconds=max_runtime_in_seconds,
    )

    my_default_monitor.suggest_baseline(
        job_name=f"mq-baseline-{execution_id}",
        baseline_dataset=baseline_dataset,
        dataset_format=get_dataset_format(dataset_format),
        output_s3_uri=output_s3_uri,
        wait=True
    )
    print(f"ModelBiasMonitor baselining job: {my_default_monitor.latest_baselining_job_name}")


def create_mq_baseline_handler(event, context):
    role = event['role']
    baseline_dataset = event['baseline_dataset']
    output_s3_uri = event['output_s3_uri']
    problem_type = event['problem_type']
    inference_attribute = event['inference_attribute'] # The column in the dataset that contains predictions.
    probability_attribute = event['probability_attribute'] # The column in the dataset that contains probabilities.
    ground_truth_attribute = event['ground_truth_attribute'] # The column in the dataset that contains ground truth labels.
    execution_id=event['execution_id']
    dataset_format = event['dataset_format'] if 'dataset_format' in event else {'csv': {'header': True}}
    instance_count = event['instance_count'] if 'instance_count' in event else 1
    instance_type = event['instance_type'] if 'instance_type' in event else 'ml.m5.xlarge'
    volume_size_in_gb = event['volume_size_in_gb'] if 'volume_size_in_gb' in event else 20
    max_runtime_in_seconds = event['max_runtime_in_seconds'] if 'max_runtime_in_seconds' in event else 3600

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
        baseline_dataset=baseline_dataset, # The S3 location of the validation dataset.
        dataset_format=get_dataset_format(dataset_format),
        output_s3_uri = output_s3_uri, # The S3 location to store the results.
        problem_type=problem_type,
        inference_attribute= inference_attribute, # The column in the dataset that contains predictions.
        probability_attribute= probability_attribute, # The column in the dataset that contains probabilities.
        ground_truth_attribute= ground_truth_attribute, # The column in the dataset that contains ground truth labels.
        wait=True
    )
    print(f"ModelBiasMonitor baselining job: {model_quality_monitor.latest_baselining_job_name}")



def create_mb_baseline_handler(event, context):
    role = event['role']
    model_name = event['model_name']
    baseline_dataset = event['baseline_dataset']
    output_s3_uri = event['output_s3_uri']
    label = event['label']
    bias_config = event['bias_config'] if 'bias_config' in event else None # {'label_values_or_threshold':[1], 'function':"Account Length", 'facet_values_or_threshold':[100]}
    model_predicted_label_config = event['model_predicted_label_config'] if 'model_predicted_label_config' in event else None # {'probability_threshold':0.8}
    content_type = event['content_type'] if 'content_type' in event else 'text/csv'
    instance_count = event['instance_count'] if 'instance_count' in event else 1
    instance_type = event['instance_type'] if 'instance_type' in event else 'ml.m5.xlarge'
    max_runtime_in_seconds = event['max_runtime_in_seconds'] if 'max_runtime_in_seconds' in event else 3600

    model_bias_monitor = ModelBiasMonitor(
        role=role,
        sagemaker_session=Session(),
        max_runtime_in_seconds=max_runtime_in_seconds,
    )
    
    model_bias_data_config = CfyDataConfig(
        s3_data_input_path=baseline_dataset,
        s3_output_path=output_s3_uri,
        label=label,
        headers=None,
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
        model_config=model_config_obj,
        data_config=model_bias_data_config,
        bias_config=bias_config_obj,
        model_predicted_label_config=model_predicted_label_config_obj,
        wait=True
    )
    print(f"ModelBiasMonitor baselining job: {model_bias_monitor.latest_baselining_job_name}")


def create_me_baseline_handler(event, context):
    role = event['role']
    model_name = event['model_name']
    baseline_dataset = event['baseline_dataset']
    output_s3_uri = event['output_s3_uri']
    label = event['label']
    baseline_cols = event['baseline_cols']
    test_X_dataset = event['test_X_dataset']
    num_samples = event['num_samples'] if 'num_samples' in event else 100
    agg_method = event['agg_method'] if 'agg_method' in event else 'mean_sq' # "mean_abs", "median", "mean_sq"
    content_type = event['content_type'] if 'content_type' in event else 'text/csv'
    instance_count = event['instance_count'] if 'instance_count' in event else 1
    instance_type = event['instance_type'] if 'instance_type' in event else 'ml.m5.xlarge'
    max_runtime_in_seconds = event['max_runtime_in_seconds'] if 'max_runtime_in_seconds' in event else 3600
    dataset_format = event['dataset_format'] if 'dataset_format' in event else {'csv': {'header': True}}

    model_explainability_monitor = ModelExplainabilityMonitor(
        role=role,
        sagemaker_session=Session(),
        max_runtime_in_seconds=max_runtime_in_seconds,
    )
    features = baseline_cols.remove(label)
    model_explainability_data_config = CfyDataConfig(
        s3_data_input_path=baseline_dataset,
        s3_output_path=output_s3_uri,
        label=label,
        headers=None,
        features=features,
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
    test_X_dataframe = load_csv_from_s3(test_X_dataset, header=None)
    shap_baseline = [list(test_X_dataframe.mean())]

    shap_config = CfySHAPConfig(
        baseline=shap_baseline,
        num_samples=num_samples,
        agg_method=agg_method,
        save_local_shap_values=False,
    )

    model_explainability_monitor.suggest_baseline(
        data_config=model_explainability_data_config,
        model_config=model_config_obj,
        explainability_config=shap_config,
        wait=True
    )
    print(f"ModelExplainabilityMonitor baselining job: {model_explainability_monitor.latest_baselining_job_name}")