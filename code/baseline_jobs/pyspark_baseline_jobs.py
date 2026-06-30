# spark_image='173754725891.dkr.ecr.us-east-1.amazonaws.com/sagemaker-spark-processing:3.5-cpu-py312-v1.4'

def create_model_explainability_baseline_job(
    sm_client,
    name,
    role_arn,
    monitor_dir,
    baseline_dataset_uri,
    image_uri="173754725891.dkr.ecr.us-east-1.amazonaws.com/sagemaker-spark-processing:3.5-cpu-py312-v1.4",
    instance_count=1,
    instance_type='ml.m5.large',
    volume_size_in_gb=20,
    max_runtime_in_seconds=1800,
    dataset_format={'csv': {'header': True}},
    inference_attribute=None,        # column name of model prediction in dataset
    probability_attribute=None,      # column name of prediction probability (classification only)
    exclude_features_attribute=None, # comma separated list of features to exclude
):
    environment = {
        'dataset_source': baseline_dataset_uri,
        'output_path': f'{monitor_dir}/info',
        'dataset_format': json.dumps(dataset_format),
        'analysis_type': 'EXPLAINABILITY',
        'publish_cloudwatch_metrics': 'Disabled'
    }
    if inference_attribute: environment['inference_attribute'] = inference_attribute
    if probability_attribute: environment['probability_attribute'] = probability_attribute
    if exclude_features_attribute: environment['exclude_features_attribute'] = exclude_features_attribute

    response = sm_client.create_processing_job(
        ProcessingJobName=name,
        ProcessingInputs=[{
            'InputName': 'input',
            'S3Input': {
                'S3Uri': monitor_dir+'/baseline.csv',
                'LocalPath': '/opt/ml/processing/sm_input',
                'S3DataType': 'S3Prefix',
                'S3InputMode': 'File'
            }
        }],
        ProcessingResources={
            'ClusterConfig': {
                'InstanceCount': instance_count,
                'InstanceType': instance_type,
                'VolumeSizeInGB': volume_size_in_gb
            }
        },
        AppSpecification={
            'ImageUri': image_uri,
            'ContainerEntrypoint':[]
        },
        Environment=environment,
        RoleArn=role_arn,
        StoppingCondition={
            'MaxRuntimeInSeconds': max_runtime_in_seconds
        }
    )
    return response

if __name__ == '__main__':
    response=create_model_explainability_baseline_job(
        sm_client,
        name,
        role_arn,
        monitor_dir,
        baseline_dataset_uri,
        image_uri="173754725891.dkr.ecr.us-east-1.amazonaws.com/sagemaker-spark-processing:3.5-cpu-py312-v1.4",
        instance_count=1,
        instance_type='ml.m5.large',
        volume_size_in_gb=20,
        max_runtime_in_seconds=1800,
        dataset_format={'csv': {'header': True}},
        inference_attribute=None,        # column name of model prediction in dataset
        probability_attribute=None,      # column name of prediction probability (classification only)
        exclude_features_attribute=None, # comma separated list of features to exclude
    )

from sagemaker.core.model_monitor import DefaultModelMonitor
from sagemaker.core.model_monitor.dataset_format import DatasetFormat
from sagemaker.core.helper.session_helper import Session
from sagemaker.model_monitor import ModelQualityMonitor
from sagemaker.model_monitor.clarify_model_monitoring import ModelBiasMonitor, ModelExplainabilityMonitor
from sagemaker.clarify import DataConfig as CfyDataConfig, BiasConfig as CfyBiasConfig, ModelConfig as CfyModelConfig, ModelPredictedLabelConfig as CfyModelPredictedLabelConfig, SHAPConfig as CfySHAPConfig
import pandas as pd 

def get_dataset_format(dataset_format):
    if 'csv' in list(dataset_format.keys()):
        return DatasetFormat.csv(header = dataset_format['csv']['header'], output_columns_position="START")
    elif 'json' in list(dataset_format.keys()):
        return DatasetFormat.json(lines = dataset_format['json']['lines'])
    else:
        return None

def create_dq_baseline(
        role, 
        baseline_dataset, 
        output_s3_uri, 
        dataset_format={'csv': {'header': True}},
        instance_count=1,
        instance_type='ml.m5.xlarge',
        volume_size_in_gb=20,
        max_runtime_in_seconds=3600,
):
    my_default_monitor = DefaultModelMonitor(
        role=role,
        instance_count=instance_count,
        instance_type=instance_type,
        volume_size_in_gb=volume_size_in_gb,
        max_runtime_in_seconds=max_runtime_in_seconds,
    )

    my_default_monitor.suggest_baseline(
        baseline_dataset=baseline_dataset,
        dataset_format=get_dataset_format(dataset_format),
        output_s3_uri=output_s3_uri,
        wait=True
    )


def create_mq_baseline(
    role, 
    baseline_dataset, 
    output_s3_uri, 
    problem_type,
    inference_attribute, # The column in the dataset that contains predictions.
    probability_attribute, # The column in the dataset that contains probabilities.
    ground_truth_attribute, # The column in the dataset that contains ground truth labels.
    dataset_format={'csv': {'header': True}},
    instance_count=1,
    instance_type='ml.m5.xlarge',
    volume_size_in_gb=20,
    max_runtime_in_seconds=3600,
):
    model_quality_monitor = ModelQualityMonitor(
        role=role,
        instance_count=instance_count,
        instance_type=instance_type,
        volume_size_in_gb=volume_size_in_gb,
        max_runtime_in_seconds=max_runtime_in_seconds,
        sagemaker_session=Session()
    )
    baseline_job_name = "MyBaseLineJob"
    job = model_quality_monitor.suggest_baseline(
        job_name=baseline_job_name,
        baseline_dataset=baseline_dataset, # The S3 location of the validation dataset.
        dataset_format=get_dataset_format(dataset_format),
        output_s3_uri = output_s3_uri, # The S3 location to store the results.
        problem_type=problem_type,
        inference_attribute= inference_attribute, # The column in the dataset that contains predictions.
        probability_attribute= probability_attribute, # The column in the dataset that contains probabilities.
        ground_truth_attribute= ground_truth_attribute, # The column in the dataset that contains ground truth labels.
        wait=True
    )
    # job.wait(logs=False)



def create_mb_baseline(
    role,
    model_name,
    baseline_dataset, 
    output_s3_uri,
    label,
    headers,
    content_type='text/csv',
    instance_count=1,
    instance_type='ml.m5.xlarge',
    max_runtime_in_seconds=3600,
    bias_config=None, #{'label_values_or_threshold':[1], 'function':"Account Length", 'facet_values_or_threshold':[100]}
    model_predicted_label_config=None # {'probability_threshold':0.8}
    ):
    model_bias_monitor = ModelBiasMonitor(
        role=role,
        sagemaker_session=Session(),
        max_runtime_in_seconds=max_runtime_in_seconds,
    )
    
    model_bias_data_config = CfyDataConfig(
        s3_data_input_path=baseline_dataset,
        s3_output_path=output_s3_uri,
        label=label,
        headers=headers,
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
    )
    print(f"ModelBiasMonitor baselining job: {model_bias_monitor.latest_baselining_job_name}")


def create_me_baseline(
    role, 
    model_name,
    baseline_dataset, 
    output_s3_uri, 
    label,
    headers,
    test_X_dataset,
    num_samples=100,
    agg_method="mean_abs",
    content_type='text/csv',
    instance_count=1,
    instance_type='ml.m5.xlarge',
    max_runtime_in_seconds=3600,
    dataset_format={'csv': {'header': True}}
    ):
    model_explainability_monitor = ModelExplainabilityMonitor(
        role=role,
        sagemaker_session=Session(),
        max_runtime_in_seconds=max_runtime_in_seconds,
    )

    model_explainability_data_config = CfyDataConfig(
        s3_data_input_path=baseline_dataset,
        s3_output_path=output_s3_uri,
        label=label,
        headers=headers,
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
    test_X_dataframe = pd.read_csv(test_X_dataset, header=None)
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
    )
    print(f"ModelExplainabilityMonitor baselining job: {model_explainability_monitor.latest_baselining_job_name}")



