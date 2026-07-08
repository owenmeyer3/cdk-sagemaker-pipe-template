from sagemaker.core.model_monitor import DefaultModelMonitor
from sagemaker.core.model_monitor.dataset_format import DatasetFormat
from sagemaker.core.helper.session_helper import Session
from sagemaker.core.model_monitor import ModelQualityMonitor
from sagemaker.core.model_monitor.clarify_model_monitoring import ModelBiasMonitor, ModelExplainabilityMonitor
from sagemaker.core.clarify import DataConfig as CfyDataConfig, BiasConfig as CfyBiasConfig, ModelConfig as CfyModelConfig, ModelPredictedLabelConfig as CfyModelPredictedLabelConfig, SHAPConfig as CfySHAPConfig
import pandas as pd 
from urllib.parse import urlparse
import datetime, boto3, json

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