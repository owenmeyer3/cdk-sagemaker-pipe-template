import boto3, json, io

s3_client = boto3.client("s3")

def parse_s3_uri(s3_uri):
    # s3://bucket-name/path/to/key
    s3_uri = s3_uri.replace('s3://', '')
    bucket, key = s3_uri.split('/', 1)
    return bucket, key

def data_quality_handler(event, context):
    monitoring_type='DataQuality'

    return {'TYPE': 'DataQuality'}


def model_bias_handler(event, context):
    monitoring_type='ModelBias'

    return {'TYPE': 'ModelBias'}


def model_explainability_handler(event, context):
    monitoring_type='ModelExplainability'

    return {'TYPE': 'ModelExplainability'}


def model_quality_handler(event, context):
    monitoring_type='ModelQuality'

    return {'TYPE': 'ModelQuality'}

# """
# Lightweight, dependency-free port of the pieces of sagemaker.clarify used to
# build analysis_config.json. No import of the sagemaker SDK required — this is
# useful specifically for Lambda, where adding the full sagemaker package as a
# layer is unnecessary overhead for what is, under the hood, just conditional
# dict-key assignment.

# Ported from sagemaker.core.clarify.DataConfig / BiasConfig / SHAPConfig
# (confirmed via source: these classes make no AWS/network calls in their
# constructors — they only validate arguments and build a plain dict).
# """


# def _set(value, key, dictionary):
#     """Mirrors sagemaker.clarify._set: sets dictionary[key] = value if value is not None."""
#     if value is not None:
#         dictionary[key] = value


# def build_data_config(
#     label=None,
#     headers=None,
#     features=None,
#     dataset_type="text/csv",
#     joinsource=None,
#     facet_dataset_uri=None,
#     facet_headers=None,
#     predicted_label_dataset_uri=None,
#     predicted_label_headers=None,
#     predicted_label=None,
#     excluded_columns=None,
# ):
#     """
#     Equivalent to sagemaker.clarify.DataConfig(...).get_config().

#     NOTE: unlike DataConfig, this does NOT take s3_data_input_path / s3_output_path —
#     confirmed from source, DataConfig never writes those into analysis_config.json either
#     (no "dataset_uri" key is ever set). The dataset itself is supplied via a
#     ProcessingInput named "dataset", not through this config dict. See the two-input
#     ProcessingInputs form (InputName: "analysis_config" + InputName: "dataset").
#     """
#     config = {"dataset_type": dataset_type}
#     _set(features, "features", config)
#     _set(headers, "headers", config)
#     _set(label, "label", config)
#     _set(joinsource, "joinsource_name_or_index", config)
#     _set(facet_dataset_uri, "facet_dataset_uri", config)
#     _set(facet_headers, "facet_headers", config)
#     _set(predicted_label_dataset_uri, "predicted_label_dataset_uri", config)
#     _set(predicted_label_headers, "predicted_label_headers", config)
#     _set(predicted_label, "predicted_label", config)
#     _set(excluded_columns, "excluded_columns", config)
#     return config


# def build_bias_config(
#     label_values_or_threshold,
#     facet_name,
#     facet_values_or_threshold=None,
#     group_name=None,
# ):
#     """Equivalent to sagemaker.clarify.BiasConfig(...).get_config()."""
#     if isinstance(facet_name, list):
#         if not facet_name:
#             raise ValueError("Please provide at least one facet")
#         if facet_values_or_threshold is None:
#             facet_list = [{"name_or_index": name} for name in facet_name]
#         elif len(facet_values_or_threshold) == len(facet_name):
#             facet_list = []
#             for i, name in enumerate(facet_name):
#                 facet = {"name_or_index": name}
#                 _set(facet_values_or_threshold[i], "value_or_threshold", facet)
#                 facet_list.append(facet)
#         else:
#             raise ValueError("The number of facet names doesn't match the number of facet values")
#     else:
#         facet = {"name_or_index": facet_name}
#         _set(facet_values_or_threshold, "value_or_threshold", facet)
#         facet_list = [facet]

#     config = {
#         "label_values_or_threshold": label_values_or_threshold,
#         "facet": facet_list,
#     }
#     _set(group_name, "group_variable", config)
#     return config


# def build_shap_config(
#     baseline=None,
#     num_samples=None,
#     agg_method=None,
#     use_logit=False,
#     save_local_shap_values=True,
#     seed=None,
#     num_clusters=None,
#     features_to_explain=None,
# ):
#     """Equivalent to sagemaker.clarify.SHAPConfig(...).get_explainability_config()."""
#     if agg_method is not None and agg_method not in ("mean_abs", "median", "mean_sq"):
#         raise ValueError(f"Invalid agg_method {agg_method}. Please choose mean_abs, median, or mean_sq.")
#     if num_clusters is not None and baseline is not None:
#         raise ValueError("Baseline and num_clusters cannot be provided together. Please specify one of the two.")

#     shap_config = {
#         "use_logit": use_logit,
#         "save_local_shap_values": save_local_shap_values,
#     }
#     _set(baseline, "baseline", shap_config)
#     _set(num_samples, "num_samples", shap_config)
#     _set(agg_method, "agg_method", shap_config)
#     _set(seed, "seed", shap_config)
#     _set(num_clusters, "num_clusters", shap_config)
#     _set(features_to_explain, "features_to_explain", shap_config)

#     return {"methods": {"shap": shap_config}}


# def analysis_config_build_handler(event, context):
#     bl_config_file=event['bl_config_file']
#     mb_monitor_dir=event['mb_monitor_dir']
#     me_monitor_dir=event['me_monitor_dir']
#     agg_method=event['agg_method']

#     bucket, key = parse_s3_uri(bl_config_file)
#     bl_config = json.loads(s3_client.get_object(Bucket=bucket, Key=key)["Body"].read())

#     # Model Bias
#     analysis_config = {
#         **build_data_config(
#             label=bl_config["target_label"],
#             headers=bl_config["mb_headers"], 
#             features=None,
#             dataset_type="text/csv",
#             joinsource=None,
#             facet_dataset_uri=None,
#             facet_headers=None,
#             predicted_label_dataset_uri=None,
#             predicted_label_headers=None,
#             predicted_label=bl_config["predict_label"],
#             excluded_columns=None,
#         ),
#         **build_bias_config(
#             label_values_or_threshold=[1], 
#             facet_name="age",
#             facet_values_or_threshold=None,
#             group_name=None,
#         ),
#     }
#     bkt, key = parse_s3_uri(f"{mb_monitor_dir}/info/analysis_config.json")
#     s3_client.put_object(Bucket=bkt, Key=key, Body=json.dumps(analysis_config))

#     # Model Explainability example
#     analysis_config = {
#         **build_data_config(
#             label=None,
#             headers=bl_config["me_headers"], 
#             features=None,
#             dataset_type="text/csv",
#             joinsource=None,
#             facet_dataset_uri=None,
#             facet_headers=None,
#             predicted_label_dataset_uri=None,
#             predicted_label_headers=None,
#             predicted_label=bl_config["predict_label"],
#             excluded_columns=None,
#         ),
#         **build_shap_config(
#             baseline=bl_config["baseline"],
#             num_samples=None,
#             agg_method=agg_method,
#             use_logit=False,
#             save_local_shap_values=True,
#             seed=None,
#             num_clusters=None,
#             features_to_explain=None,
#         ),
#     }
#     bkt, key = parse_s3_uri(f"{me_monitor_dir}/info/analysis_config.json")
#     s3_client.put_object(Bucket=bkt, Key=key, Body=json.dumps(analysis_config))

#     return {
#         "MB_ANALYSIS_CONFIG_FILE": f"{mb_monitor_dir}/info/analysis_config.json",
#         "ME_ANALYSIS_CONFIG_FILE": f"{me_monitor_dir}/info/analysis_config.json"
#     }