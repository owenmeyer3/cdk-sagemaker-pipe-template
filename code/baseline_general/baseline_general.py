import boto3, logging, io, json
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
sm_client = boto3.client('sagemaker')

def string_to_type(s):
    if s == 'float':
        return float
    elif s == 'int':
        return int
    elif s == 'str':
        return str
    else:
        return None

def parse_s3_uri(s3_uri):
    # s3://bucket-name/path/to/key
    s3_uri = s3_uri.replace('s3://', '')
    bucket, key = s3_uri.split('/', 1)
    return bucket, key

def df_from_s3(s3_uri, header=0, names=None):
    bucket, key = parse_s3_uri(s3_uri)
    obj = s3_client.get_object(Bucket=bucket, Key=key)

    if names:
        return pd.read_csv(io.BytesIO(obj['Body'].read()), header=header, names=names)
    else:
        return pd.read_csv(io.BytesIO(obj['Body'].read()), header=header)

def df_to_s3_csv(df, s3_uri, index=False, header=True):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=index, header=header)

    bucket, key = parse_s3_uri(s3_uri)
    s3_client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())

def df_to_s3_json(data, s3_uri):
    bucket, key = parse_s3_uri(s3_uri)
    s3_client.put_object(Bucket=bucket, Key=key, Body=json.dumps(data), ContentType="application/json",)

def get_only_file_in_dir(s3_uri):
    bucket, key = parse_s3_uri(s3_uri)
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)
    contents = response.get("Contents", [])

    # filter out "folder marker" objects (keys ending in / with zero size)
    files = [obj for obj in contents if not obj["Key"].endswith("/")]

    if len(files) == 0:
        raise FileNotFoundError(f"No files found under s3://{bucket}/{key}")
    if len(files) > 1:
        raise ValueError(f"Expected exactly one file under s3://{bucket}/{key}, found {len(files)}: {[f['Key'] for f in files]}")

    return f's3://{bucket}/{files[0]["Key"]}'

"""
Lightweight, dependency-free port of the pieces of sagemaker.clarify used to
build analysis_config.json. No import of the sagemaker SDK required — this is
useful specifically for Lambda, where adding the full sagemaker package as a
layer is unnecessary overhead for what is, under the hood, just conditional
dict-key assignment.

Ported from sagemaker.core.clarify.DataConfig / BiasConfig / SHAPConfig
(confirmed via source: these classes make no AWS/network calls in their
constructors — they only validate arguments and build a plain dict).
"""


def _set(value, key, dictionary):
    """Mirrors sagemaker.clarify._set: sets dictionary[key] = value if value is not None."""
    if value is not None:
        dictionary[key] = value


def build_data_config(
    label=None,
    headers=None,
    features=None,
    dataset_type="text/csv",
    joinsource=None,
    facet_dataset_uri=None,
    facet_headers=None,
    predicted_label_dataset_uri=None,
    predicted_label_headers=None,
    predicted_label=None,
    excluded_columns=None,
):
    """
    Equivalent to sagemaker.clarify.DataConfig(...).get_config().

    NOTE: unlike DataConfig, this does NOT take s3_data_input_path / s3_output_path —
    confirmed from source, DataConfig never writes those into analysis_config.json either
    (no "dataset_uri" key is ever set). The dataset itself is supplied via a
    ProcessingInput named "dataset", not through this config dict. See the two-input
    ProcessingInputs form (InputName: "analysis_config" + InputName: "dataset").
    """
    config = {"dataset_type": dataset_type}
    _set(features, "features", config)
    _set(headers, "headers", config)
    _set(label, "label", config)
    _set(joinsource, "joinsource_name_or_index", config)
    _set(facet_dataset_uri, "facet_dataset_uri", config)
    _set(facet_headers, "facet_headers", config)
    _set(predicted_label_dataset_uri, "predicted_label_dataset_uri", config)
    _set(predicted_label_headers, "predicted_label_headers", config)
    _set(predicted_label, "predicted_label", config)
    _set(excluded_columns, "excluded_columns", config)
    return config


def build_bias_config(
    label_values_or_threshold,
    facet_name,
    facet_values_or_threshold=None,
    group_name=None,
):
    """Equivalent to sagemaker.clarify.BiasConfig(...).get_config()."""
    if isinstance(facet_name, list):
        if not facet_name:
            raise ValueError("Please provide at least one facet")
        if facet_values_or_threshold is None:
            facet_list = [{"name_or_index": name} for name in facet_name]
        elif len(facet_values_or_threshold) == len(facet_name):
            facet_list = []
            for i, name in enumerate(facet_name):
                facet = {"name_or_index": name}
                _set(facet_values_or_threshold[i], "value_or_threshold", facet)
                facet_list.append(facet)
        else:
            raise ValueError("The number of facet names doesn't match the number of facet values")
    else:
        facet = {"name_or_index": facet_name}
        _set(facet_values_or_threshold, "value_or_threshold", facet)
        facet_list = [facet]

    config = {
        "label_values_or_threshold": label_values_or_threshold,
        "facet": facet_list,
    }
    _set(group_name, "group_variable", config)
    return config

def build_model_config(
    model_name,
    instance_type,
    initial_instance_count,
    accept_type=None,
    content_type=None,
):
    """Equivalent to sagemaker.clarify.ModelConfig(...).get_predictor_config()."""
    config = {
        "model_name": model_name,
        "instance_type": instance_type,
        "initial_instance_count": initial_instance_count,
    }
    _set(accept_type, "accept_type", config)
    _set(content_type, "content_type", config)
    return {"predictor": config}

def build_shap_config(
    baseline=None,
    num_samples=None,
    agg_method=None,
    use_logit=False,
    save_local_shap_values=True,
    seed=None,
    num_clusters=None,
    features_to_explain=None,
):
    """Equivalent to sagemaker.clarify.SHAPConfig(...).get_explainability_config()."""
    if agg_method is not None and agg_method not in ("mean_abs", "median", "mean_sq"):
        raise ValueError(f"Invalid agg_method {agg_method}. Please choose mean_abs, median, or mean_sq.")
    if num_clusters is not None and baseline is not None:
        raise ValueError("Baseline and num_clusters cannot be provided together. Please specify one of the two.")

    shap_config = {
        "use_logit": use_logit,
        "save_local_shap_values": save_local_shap_values,
    }
    _set(baseline, "baseline", shap_config)
    _set(num_samples, "num_samples", shap_config)
    _set(agg_method, "agg_method", shap_config)
    _set(seed, "seed", shap_config)
    _set(num_clusters, "num_clusters", shap_config)
    _set(features_to_explain, "features_to_explain", shap_config)

    return {"methods": {"shap": shap_config}}


def prep_baseline_sets_handler(event, context):
    baseline_file = event['baseline_file']
    target_label = event['target_label']
    target_type = event['target_type']
    baseline_dir = event['baseline_dir']
    columns=json.loads(event['columns']) if 'columns' in event else None
        
    # get baseline X
    headered_baseline=df_from_s3(baseline_file, header=None, names=columns) # baseline file == validation file
    headered_baseline[target_label] = headered_baseline[target_label].astype(string_to_type(target_type))
    baseline_headered_file=f'{baseline_dir}/baseline_headered.csv'
    df_to_s3_csv(headered_baseline, baseline_headered_file, index=False, header=True)

    # make baseline X
    baseline_X = headered_baseline.drop(columns=[target_label])
    baseline_X_file=f'{baseline_dir}/baseline_X.csv'
    df_to_s3_csv(baseline_X, baseline_X_file, index=False, header=False)

    return {
        'BASELINE_HEADERED_FILE': baseline_headered_file,
        'BASELINE_X_FILE':baseline_X_file
    }


def build_bias_methods_config(pre_training_methods="all", post_training_methods="all"):
    """
    Equivalent to the pre_training_methods/post_training_methods args passed to
    SageMakerClarifyProcessor.run_bias() — not part of BiasConfig itself.
    Pass None for either to omit that section entirely (e.g. pre-training only,
    which avoids the predictor/shadow-endpoint requirement).
    """
    methods = {}
    if pre_training_methods is not None:
        methods["pre_training_bias"] = {"methods": pre_training_methods}
    if post_training_methods is not None:
        methods["post_training_bias"] = {"methods": post_training_methods}
    return {"methods": methods}


def get_baseline_preds_handler(event, context):
    model_name = event['model_name']
    transform_out_dir = event['transform_out_dir']
    baseline_dir = event['baseline_dir']
    dq_monitor_dir = event['dq_monitor_dir']
    mq_monitor_dir = event['mq_monitor_dir']
    mb_monitor_dir = event['mb_monitor_dir']
    me_monitor_dir = event['me_monitor_dir']
    baseline_headered_file = event['baseline_headered_file']
    predict_label = event['predict_label']
    target_label = event['target_label']
    target_type = event['target_type'] 
    agg_method = event['agg_method']
    predict_probability_label = event['predict_probability_label'] if 'predict_probability_label' in event else None

    transformer_out_file = get_only_file_in_dir(transform_out_dir)
    baseline_fs_p_file = f'{baseline_dir}/baseline_fs_p.csv'
    
    print(f'transformer_out_file {transformer_out_file}')
    print(f'baseline_fs_p_file {baseline_fs_p_file}')

    # move file to dest
    uri_1_bucket, uri_1_key = parse_s3_uri(transformer_out_file) # [features] + prediction
    uri_2_bucket, uri_2_key = parse_s3_uri(baseline_fs_p_file)
    s3_client.copy_object(CopySource={'Bucket': uri_1_bucket, 'Key': uri_1_key}, Bucket=uri_2_bucket, Key=uri_2_key)
    s3_client.delete_object(Bucket=uri_1_bucket, Key=uri_1_key)

    # Assemble all BL columns and save
    # Get headered predictions colum
    baseline_fs_p_df = df_from_s3(baseline_fs_p_file, header=None)
    baseline_p_df = baseline_fs_p_df.iloc[:, [-1]]
    baseline_p_df.columns=[predict_label]

    # Get 
    baseline_headered_df = df_from_s3(baseline_headered_file, header=0)
    baseline_full_df = pd.concat([baseline_headered_df.reset_index(drop=True), baseline_p_df.reset_index(drop=True)], axis=1)
    baseline_full_df[target_label] = baseline_full_df[target_label].astype(string_to_type(target_type))
    baseline_full_df[predict_label] = baseline_full_df[predict_label].astype(string_to_type(target_type))
    baseline_full_file = f'{baseline_dir}/baseline_full.csv'
    print(f'baseline_full_file {baseline_full_file}')
    df_to_s3_csv(baseline_full_df, baseline_full_file, index=False, header=True)

    # Save monitor specific baseline files
    if predict_probability_label:
        feature_columns = [c for c in baseline_full_df.columns if c not in (target_label, predict_label, predict_probability_label)]
        dq_df=baseline_full_df.drop(columns=[target_label, predict_label, predict_probability_label])
        mq_df=baseline_full_df[[target_label, predict_label, predict_probability_label]]
        mb_df=baseline_full_df.drop(columns=[predict_probability_label])
        me_df=baseline_full_df.drop(columns=[target_label, predict_probability_label])
    else:
        feature_columns = [c for c in baseline_full_df.columns if c not in (target_label, predict_label)]
        dq_df=baseline_full_df.drop(columns=[target_label, predict_label])
        mq_df=baseline_full_df[[target_label, predict_label]]
        mb_df=baseline_full_df
        me_df=baseline_full_df.drop(columns=[target_label])

    # DQ Baseline
    df_to_s3_csv(dq_df, f"{dq_monitor_dir}/baseline.csv", index=False, header=True)
    # MQ Baseline
    df_to_s3_csv(mq_df, f"{mq_monitor_dir}/baseline.csv", index=False, header=True)
    # MB Basline
    df_to_s3_csv(mb_df, f"{mb_monitor_dir}/baseline.csv", index=False, header=True)
    # ME Baseline
    df_to_s3_csv(me_df, f"{me_monitor_dir}/baseline.csv", index=False, header=True)

    # Model Bias
    analysis_config = {
        **build_data_config(
            label=target_label,
            headers=list(mb_df.columns), 
            features=None,
            dataset_type="text/csv",
            joinsource=None,
            facet_dataset_uri=None,
            facet_headers=None,
            predicted_label_dataset_uri=None,
            predicted_label_headers=None,
            predicted_label=predict_label,
            excluded_columns=None,
        ),
        **build_bias_config(
            label_values_or_threshold=[0.5], # what counts as the positive/favorable outcome for your target label (Binary — exactly one positive value) (Categorical — one or more (but not all) categories) (Regression — a single threshold value)
            facet_name="sex_F",
            facet_values_or_threshold=None, # treat these value as the sensitive group - None = All
            group_name=None, # for bias interaction - None = do not compute
        ),
        **build_bias_methods_config(
            pre_training_methods="all", 
            post_training_methods="all"
        ),
        **build_model_config(
            model_name=model_name, 
            instance_type="ml.m5.xlarge", 
            initial_instance_count=1
        ),
    }
    bkt, key = parse_s3_uri(f"{mb_monitor_dir}/info/analysis_config.json")
    s3_client.put_object(Bucket=bkt, Key=key, Body=json.dumps(analysis_config))

    # Model Explainability example
    analysis_config = {
        **build_data_config(
            label=None,
            headers=list(me_df.columns), 
            features=None,
            dataset_type="text/csv",
            joinsource=None,
            facet_dataset_uri=None,
            facet_headers=None,
            predicted_label_dataset_uri=None,
            predicted_label_headers=None,
            predicted_label=predict_label,
            excluded_columns=None,
        ),
        **build_shap_config(
            baseline=[baseline_full_df[feature_columns].mean().tolist()],
            num_samples=None,
            agg_method=agg_method,
            use_logit=False,
            save_local_shap_values=True,
            seed=None,
            num_clusters=None,
            features_to_explain=None,
        ),
        **build_model_config(
            model_name=model_name, 
            instance_type="ml.m5.xlarge", 
            initial_instance_count=1
        ),
    }
    bkt, key = parse_s3_uri(f"{me_monitor_dir}/info/analysis_config.json")
    s3_client.put_object(Bucket=bkt, Key=key, Body=json.dumps(analysis_config))

    return {
        'BASELINE_FS_P_FILE': baseline_fs_p_file,
        'BASELINE_FULL_FILE': baseline_full_file,
        'BASELINE_DQ_FILE': f"{dq_monitor_dir}/baseline.csv",
        'BASELINE_MQ_FILE': f"{mq_monitor_dir}/baseline.csv",
        'BASELINE_MB_FILE': f"{mb_monitor_dir}/baseline.csv",
        'BASELINE_ME_FILE': f"{me_monitor_dir}/baseline.csv",
        "MB_ANALYSIS_CONFIG_FILE": f"{mb_monitor_dir}/info/analysis_config.json",
        "ME_ANALYSIS_CONFIG_FILE": f"{me_monitor_dir}/info/analysis_config.json"
    }

def process_baseline_results_handler(event, context):
    dq_bl_out_dir = event['dq_bl_out_dir'] if 'dq_bl_out_dir' in event else None
    mq_bl_out_dir = event['mq_bl_out_dir'] if 'mq_bl_out_dir' in event else None
    mb_bl_out_dir = event['mb_bl_out_dir'] if 'mb_bl_out_dir' in event else None
    me_bl_out_dir = event['me_bl_out_dir'] if 'me_bl_out_dir' in event else None

    dq_monitor_dir = event['dq_monitor_dir'] if 'dq_monitor_dir' in event else None
    mq_monitor_dir = event['mq_monitor_dir'] if 'mq_monitor_dir' in event else None
    mb_monitor_dir = event['mb_monitor_dir'] if 'mb_monitor_dir' in event else None
    me_monitor_dir = event['me_monitor_dir'] if 'me_monitor_dir' in event else None



    def move_file_dirs(source_dir, dest_dir):
        source_bucket, source_prefix = parse_s3_uri(dq_bl_out_dir+'/')
        dest_bucket, dest_prefix = parse_s3_uri(dq_monitor_dir+'/info')
        resp = s3_client.list_objects_v2(Bucket=source_bucket,Prefix=source_prefix,Delimiter="/")  # delimiter=only immediate files, not nested "subdirectories"

        for obj in resp.get("Contents", []):
            src_key = obj["Key"]
            if src_key.endswith("/"):
                continue  # skip folder placeholder objects

            relative_path = src_key[len(source_prefix):]
            dest_key = f"{dest_prefix.rstrip('/')}/{relative_path}"

            s3_client.copy_object(Bucket=dest_bucket, Key=dest_key, CopySource={"Bucket": source_bucket, "Key": src_key},)
            s3_client.delete_object(Bucket=source_bucket, Key=src_key)

        if dq_bl_out_dir and dq_monitor_dir: move_file_dirs(dq_bl_out_dir, dq_monitor_dir)
        if mq_bl_out_dir and mq_monitor_dir: move_file_dirs(mq_bl_out_dir, mq_monitor_dir)
        if mb_bl_out_dir and mb_monitor_dir: move_file_dirs(mb_bl_out_dir, mb_monitor_dir)
        if me_bl_out_dir and me_monitor_dir: move_file_dirs(me_bl_out_dir, me_monitor_dir)