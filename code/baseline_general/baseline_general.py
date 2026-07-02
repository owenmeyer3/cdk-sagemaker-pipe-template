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

def df_to_s3(df, s3_uri, index=False, header=True):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=index, header=header)

    bucket, key = parse_s3_uri(s3_uri)
    s3_client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())

def make_baseline_sets(
    baseline_headered_file,
    baseline_pred_file,
    dq_monitor_dir,
    db_monitor_dir,
    mq_monitor_dir,
    mb_monitor_dir,
    me_monitor_dir,
    target_name,
    prediction_name,
    target_type='float'
):
    print(f'baseline_headered_file:{baseline_headered_file}')
    print(f'baseline_pred_file:{baseline_pred_file}')
    baseline_headered=df_from_s3(baseline_headered_file, header=0)
    baseline_pred=df_from_s3(baseline_pred_file, header=None)
    baseline_pred.columns=[prediction_name]
    baseline_full = pd.concat([baseline_pred, baseline_headered], axis=1)
    baseline_full[target_name] = baseline_full[target_name].astype(string_to_type(target_type))
    baseline_full[prediction_name] = baseline_full[prediction_name].astype(string_to_type(target_type))

    # Data Quality → input features only
    df_to_s3(
        baseline_full.drop(columns=[target_name, prediction_name]), 
        f'{dq_monitor_dir}/baseline.csv', 
        index=False, 
        header=True
    )

    # Data Bias → input features + target
    df_to_s3(
        baseline_full.drop(columns=[prediction_name]), 
        f'{db_monitor_dir}/baseline.csv', 
        index=False, 
        header=True
    )

    # Model Quality → predictions + ground truth labels
    df_to_s3(
        baseline_full[[target_name, prediction_name]], 
        f'{mq_monitor_dir}/baseline.csv', 
        index=False, 
        header=True
    )

    # Model Bias → features + predictions + labels
    df_to_s3(
        baseline_full, 
        f'{mb_monitor_dir}/baseline.csv', 
        index=False, 
        header=True
    )

    # Model Explainability → input features + predictions (uses SHAP values)
    df_to_s3(
        baseline_full.drop(columns=[target_name]), 
        f'{me_monitor_dir}/baseline.csv', 
        index=False, 
        header=True
    )

    # baseline_X_file for SHAP
    # df_to_s3(
    #     baseline_full.drop(columns=[target_name, prediction_name]),
    #     baseline_X_file, 
    #     index=False, 
    #     header=False
    # )

    return None


def prep_baseline_sets_handler(event, context):
    baseline_file = event['baseline_file']
    target_name = event['target_name']
    target_type = event['target_type']
    baseline_dir = event['baseline_dir']
    columns=json.loads(event['columns']) if 'columns' in event else None
        
    # get baseline X
    headered_baseline=df_from_s3(baseline_file, header=None, names=columns) # baseline file == validation file
    headered_baseline[target_name] = headered_baseline[target_name].astype(string_to_type(target_type))
    baseline_headered_file=f'{baseline_dir}/baseline_headered.csv'
    df_to_s3(headered_baseline, baseline_headered_file, index=False, header=True)

    # make baseline X
    baseline_X = headered_baseline.drop(columns=[target_name])
    baseline_X_file=f'{baseline_dir}/baseline_X.csv'
    df_to_s3(baseline_X, baseline_X_file, index=False, header=False)

    return {
        'BASELINE_HEADERED_FILE': baseline_headered_file,
        'BASELINE_X_FILE':baseline_X_file,
        'BASELINE_X_FILENAME': 'baseline_X.csv'
    }


def get_baseline_preds_handler(event, context):
    transform_out_dir = event['transform_out_dir']
    baseline_X_filename = event['baseline_X_filename']
    baseline_dir = event['baseline_dir']

    transformer_out_file = f'{transform_out_dir}/{baseline_X_filename}.out'
    baseline_pred_file = f'{baseline_dir}/baseline_preds.csv'
    print(f'transformer_out_file {transformer_out_file}')
    print(f'baseline_pred_file {baseline_pred_file}')

    # move file to dest
    uri_1_bucket, uri_1_key = parse_s3_uri(transformer_out_file)
    uri_2_bucket, uri_2_key = parse_s3_uri(baseline_pred_file)
    s3_client.copy_object(CopySource={'Bucket': uri_1_bucket, 'Key': uri_1_key}, Bucket=uri_2_bucket, Key=uri_2_key)
    s3_client.delete_object(Bucket=uri_1_bucket, Key=uri_1_key)

    return {
        'BASELINE_PRED_FILE': baseline_pred_file
    }


def make_baseline_sets_handler(event, context):
    baseline_headered_file = event['baseline_headered_file']
    baseline_pred_file = event['baseline_pred_file']
    dq_monitor_dir = event['dq_monitor_dir']
    db_monitor_dir = event['db_monitor_dir']
    mq_monitor_dir = event['mq_monitor_dir']
    mb_monitor_dir = event['mb_monitor_dir']
    me_monitor_dir = event['me_monitor_dir']
    target_name = event['target_name']
    prediction_name = event['prediction_name']
    baseline_X_file = event['baseline_X_file']
    target_type = event['target_type'] if 'target_type' in event else 'float'

    print(f'baseline_headered_file: {baseline_headered_file}')
    print(f'baseline_pred_file: {baseline_pred_file}')
    print(f'dq_monitor_dir: {dq_monitor_dir}')
    print(f'db_monitor_dir: {db_monitor_dir}')
    print(f'mq_monitor_dir: {mq_monitor_dir}')
    print(f'mb_monitor_dir: {mb_monitor_dir}')
    print(f'me_monitor_dir: {me_monitor_dir}')
    print(f'target_name: {target_name}')
    print(f'prediction_name: {prediction_name}')
    print(f'baseline_X_file: {baseline_X_file}')
    print(f'target_type: {target_type}')

    result = make_baseline_sets(
        baseline_headered_file,
        baseline_pred_file,
        dq_monitor_dir,
        db_monitor_dir,
        mq_monitor_dir,
        mb_monitor_dir,
        me_monitor_dir,
        target_name,
        prediction_name,
        target_type=target_type
    )
    
    return {}