import boto3, json, uuid
sfn_client = boto3.client('stepfunctions')

# def parse_s3_uri(s3_uri):
#     # s3://bucket-name/path/to/key
#     s3_uri = s3_uri.replace('s3://', '')
#     bucket, key = s3_uri.split('/', 1)
#     return bucket, key



#     transform_out_dir = event['transform_out_dir']
#     baseline_X_filename = event['baseline_X_filename']
#     baseline_pred_file_dest = event['baseline_pred_file_dest']

# def get_baseline_preds_handler(event, context):
#     transform_out_dir = event['transform_out_dir']
#     baseline_X_filename = event['baseline_X_filename']
#     baseline_pred_file_dest = event['baseline_pred_file_dest']

#     transform_out_dir
#     baseline_X_filename
#     baseline_pred_file_dest

#     transformer_out_file = f'{transform_out_dir}/{baseline_X_filename}.out'

#     # move file to dest
#     s3_client = boto3.client('s3')
#     uri_1_bucket, uri_1_key = parse_s3_uri(transformer_out_file)
#     uri_2_bucket, uri_2_key = parse_s3_uri(baseline_pred_file_dest)
#     s3_client.copy_object(CopySource={'Bucket': uri_1_bucket, 'Key': uri_1_key}, Bucket=uri_2_bucket, Key=uri_2_key)
#     s3_client.delete_object(Bucket=uri_1_bucket, Key=uri_1_key)

#     return {
#         'BASELINE_PRED_FILE': baseline_pred_file_dest
#     }

# event={
#     'transform_out_dir': "s3://omm-test-bucket/pipelines/abalone/baseline",
#     'baseline_X_filename':'target_name',
#     'baseline_pred_file_dest': target_type,
# }

response = sfn_client.start_execution(
    stateMachineArn='arn:aws:states:us-east-1:088461143167:stateMachine:SM934E715A-oZm8fUzkL9YU',
    # name='string',
    input=json.dumps({
        'MODEL_PACKAGE_VERSION':1,
        'ACTION':'deploy',
        'BASELINE_FILE':'s3://omm-test-bucket/abalone-train/data/validation/validation.csv',
        'BASELINE_COLS':['rings','length', 'diameter', 'height', 'whole_weight', 'shucked_weight', 'viscera_weight', 'shell_weight', 'sex_F', 'sex_I', 'sex_M'],
        'MONITOR_INSTANCE_TYPE':'ml.m5.large',
        'ENDPOINT_INSTANCE_TYPE':'ml.m5.large',
        'TRANSFORM_INSTANCE_TYPE':'ml.m5.large',
        'FAIL_ON_VIOLATION':"TRUE",
        'MONITOR_SCHEDULE_EXPRESSION':'cron(0 * ? * * *)',
        'MONITOR_ANALYSIS_START_TIME':'-PT2H',
        'MONITOR_ANALYSIS_END_TIME':'-PT1H',
        'REBASELINE':"TRUE",
        'ENABLE_DATA_QUALITY_MONITORING':"TRUE",
        'ENABLE_MODEL_BIAS_MONITORING':"TRUE",
        'ENABLE_MODEL_EXPLAINABILITY_MONITORING':"TRUE",
        'ENABLE_MODEL_QUALITY_MONITORING':"TRUE",
        'ENABLE_DATA_QUALITY_CHECK':"TRUE",
        'ENABLE_MODEL_BIAS_CHECK':"TRUE",
        'ENABLE_MODEL_EXPLAINABILITY_CHECK':"TRUE",
        'ENABLE_MODEL_QUALITY_CHECK':"TRUE",
        'SNS_TOPIC_ARN':'arn',
        'ENABLE_SNS_NOTIFICATION':"FALSE",
        'GROUND_TRUTH_DIR':f's3://omm-test-bucket/abalone-ground-truth',
        'BATCH_INPUT_DIR':f's3://omm-test-bucket/abalone-batch-input',
    }),
    traceHeader=str(uuid.uuid4())
)

print(response['executionArn'])
print(response['startDate'])