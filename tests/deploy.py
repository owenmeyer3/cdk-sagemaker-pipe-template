import boto3, json, uuid
sfn_client = boto3.client('stepfunctions')

response = sfn_client.start_execution(
    stateMachineArn='arn:aws:states:us-east-1:088461143167:stateMachine:SM934E715A-oZm8fUzkL9YU',
    # name='string',
    input=json.dumps({
        'MODEL_PACKAGE_VERSION':1,
        'ACTION':'', # realtime / batch
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