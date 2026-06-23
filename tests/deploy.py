import boto3, json, uuid
sfn_client = boto3.client('stepfunctions')

response = sfn_client.start_execution(
    stateMachineArn='string',
    # name='string',
    input=json.dumps({
        'event':{
            'MODEL_PACKAGE_VERSION':1,
            'ACTION':'deploy',
            'BASELINE_FILE':'aaa',
            'MONITOR_INSTANCE_TYPE':'ml.m5.large',
            'ENDPOINT_INSTANCE_TYPE':'ml.m5.large',
            'TRANSFORM_INSTANCE_TYPE':'ml.m5.large',
            'FAIL_ON_VIOLATION':False,
            'RGISTER_NEW_BASELINE':False,
            'MONITOR_SCHEDULE_EXPRESSION':'cron(0 * ? * * *)',
            'ENABLE_DATA_QUALITY_MONITORING':True,
            'ENABLE_MODEL_BIAS_MONITORING':True,
            'ENABLE_MODEL_EXPLAINABILITY_MONITORING':True,
            'ENABLE_MODEL_QUALITY_MONITORING':True,
            'SNS_TOPIC_ARN':'aaa',
            'ENABLE_SNS_NOTIFICATION':False,
            'GROUND_TRUTH_DIR':f's3://omm-test-bucket/ground-truth/abalone',
            'BATCH_INPUT_DIR':f's3://omm-test-bucket/batch_input/abalone',
        }
    }),
    traceHeader=str(uuid.uuid4())
)

print(response['executionArn'])
print(response['startDate'])