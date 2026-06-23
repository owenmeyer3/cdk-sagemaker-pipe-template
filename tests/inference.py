import boto3, json, uuid
sfn_client = boto3.client('stepfunctions')

response = sfn_client.start_execution(
    stateMachineArn='string',
    # name='string',
    input=json.dumps({
        'ModelPackageVersion':1,
        'Action':'inference',
        'BaselineFile':'aaa',
        'MonitorInstanceType':'ml.m5.large',
        'EndpointInstanceType':'ml.m5.large',
        'TransformInstanceType':'ml.m5.large',
        'FailOnViolation':False,
        'RegisterNewBaseline':False,
        'MonitorScheduleExpression':'cron(0 * ? * * *)',
        'EnableDataQualityMonitoring':True,
        'EnableModelBiasMonitoring':True,
        'EnableModelExplainabilityMonitoring':True,
        'EnableModelQualityMonitoring':True,
        'Environment':'dev',
        'SnsTopicArn':'aaa',
        'EnableSnsNotification':False,
        'GroundTruthDir':f's3://omm-test-bucket/ground-truth/abalone',
        'BatchInputDir':f's3://omm-test-bucket/batch_input/abalone',

    }),
    traceHeader=str(uuid.uuid4())
)

print(response['executionArn'])
print(response['startDate'])