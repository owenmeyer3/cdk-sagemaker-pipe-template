import sys, boto3, json

region='us-east-1'
data_bucket='omm-test-bucket'
name='abalone'
model_package_group_name='abalone'
model_package_version = 1
#endpoint_name=f'{model_package_group_name}-{model_package_version}-endpoint'
endpoint_name='abalone-endpoint'
endpoint_config_name = endpoint_name + "-config"
s3_resource=boto3.resource('s3')
boto_session=boto3.Session(region_name=region)
sm_client = boto_session.client('sagemaker', region_name=region)

pipeline_dir =   f's3://{data_bucket}/pipelines/{name}'
baseline_dir =   f'{pipeline_dir}/baseline'
monitors_dir=    f'{pipeline_dir}/monitors'
batch_out_dir=   f'{pipeline_dir}/batch_out'
data_capture_dir=f'{pipeline_dir}/capture'
dq_monitor_dir=  f'{pipeline_dir}/data-quality'
mq_monitor_dir=  f'{pipeline_dir}/model-quality'
mb_monitor_dir=  f'{pipeline_dir}/model-bias'
me_monitor_dir=  f'{pipeline_dir}/model-explainability'
db_monitor_dir=  f'{pipeline_dir}/data-bias'

# Delete Monitoring Schedules
response = sm_client.list_monitoring_schedules(EndpointName=endpoint_name)

# Delete monitor schedules
existing_monitor_types=[] # 'DataQuality'|'ModelQuality'|'ModelBias'|'ModelExplainability'
for ms in response["MonitoringScheduleSummaries"]:
    print(f'MonitoringSchedule: {ms}')
    print("DELETING SCHEDULE " + ms['MonitoringScheduleName'])
    existing_monitor_types.append(ms["MonitoringType"])
    sm_client.delete_monitoring_schedule(MonitoringScheduleName=ms['MonitoringScheduleName'])

# Delete monitor jobs
if 'DataQuality' in existing_monitor_types:
    jobs = sm_client.list_data_quality_job_definitions(EndpointName=endpoint_name)['JobDefinitionSummaries']
    for job in jobs:
        print("DELETING JOB " + job['MonitoringJobDefinitionName'])
        response = sm_client.delete_data_quality_job_definition(JobDefinitionName=job['MonitoringJobDefinitionName'])

if 'ModelQuality' in existing_monitor_types:
    jobs = sm_client.list_model_quality_job_definitions(EndpointName=endpoint_name)['JobDefinitionSummaries']
    for job in jobs:
        print("DELETING JOB " + job['MonitoringJobDefinitionName'])
        response = sm_client.delete_data_quality_job_definition(JobDefinitionName=job['MonitoringJobDefinitionName'])

if 'ModelBias' in existing_monitor_types:
    jobs = sm_client.list_model_bias_job_definitions(EndpointName=endpoint_name)['JobDefinitionSummaries']
    for job in jobs:
        print("DELETING JOB " + job['MonitoringJobDefinitionName'])
        response = sm_client.delete_data_quality_job_definition(JobDefinitionName=job['MonitoringJobDefinitionName'])

if 'ModelExplainability' in existing_monitor_types:
    jobs = sm_client.list_data_quality_job_definitions(EndpointName=endpoint_name)['JobDefinitionSummaries']
    for job in jobs:
        print("DELETING JOB " + job['MonitoringJobDefinitionName'])
        response = sm_client.delete_model_explainability_job_definition(JobDefinitionName=job['MonitoringJobDefinitionName'])

# Delete endpoint first (must delete before config)
try:
    print("DELETING ENDPOINT " + endpoint_name)
    sm_client.delete_endpoint(EndpointName=endpoint_name)
    waiter = sm_client.get_waiter('endpoint_deleted')
    waiter.wait(EndpointName=endpoint_name)
except sm_client.exceptions.ClientError:
    print("Endpoint does not exist")

# Delete endpoint config
try:
    print("DELETING ENDPOINT CONFIG " + endpoint_config_name)
    sm_client.delete_endpoint_config(EndpointConfigName=endpoint_config_name)
    print("Deleted endpoint config")
except sm_client.exceptions.ClientError:
    print("Endpoint config does not exist")

# # Delete all objects with a specific prefix (directory)
bucket = s3_resource.Bucket(data_bucket)
bucket.objects.filter(Prefix=f'{pipeline_dir}/baseline/').delete()
bucket.objects.filter(Prefix=f'{pipeline_dir}/monitors/').delete()
bucket.objects.filter(Prefix=f'{pipeline_dir}/batch-out/').delete()
bucket.objects.filter(Prefix=f'{pipeline_dir}/capture/').delete()
bucket.objects.filter(Prefix=f'{pipeline_dir}/data-quality/').delete()
bucket.objects.filter(Prefix=f'{pipeline_dir}/model-quality/').delete()
bucket.objects.filter(Prefix=f'{pipeline_dir}/model-bias/').delete()
bucket.objects.filter(Prefix=f'{pipeline_dir}/model-explainability/').delete()
bucket.objects.filter(Prefix=f'{pipeline_dir}/data-bias/').delete()
