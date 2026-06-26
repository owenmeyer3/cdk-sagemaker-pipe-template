import boto3
import sagemaker.core.image_uris as sm_image_uris

region='us-east-1'
data_bucket='omm-test-bucket'
name='abalone'
model_package_group_name='abalone'
model_package_version = 1
#endpoint_name=f'{model_package_group_name}-{model_package_version}-endpoint'
endpoint_name=f'{model_package_group_name}-{model_package_version}-endpoint'
endpoint_config_name = endpoint_name + "-config"
s3_resource=boto3.resource('s3')
boto_session=boto3.Session(region_name=region)
sm_client = boto_session.client('sagemaker', region_name=region)

def register_new_model_version(model_package_group_name, model_data_url, image, inference_types=['ml.m5.large', 'ml.m5.xlarge'], tranform_types=['ml.m5.large', 'ml.m5.xlarge'], context_types=['text/csv'], response_mime_types=['text/csv']):
    sm_client = boto3.client('sagemaker')
    model_package_exists = False
    groups = sm_client.list_model_package_groups()
    for item in groups['ModelPackageGroupSummaryList']:
        if item['ModelPackageGroupName'] == model_package_group_name:
            print('Using existing ModelPackageGroupName')
            model_package_exists = True
            break
    if not model_package_exists:
        print('Making new ModelPackageGroupName')
        sm_client.create_model_package_group(ModelPackageGroupName=model_package_group_name)

    response = sm_client.create_model_package(
        ModelPackageGroupName='abalone',  # use group name, not ModelPackageName
        InferenceSpecification={
            'Containers': [
                {
                    'Image': image,
                    'ModelDataUrl': model_data_url
                }
            ],
            'SupportedContentTypes': context_types,
            'SupportedResponseMIMETypes': response_mime_types,
            'SupportedRealtimeInferenceInstanceTypes': inference_types,
            'SupportedTransformInstanceTypes': tranform_types
        },
        ModelApprovalStatus='PendingManualApproval'
    )

    return response['ModelPackageArn']



model_package_group_name='abalone'
model_data_url='s3://omm-test-bucket/abalone-train/model/abalone-train-job-20260625213553/output/model.tar.gz'
image=sm_image_uris.retrieve('xgboost', region, version='1.5-1')
register_new_model_version(
    model_package_group_name, 
    model_data_url, 
    image, 
    inference_types=['ml.m5.large', 'ml.m5.xlarge'], 
    tranform_types=['ml.m5.large', 'ml.m5.xlarge'], 
    context_types=['text/csv'], 
    response_mime_types=['text/csv']
)