import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_model_versions(sm_client, model_package_name, model_package_version):

    groups = sm_client.list_model_package_groups()
    for g in groups['ModelPackageGroupSummaryList']:
        group_name=g['ModelPackageGroupName']
        versions = sm_client.list_model_packages(ModelPackageGroupName=group_name)

        for v in versions['ModelPackageSummaryList']:
            if (v['ModelPackageGroupName'] == model_package_name) & (v['ModelPackageVersion'] == model_package_version):
                return v
    logger.error('No model_version found in registry')
    return None

def model_name_exists(sm_client, model_name):
    try:
        sm_client.describe_model(ModelName=model_name)
        return True
    except sm_client.exceptions.ClientError:
        return False

def get_or_create_model_object_from_registry(sm_client, model_package_name, role, model_package_version='latest'):

    model_package_details = sm_client.list_model_packages(
        ModelPackageGroupName=model_package_name,
        # ModelApprovalStatus='Approved',
        SortBy='CreationTime',
        SortOrder='Descending'
    )
    if not model_package_details:
        logger.error('model package group name does not exist')
        return

    if model_package_version=='latest':
        model_package_version = model_package_details['ModelPackageSummaryList'][0]['ModelPackageVersion']
    elif isinstance(model_package_version, str):
        model_package_version=int(model_package_version)
    
    model_version = get_model_versions(sm_client, model_package_name, model_package_version)
    model_package_arn=model_version['ModelPackageArn']
    if model_version['ModelApprovalStatus'] != 'Approved':
        logger.error('model version not approved')
        return None

    model_name = model_package_name + "-" + str(model_package_version)

    # look for existing model
    if model_name_exists(sm_client, model_name):
        print("using existing model")
        describe_model_response = sm_client.describe_model(ModelName=model_name)
        return [model_name, describe_model_response["ModelArn"]]

    # create new model
    print("using new model")
    create_model_response = sm_client.create_model(
        ModelName = model_name,
        ExecutionRoleArn = role,
        Containers = [{'ModelPackageName': model_package_arn}]
    )

    return [model_name, create_model_response["ModelArn"]]

def handler(event, context):
    sm_client = boto3.client('sagemaker')

    model_package_group_name = event['model_package_group_name']
    model_package_version = event['model_package_version']
    role = event['role']

    model_name, model_package_arn = get_or_create_model_object_from_registry(sm_client, model_package_group_name, role, model_package_version=model_package_version)
    return {'model_name': model_name, 'model_package_arn': model_package_arn}
