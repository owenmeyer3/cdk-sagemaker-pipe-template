import boto3, logging, json
import pandas as pd
import sqlalchemy, json, datetime, botocore, time, json, boto3
from sklearn.model_selection import train_test_split
import networkx as nx
import matplotlib.pyplot as plt

logger = logging.getLogger()
logger.setLevel(logging.INFO)

### static
def train_val_test_split(X, y, train_size=0.7, val_size=0.15, random_state=None):
    test_val_size = 1-train_size
    test_size_aug=(test_val_size-val_size)/test_val_size
    X_train, X_testval, y_train, y_testval = train_test_split(X, y, test_size=test_val_size, random_state=random_state)
    X_val, X_test, y_val, y_test = train_test_split(X_testval, y_testval, test_size=test_size_aug, random_state=random_state)
    return [X_train, X_val, X_test, y_train, y_val, y_test]


def get_sm_service_role_arn():
    return "arn:aws:iam::088461143167:role/SageMakerExecutionRole-1"


def move_s3_file(boto_session, s3_uri_1, s3_uri_2):
    s3_client = boto_session.client('s3')
    uri_1_bucket, uri_1_key = parse_s3_uri(s3_uri_1)
    uri_2_bucket, uri_2_key = parse_s3_uri(s3_uri_2)

    s3_client.copy_object(CopySource={'Bucket': uri_1_bucket, 'Key': uri_1_key}, Bucket=uri_2_bucket, Key=uri_2_key)
    s3_client.delete_object(Bucket=uri_1_bucket, Key=uri_1_key)


def view_model_versions(boto_session):
    sm_client = boto_session.client('sagemaker')
    groups = sm_client.list_model_package_groups()

    ModelPackageGroupNames=[]
    ModelPackageVersions=[]
    ModelPackageGroupArns=[]
    ModelPackageDescriptions=[]
    CreationTimes=[]
    ModelPackageStati=[]
    ModelApprovalStati=[]

    for g in groups['ModelPackageGroupSummaryList']:
        group_name=g['ModelPackageGroupName']
        versions = sm_client.list_model_packages(ModelPackageGroupName=group_name)

        for v in versions['ModelPackageSummaryList']:
            ModelPackageGroupNames.append(v['ModelPackageGroupName'] if 'ModelPackageGroupName' in v else '')
            ModelPackageVersions.append(v['ModelPackageVersion'] if 'ModelPackageVersion' in v else '')
            ModelPackageGroupArns.append(v['ModelPackageArn'] if 'ModelPackageArn' in v else '')
            ModelPackageDescriptions.append(v['ModelPackageDescription'] if 'ModelPackageDescription' in v else '')
            CreationTimes.append(v['CreationTime'] if 'CreationTime' in v else '')
            ModelPackageStati.append(v['ModelPackageStatus'] if 'ModelPackageStatus' in v else '')
            ModelApprovalStati.append(v['ModelApprovalStatus'] if 'ModelApprovalStatus' in v else '')

    df_dict={
        'ModelPackageGroupName':ModelPackageGroupNames, 
        'ModelPackageVersion':ModelPackageVersions, 
        'ModelPackageArn':ModelPackageGroupArns,
        'ModelPackageDescription':ModelPackageDescriptions, 
        'CreationTime':CreationTimes, 
        'ModelPackageStatus':ModelPackageStati,  
        'ModelApprovalStatus':ModelApprovalStati
        }
    return pd.DataFrame(df_dict)


def view_model_groups(boto_session):
    sm_client = boto_session.client('sagemaker')
    groups = sm_client.list_model_package_groups()

    ModelPackageGroupNames=[]
    ModelPackageGroupArns=[]
    CreationTimes=[]
    ModelPackageGroupStati=[]

    for item in groups['ModelPackageGroupSummaryList']:
        ModelPackageGroupNames.append(item['ModelPackageGroupName'])
        ModelPackageGroupArns.append(item['ModelPackageGroupArn'])
        CreationTimes.append(item['CreationTime'])
        ModelPackageGroupStati.append(item['ModelPackageGroupStatus'])

    df_dict={'ModelPackageGroupName':ModelPackageGroupNames, 'ModelPackageGroupArn':ModelPackageGroupArns, 'CreationTime':CreationTimes, 'ModelPackageGroupStatus':ModelPackageGroupStati}
    return pd.DataFrame(df_dict)


def write_data_capture(data, path, boto_session, header_order=None, encoding="CSV", created_at=datetime.datetime.now(datetime.timezone.utc)):
    # input = [{"features":{}, "prediction":"", "event_id":""}]
    s3_client = boto_session.client('s3')
    records = []

    for d in data:

        formatted_features=None

        if encoding=="CSV":
            values=None
            if header_order:
                values = [str(d["features"][k]) for k in header_order]
            else:
                values=list(d.values())
            formatted_features = ','.join()
        else:
            raise ValueError("encoding must be in [CSV]")

        records.append(
            json.dumps({
                "captureData": {
                    "endpointInput": {
                        "data": formatted_features,
                        "encoding": encoding
                    },
                    "endpointOutput": {
                        "data": str(d["prediction"]),
                        "encoding": encoding
                    }
                },
                "eventMetadata": {
                    "eventId": d["event_id"],
                    "inferenceTime": created_at.strftime('%Y-%m-%dT%H:%M:%SZ')
                },
                "eventVersion": "0"
            })
        )

    jsonl = '\n'.join(records)

    bucket, key = parse_s3_uri(path)
    s3_client.put_object(
        Bucket=bucket,
        Key=f'{key}/{created_at.strftime("%Y/%m/%d/%H")}/{created_at.strftime("%M-%S-%f")}.jsonl',
        Body=jsonl
    )


def write_ground_truth(boto_session, ground_truths, path, created_at=datetime.datetime.now(datetime.timezone.utc)):
    s3_client = boto_session.client('s3')
    # input = [{"value":"", "event_id":""}]
    records = []
    for gt in ground_truths:
        records.append(
            json.dumps({
                "groundTruthData": { "data": str(gt['value']), "encoding": "CSV" }, # encoding is irrelevant. Do not pay attention to this arg for 1 value gt
                "eventMetadata": { "eventId": gt['event_id'] },
                "eventVersion": "0"
            })
        )
    jsonl = '\n'.join(records)

    bucket, key = parse_s3_uri(path)
    s3_client.put_object(
        Bucket=bucket,
        Key=f'{key}/{created_at.strftime("%Y/%m/%d/%H")}/{created_at.strftime("%M-%S-%f")}.jsonl',
        Body=jsonl
    )


def parse_s3_uri(s3_uri):
    # s3://bucket-name/path/to/key
    s3_uri = s3_uri.replace('s3://', '')
    bucket, key = s3_uri.split('/', 1)
    return bucket, key


def endpoint_exists(boto_session, endpoint_name):
    sm_client = boto_session.client('sagemaker')
    try:
        sm_client.describe_endpoint(EndpointName=endpoint_name)
        return True
    except sm_client.exceptions.ClientError:
        return False


def endpoint_config_exists(boto_session, config_name):
    sm_client = boto_session.client('sagemaker')
    try:
        sm_client.describe_endpoint_config(EndpointConfigName=config_name)
        return True
    except sm_client.exceptions.ClientError:
        return False


def model_name_exists(boto_session, model_name):
    sm_client = boto_session.client('sagemaker')
    try:
        sm_client.describe_model(ModelName=model_name)
        return True
    except sm_client.exceptions.ClientError:
        return False


def get_existing_endpoint_config(boto_session, config_name):
    sm_client = boto_session.client('sagemaker')
    configs = sm_client.list_endpoint_configs()
    existing_config_names = [c['EndpointConfigName'] for c in configs['EndpointConfigs']]

    if config_name in existing_config_names:
        return sm_client.describe_endpoint_config(EndpointConfigName=config_name)
    else:
        return False


def deploy_or_update_endpoint(boto_session, endpoint_name, endpoint_config_name):
    sm_client = boto_session.client('sagemaker')

    if endpoint_exists(boto_session, endpoint_name):
        sm_client.describe_endpoint(EndpointName=endpoint_name)
        # Endpoint exists — update it
        sm_client.update_endpoint(
            EndpointName=endpoint_name,
            EndpointConfigName=endpoint_config_name
        )
        print(f"Updated existing endpoint: {endpoint_name}")


    try:
        sm_client.describe_endpoint(EndpointName=endpoint_name)
        # Endpoint exists — update it
        sm_client.update_endpoint(
            EndpointName=endpoint_name,
            EndpointConfigName=endpoint_config_name
        )
        print(f"Updated existing endpoint: {endpoint_name}")
    except botocore.exceptions.ClientError:
        # Endpoint doesn't exist — create it
        sm_client.create_endpoint(
            EndpointName=endpoint_name,
            EndpointConfigName=endpoint_config_name
        )
        print(f"Created new endpoint: {endpoint_name}")

def get_model_versions(sm_client, model_package_name, model_package_version):

    groups = sm_client.list_model_package_groups()
    for g in groups['ModelPackageGroupSummaryList']:
        group_name=g['ModelPackageGroupName']
        versions = sm_client.list_model_packages(ModelPackageGroupName=group_name)

        for v in versions['ModelPackageSummaryList']:
            if (v['ModelPackageGroupName'] == model_package_name) & (v['ModelPackageVersion'] == model_package_version):
                return v
    print('No model_version found in registry')
    return None

def get_or_create_model_object_from_registry(sm_client, model_package_name, role, model_package_version='latest'):

    model_package_details = sm_client.list_model_packages(
        ModelPackageGroupName=model_package_name,
        # ModelApprovalStatus='Approved',
        SortBy='CreationTime',
        SortOrder='Descending'
    )
    if not model_package_details:
        print('model package group name does not exist')
        return

    if model_package_version=='latest':
        model_package_version = model_package_details['ModelPackageSummaryList'][0]['ModelPackageVersion']
    elif isinstance(model_package_version, str):
        model_package_version=int(model_package_version)
    
    model_version = get_model_versions(sm_client, model_package_name, model_package_version)
    model_package_arn=model_version['ModelPackageArn']
    if model_version['ModelApprovalStatus'] != 'Approved':
        print('model version not approved')
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


def deploy_model_endpoint(boto_session, model_name, endpoint_name, data_capture_path, instance_type='ml.m5.large'):
    sm_client = boto_session.client('sagemaker')
    # Create or select endpoint
    endpoint_config_name = endpoint_name + "-config"
    if not endpoint_config_exists(boto_session, endpoint_config_name):
        print("creating endpoint config")
        response = sm_client.create_endpoint_config(
            EndpointConfigName=endpoint_config_name,
            ProductionVariants=[
                {
                    'VariantName': 'AllTraffic',
                    'ModelName': model_name,
                    'InstanceType': instance_type,
                    'InitialInstanceCount': 1,
                    'InitialVariantWeight': 1.0
                }
            ],
            DataCaptureConfig={
                'EnableCapture': True,
                'InitialSamplingPercentage': 100,
                'DestinationS3Uri': data_capture_path,
                'CaptureOptions': [
                    {'CaptureMode': 'Input'},
                    {'CaptureMode': 'Output'}
                ]
            }
        )
        while not endpoint_config_exists(boto_session, endpoint_config_name):
            time.sleep(5)
        print(response['EndpointConfigArn'])
    else:
        print(f"using existing endpoint config {endpoint_config_name}")

    # Create or update endpoint
    if endpoint_exists(boto_session, endpoint_name):
        print("updating endpoint")
        response = sm_client.update_endpoint(EndpointName=endpoint_name, EndpointConfigName=endpoint_config_name)
        print(response)
    else:
        print("creating endpoint")
        response = sm_client.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=endpoint_config_name)
        print(response)


# useful before bias monitoring
def decode_ohe(df, original_col, ohe_cols, drop_ohe=True):
    # Find which OHE column is 1 for each row
    df[original_col] = df[ohe_cols].idxmax(axis=1).str.replace(f'{original_col}_', '')
    if drop_ohe:
        df = df.drop(columns=ohe_cols)
    return df

def draw_dag(pipeline_definition):

    definition = json.loads(pipeline_definition)

    G = nx.DiGraph()

    # Add nodes
    for step in definition['Steps']:
        G.add_node(step['Name'])

    # Add edges from DependsOn
    for step in definition['Steps']:
        if 'DependsOn' in step:
            for dep in step['DependsOn']:
                G.add_edge(dep, step['Name'])

    # Also add edges from if_steps/else_steps for ConditionSteps
    for step in definition['Steps']:
        if step.get('Type') == 'Condition':
            for if_step in step.get('IfSteps', []):
                G.add_edge(step['Name'], if_step['Name'])
            for else_step in step.get('ElseSteps', []):
                G.add_edge(step['Name'], else_step['Name'])

    # Draw
    plt.figure(figsize=(12, 8))
    pos = nx.spring_layout(G, seed=42)
    nx.draw(G, pos, with_labels=True, node_color='lightblue',
            node_size=2000, font_size=8, arrows=True,
            arrowsize=20, edge_color='gray')
    plt.title('Pipeline DAG')
    plt.tight_layout()
    plt.savefig('pipeline_dag.png', dpi=150)
    plt.show()


def ensure_lambda_trust_policy(role_arn):
    
    # Get current trust policy
    iam = boto3.client('iam')
    role_name=role_arn.split('/')[-1]
    role = iam.get_role(RoleName=role_name)
    current_policy = role['Role']['AssumeRolePolicyDocument']
    
    # Get existing principals
    existing_services = set()
    for statement in current_policy.get('Statement', []):
        principal = statement.get('Principal', {})
        services = principal.get('Service', [])
        if isinstance(services, str):
            services = [services]  # normalize to list
        existing_services.update(services)
    
    print(f"Existing trusted services: {existing_services}")
    
    # Services we need
    required_services = {
        'sagemaker.amazonaws.com',
        'lambda.amazonaws.com',
        'states.amazonaws.com',
        'events.amazonaws.com'
    }
    
    # Check if all required services already present
    if required_services.issubset(existing_services):
        print(f"Trust policy already up to date for {role_name}")
        return
    
    # Missing services — update policy
    missing = required_services - existing_services
    print(f"Adding missing services: {missing}")
    
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": list(required_services | existing_services)
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    iam.update_assume_role_policy(
        RoleName=role_name,
        PolicyDocument=json.dumps(trust_policy)
    )
    print(f"Trust policy updated for {role_name}")

# Usage
role_arn = 'arn:aws:iam::088461143167:role/SageMakerExecutionRole-1'
role_name = role_arn.split('/')[-1]
ensure_lambda_trust_policy(role_name)