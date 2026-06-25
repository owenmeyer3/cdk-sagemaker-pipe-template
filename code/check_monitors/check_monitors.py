import boto3, logging, json

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def data_quality_handler(event, context):
    monitoring_type='DataQuality'

    return {'TYPE': 'DataQuality'}


def model_bias_handler(event, context):
    monitoring_type='ModelBias'

    return {'TYPE': 'ModelBias'}


def model_explainability_handler(event, context):
    monitoring_type='ModelExplainability'

    return {'TYPE': 'ModelExplainability'}


def model_quality_handler(event, context):
    monitoring_type='ModelQuality'

    return {'TYPE': 'ModelQuality'}