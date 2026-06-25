import boto3, logging, json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def instance_class(instance):
    return instance.split('.')[1]

def instance_size(instance):
    return instance.split('.')[2]

def instance_parse_handler(event, context):
    monitor_instance = event['monitor_instance']
    transform_instance = event['transform_instance']
    endpoint_instance = event['endpoint_instance']

    m_class=instance_class(monitor_instance)
    m_size=instance_size(monitor_instance)
    logger.info(f'monitor_instance: {monitor_instance} => {m_class} & {m_size}')

    t_class=instance_class(transform_instance)
    t_size=instance_size(transform_instance)
    logger.info(f'transform_instance: {transform_instance} => {t_class} & {t_size}')

    e_class=instance_class(endpoint_instance)
    e_size=instance_size(endpoint_instance)
    logger.info(f'endpoint_instance: {endpoint_instance} => {e_class} & {e_size}')

    return {
        'MONITOR_INSTANCE': {
            'CLASS':m_class, 
            'SIZE':m_size
        },
        'TRANSFORM_INSTANCE': {
            'CLASS':t_class, 
            'SIZE':t_size
        },
        'ENDPOINT_INSTANCE': {
            'CLASS':e_class, 
            'SIZE':e_size
        },
    }