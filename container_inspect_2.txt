import logging
import os
import psutil
import socket
import subprocess
import sys
import time
from shutil import copyfile
import json
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)
HADOOP_CONFIG_PATH = '/opt/hadoop-config/'
HADOOP_PATH = '/usr/hadoop-3.0.0'
SPARK_PATH = '/usr/spark-3.3.0'
def copy_cluster_config():
    log.info("Copy cluster config")
    # src = '/tmp/hdfs-site.xml'
    src = os.path.join(HADOOP_CONFIG_PATH, "hdfs-site.xml")
    dst = HADOOP_PATH + '/etc/hadoop/hdfs-site.xml'
    copyfile(src, dst)
    # src= '/tmp/core-site.xml'
    src = os.path.join(HADOOP_CONFIG_PATH, "core-site.xml")
    dst = HADOOP_PATH + '/etc/hadoop/core-site.xml'
    copyfile(src, dst)
    # src= '/tmp/yarn-site.xml'
    src = os.path.join(HADOOP_CONFIG_PATH, "yarn-site.xml")
    dst = HADOOP_PATH + '/etc/hadoop/yarn-site.xml'
    copyfile(src, dst)
    # src= '/tmp/spark-defaults.conf'
    src = os.path.join(HADOOP_CONFIG_PATH, "spark-defaults.conf")
    dst = SPARK_PATH + '/conf/spark-defaults.conf'
    copyfile(src, dst)
def copy_aws_jars():
    log.info("Copy aws jars")
    src = HADOOP_PATH + "/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.199.jar"
    dst = HADOOP_PATH + "/share/hadoop/common/lib/aws-java-sdk-bundle-1.11.199.jar"
    copyfile(src, dst)
    src = HADOOP_PATH + "/share/hadoop/tools/lib/hadoop-aws-3.0.0.jar"
    dst = HADOOP_PATH + "/share/hadoop/common/lib/hadoop-aws-3.0.0.jar"
    copyfile(src, dst)
def get_resource_config():
    resource_config_path = '/opt/ml/config/resourceconfig.json'
    with open(resource_config_path, 'r') as f:
        return json.load(f)
def write_runtime_cluster_config():
    log.info("Write runtime cluster config")
    resource_config = get_resource_config()
    log.info("Resource Config is: {}".format(resource_config))
    master_host = resource_config['hosts'][0]
    master_ip = get_ip_from_host(master_host)
    current_host = resource_config['current_host']
    core_site_file_path = HADOOP_PATH + "/etc/hadoop/core-site.xml"
    yarn_site_file_path = HADOOP_PATH + "/etc/hadoop/yarn-site.xml"
    hadoop_env_file_path = HADOOP_PATH + "/etc/hadoop/hadoop-env.sh"
    yarn_env_file_path = HADOOP_PATH + "/etc/hadoop/yarn-env.sh"
    spark_conf_file_path = SPARK_PATH + "/conf/spark-defaults.conf"
    # add JAVA_HOME to hadoop env
    with open(hadoop_env_file_path, 'a') as hadoop_env_file:
        hadoop_env_file.write("export JAVA_HOME=" + os.environ['JAVA_HOME'] + "\n")
        hadoop_env_file.write("export SPARK_MASTER_HOST=" + master_ip)
        hadoop_env_file.write(
            "export AWS_CONTAINER_CREDENTIALS_RELATIVE_URI=" + os.environ.get('AWS_CONTAINER_CREDENTIALS_RELATIVE_URI',
                                                                              '') + "\n")
    # add YARN log directory
    with open(yarn_env_file_path, 'a') as yarn_env_file:
        yarn_env_file.write("export YARN_LOG_DIR=/var/log/yarn/")
    # configure ip address for name node
    with open(core_site_file_path, 'r') as core_file:
        file_data = core_file.read()
    file_data = file_data.replace('nn_uri', master_ip)
    with open(core_site_file_path, 'w') as core_file:
        core_file.write(file_data)
    # configure hostname for RM and NM
    with open(yarn_site_file_path, 'r') as yarn_file:
        file_data = yarn_file.read()
    file_data = file_data.replace('rm_hostname', master_ip)
    file_data = file_data.replace('nm_hostname', current_host)
    with open(yarn_site_file_path, 'w') as yarn_file:
        yarn_file.write(file_data)
    # configure yarn resource limitation
    mem = int(psutil.virtual_memory().total / (1024 * 1024))  # total physical memory in mb
    cores = psutil.cpu_count(logical=True)  # vCPUs
    minimum_allocation_mb = '1'
    maximum_allocation_mb = str(mem)
    minimum_allocation_vcores = '1'
    maximum_allocation_vcores = str(cores)
    # add some residual in memory due to rounding in memory allocation
    memory_mb_total = str(mem + 2048)
    # virtualized 32x cores to ensure core allocations
    cpu_vcores_total = str(cores * 16)
    with open(yarn_site_file_path, 'r') as yarn_file:
        file_data = yarn_file.read()
    file_data = file_data.replace('minimum_allocation_mb', minimum_allocation_mb)
    file_data = file_data.replace('maximum_allocation_mb', maximum_allocation_mb)
    file_data = file_data.replace('minimum_allocation_vcores', minimum_allocation_vcores)
    file_data = file_data.replace('maximum_allocation_vcores', maximum_allocation_vcores)
    file_data = file_data.replace('memory_mb_total', memory_mb_total)
    file_data = file_data.replace('cpu_vcores_total', cpu_vcores_total)
    with open(yarn_site_file_path, 'w') as yarn_file:
        yarn_file.write(file_data)
    # configure Spark defaults
    with open(spark_conf_file_path, 'r') as spark_file:
        file_data = spark_file.read()
    file_data = file_data.replace('sd_host', master_ip)
    file_data = file_data.replace('exec_mem', str(int((mem / 3) * 2.2)) + 'm')
    file_data = file_data.replace('exec_cores', str(min(5, cores - 1)))
    file_data = file_data.replace('num_exec', '40')
    file_data = file_data.replace('driver_mem', '3g')
    with open(spark_conf_file_path, 'w') as spark_file:
        spark_file.write(file_data)
    log.info("Finished Yarn configuration files setup.")
def start_daemons():
    resource_config = get_resource_config()
    master_host = resource_config['hosts'][0]
    current_host = resource_config['current_host']
    cmd_hdfs_namenode_format = HADOOP_PATH + '/bin/hdfs namenode -format -force'
    cmd_hdfs_start_namenode = HADOOP_PATH + '/bin/hdfs --daemon start namenode'
    cmd_hdfs_start_datanode = HADOOP_PATH + '/bin/hdfs --daemon start datanode'
    cmd_yarn_start_resourcemanager = HADOOP_PATH + '/bin/yarn --daemon start resourcemanager'
    cmd_yarn_start_nodemanager = HADOOP_PATH + '/bin/yarn --daemon start nodemanager'
    cmd_yarn_start_proxyserver = HADOOP_PATH + '/bin/yarn --daemon start proxyserver'
    if current_host == master_host:
        log.info("Starting spark process for master node {}".format(current_host))
        log.info("Running command: {}".format(cmd_hdfs_namenode_format))
        hdfs_namenode_format_status = subprocess.run(cmd_hdfs_namenode_format, shell=True)
        if is_command_failed(hdfs_namenode_format_status):
            log.info("Failed to run {}, return code {}".format(cmd_hdfs_namenode_format, hdfs_namenode_format_status.returncode))
            # Not sure if we should exit if hdfs namenode -format fails
            # sys.exit(255)
        log.info("Running command: {}".format(cmd_hdfs_start_namenode))
        hdfs_start_namenode_status = subprocess.run(cmd_hdfs_start_namenode, shell=True)
        if is_command_failed(hdfs_start_namenode_status):
            log.info("Failed to run {}, return code {}".format(cmd_hdfs_start_namenode, hdfs_start_namenode_status.returncode))
            # Not sure if we should exit if hdfs start namenode fails
            # sys.exit(255)
        log.info("Running command: {}".format(cmd_hdfs_start_datanode))
        hdfs_start_datanode_status = subprocess.run(cmd_hdfs_start_datanode, shell=True)
        if is_command_failed(hdfs_start_datanode_status):
            log.info("Failed to run {}, return code {}".format(cmd_hdfs_start_datanode, hdfs_start_datanode_status.returncode))
            # Not sure if we should exit if hdfs start datanode fails
            # sys.exit(255)
        log.info("Running command: {}".format(cmd_yarn_start_resourcemanager))
        yarn_start_resourcemanager_status = subprocess.run(cmd_yarn_start_resourcemanager, shell=True)
        if is_command_failed(yarn_start_resourcemanager_status):
            log.info("Failed to run {}, return code {}".format(cmd_yarn_start_resourcemanager, yarn_start_resourcemanager_status.returncode))
            # Not sure if we should exit if yarn start resourcemanager fails
            # sys.exit(255)
        log.info("Running command: {}".format(cmd_yarn_start_nodemanager))
        yarn_start_nodemanager_status = subprocess.run(cmd_yarn_start_nodemanager, shell=True)
        if is_command_failed(yarn_start_nodemanager_status):
            log.info("Failed to run {}, return code {}".format(cmd_yarn_start_nodemanager, yarn_start_nodemanager_status.returncode))
            # Not sure if we should exit if yarn start nodemanager fails
            # sys.exit(255)
        log.info("Running command: {}".format(cmd_yarn_start_proxyserver))
        yarn_start_proxyserver_status = subprocess.run(cmd_yarn_start_proxyserver, shell=True)
        if is_command_failed(yarn_start_proxyserver_status):
            log.info("Failed to run {}, return code {}".format(cmd_yarn_start_proxyserver, yarn_start_proxyserver_status.returncode))
            # Not sure if we should exit if yarn start proxyserver fails
            # sys.exit(255)
    else:
        log.info("Starting spark process for worker node {}".format(current_host))
        log.info("Running command: {}".format(cmd_hdfs_start_datanode))
        hdfs_start_datanode_status = subprocess.run(cmd_hdfs_start_datanode, shell=True)
        if is_command_failed(hdfs_start_datanode_status):
            log.info("Failed to run {}, return code {}".format(cmd_hdfs_start_datanode, hdfs_start_datanode_status.returncode))
            # Not sure if we should exit if hdfs start datanode fails
            # sys.exit(255)
        log.info("Running command: {}".format(cmd_yarn_start_nodemanager))
        yarn_start_nodemanager_status = subprocess.run(cmd_yarn_start_nodemanager, shell=True)
        if is_command_failed(yarn_start_nodemanager_status):
            log.info("Failed to run {}, return code {}".format(cmd_yarn_start_nodemanager, yarn_start_nodemanager_status.returncode))
            # Not sure if we should exit if yarn start nodemanager fails
            # sys.exit(255)
def is_command_failed(command_status):
    return command_status is not None and command_status.returncode != 0
def get_ip_from_host(host_name):
    IP_WAIT_TIME = 300
    counter = 0
    ip = ''
    while counter < IP_WAIT_TIME and ip == '':
        try:
            # TODO: Remove this hardcoding once analytics job has the resource config setup
            ip = socket.gethostbyname(host_name)
            # ip = '127.0.0.1'
            break
        except:
            counter += 1
            time.sleep(1)
    if counter == IP_WAIT_TIME and ip == '':
        raise Exception("Network issue happened. Cannot retrieve ip address in past 5 minutes")
    return ip
---
import json
class AnalyticsInput:
    """
    Input class for data analyzer
    """
    def __init__(self,
                 dataset_source, dataset_format, output_path,
                 analysis_type, problem_type,
                 inference_attribute = None, probability_attribute = None, ground_truth_attribute = None, probability_threshold_attribute = None,
                 positive_label = None, cloudwatch_metrics_directory = None, output_message_file = None,
                 start_time = None, end_time = None, metric_time = None,
                 record_preprocessor_script = None,
                 post_analytics_processor_script = None, baseline_constraints = None,
                 baseline_statistics = None,
                 publish_cloudwatch_metrics = None, sagemaker_endpoint_name = None,
                 sagemaker_monitoring_schedule_name = None,
                 detect_outliers = None, detect_drift = None, image_data = None,
                 report_enabled = False, auto_ml_job_detail = None, monitoring_input_type = None,
                 data_quality_monitoring_config = None, exclude_features_attribute=None):
        self.dataset_source = dataset_source
        self.dataset_format = dataset_format
        self.output_path = output_path
        self.monitoring_input_type = monitoring_input_type
        self.analysis_type = analysis_type
        self.problem_type = problem_type
        self.inference_attribute = inference_attribute
        self.probability_attribute = probability_attribute
        self.ground_truth_attribute = ground_truth_attribute
        self.probability_threshold_attribute = probability_threshold_attribute
        self.positive_label = positive_label
        self.exclude_features_attribute = exclude_features_attribute
        self.record_preprocessor_script = record_preprocessor_script
        self.post_analytics_processor_script = post_analytics_processor_script
        self.baseline_constraints = baseline_constraints
        self.baseline_statistics = baseline_statistics
        self.data_quality_monitoring_config = data_quality_monitoring_config
        self.start_time = start_time
        self.end_time = end_time
        self.metric_time = metric_time
        self.cloudwatch_metrics_directory = cloudwatch_metrics_directory
        self.publish_cloudwatch_metrics = publish_cloudwatch_metrics
        self.sagemaker_endpoint_name = sagemaker_endpoint_name
        self.sagemaker_monitoring_schedule_name = sagemaker_monitoring_schedule_name
        self.output_message_file = output_message_file
        # Image analysis inputs
        self.detect_outliers = detect_outliers
        self.detect_drift = detect_drift
        self.image_data = image_data
        self.report_enabled = report_enabled
        self.auto_ml_job_detail = auto_ml_job_detail
    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__)
class DatasetFormat:
    def __init__(self, csv=None, json=None, captured=None, parquet=None):
        if csv is None and json is None and captured is None and parquet is None:
            raise ValueError('Dataset format is missing')
        self.csv = csv
        self.json = json
        self.captured = captured
        self.parquet = parquet
class CSV:
    default_header = False
    allowed_positions = ['START', 'END']
    default_record_delimiter = '\n'
    default_field_delimiter = ','
    default_output_columns_position = 'START'
    def __init__(self, header = None, output_columns_position = None, record_delimiter = None, field_delimiter = None, quote_character = None,
                 quote_escape_character = None):
        if output_columns_position is not None and output_columns_position not in self.allowed_positions:
            raise ValueError('Invalid value for output_columns_position')
        self.header = self.default_header if header is None else header
        self.output_columns_position = self.default_output_columns_position if output_columns_position is None else output_columns_position
        self.record_delimiter = self.default_record_delimiter if record_delimiter is None else record_delimiter
        self.field_delimiter = self.default_field_delimiter if field_delimiter is None else field_delimiter
        self.quote_character = quote_character
        self.quote_escape_character = quote_escape_character
class JSON:
    default_record_delimiter = '\n'
    def __init__(self, record_delimiter = None):
        self.record_delimiter = self.default_record_delimiter if record_delimiter is None else record_delimiter
class Captured:
    default_captured_index_names = ['endpointInput', 'endpointOutput']
    def __init__(self, captured_index_names = None):
        self.captured_index_names = self.default_captured_index_names if captured_index_names is None else captured_index_names
class DataQualityDistributionConstraints:
    """Class to define the distribution_constraints that is used to override the
    distribution_constraint object values in DataQualityMonitoringConfig object.
    """
    default_perform_comparison = 'Enabled'
    default_comparison_threshold = 0.1
    default_comparison_method = 'Robust'
    default_categorical_comparison_threshold = 0.1
    default_categorical_drift_method = 'LInfinity'
    def __init__(self, perform_comparison=None, comparison_threshold=None, comparison_method=None,
                 categorical_comparison_threshold=None, categorical_drift_method=None):
        self.perform_comparison = self.default_perform_comparison if perform_comparison is None else perform_comparison
        self.comparison_threshold = self.default_comparison_threshold if comparison_threshold is None else comparison_threshold
        self.comparison_method = self.default_comparison_method if comparison_method is None else comparison_method
        self.categorical_comparison_threshold = self.default_categorical_comparison_threshold if categorical_comparison_threshold is None else categorical_comparison_threshold
        self.categorical_drift_method = self.default_categorical_drift_method if categorical_drift_method is None else categorical_drift_method
class DataQualityMonitoringConfig:
    """Class to define the monitoring_config object that is used to override the default
    global monitoring_config values in constraints suggested by service.
    """
    default_evaluate_constraints = 'Enabled'
    default_emit_metrics = 'Enabled'
    default_datatype_check_threshold = 1.0
    default_domain_content_threshold = 1.0
    default_distribution_constraints = DataQualityDistributionConstraints()
    def __init__(self, evaluate_constraints=None, emit_metrics=None, datatype_check_threshold=None,
                 domain_content_threshold=None, distribution_constraints=None):
        self.evaluate_constraints = self.default_evaluate_constraints if evaluate_constraints is None else evaluate_constraints
        self.emit_metrics = self.default_emit_metrics if emit_metrics is None else emit_metrics
        self.datatype_check_threshold = self.default_datatype_check_threshold if datatype_check_threshold is None else datatype_check_threshold
        self.domain_content_threshold = self.default_domain_content_threshold if domain_content_threshold is None else domain_content_threshold
        self.distribution_constraints = self.default_distribution_constraints if distribution_constraints is None else distribution_constraints
---
#!/usr/bin/env python3
import logging
import os
import sys
from importlib import util
from captured_record import CapturedData
import json
import inspect
_EVENT_METADATA_FIELD = "eventMetadata"
_INVOCATION_SOURCE_FIELD = "invocationSource"
_SHADOW_INVOCATION_SOURCE_NAME = "ShadowExperiment"
class PreProcessorWrapper:
    """Wrapper class which provides ability to invoke the customer provided script.
    This is used by the Scala portion of this container to transform a record with a customer provided
    preprocessor script which conforms to a particular signature.
    
    The interfaces for the custom_preprocess_handler that we officially accept are:
    
    def preprocess_handler(inference_record):
        pass
    
    def preprocess_handler(inference_record, logger):
        pass
    """
    def __init__(self, preprocessor_script_path, dataset_format, logger):
        self._custom_preprocess_handler = self._import_handler(preprocessor_script_path)
        self._custom_preprocess_handler_signatures = inspect.signature(self._custom_preprocess_handler).parameters
        
        self._dataset_format = dataset_format
        self._logger = logger
    def _import_handler(self, preprocessor_script_path):
        """Discover the customer provided contract from  preprocessor_script_path and return the reference """
        _custom_preprocess_handler = None
        if preprocessor_script_path is None:
            # Todo: In addition introduce an inspection API which will validate the
            # pp code instead of doing this at runtime
            self._logger.info("No pre-processor code provided.")
            return None
        
        # inspect and load customer script
        spec = util.spec_from_file_location('preprocessor', preprocessor_script_path)
        preprocessor = util.module_from_spec(spec)
        spec.loader.exec_module(preprocessor)
        if hasattr(preprocessor, 'preprocess_handler'):
            _custom_preprocess_handler = preprocessor.preprocess_handler
        else:
            raise NotImplementedError('Preprocess Handler not implemented correctly in customer script.')
        return _custom_preprocess_handler
    def perform_preprocess(self, inference_event_record_string):
        """Wrapper which invokes the customer provided preprocess implementation
        Arguments:
            inference_event_record_string -- a string version of the inference record. In case of a SageMaker
                captured record format, this would be the full captured record including metadata and data
        Returns:
            Serialized JSON formatted string of the dictionary returned by the customer code
        """
        # convert the json into a python object
        inference_record = CapturedData(inference_event_record_string)
        # call custom preprocessor to do the transformation
        # to be backward compatible, we check the signatures of the handler accepts logger
        if 'logger' in self._custom_preprocess_handler_signatures:
            tranformed_record = self._custom_preprocess_handler(inference_record, self._logger)
        else:
            tranformed_record = self._custom_preprocess_handler(inference_record)
        # TODO: validate if tranformed_record is a proper dictionary
        return json.dumps(tranformed_record)
    def perform_raw_preprocess(self, raw_input):
        # to be backward compatible, we check the signatures of the handler accepts logger
        if 'logger' in self._custom_preprocess_handler_signatures:
            transformed_record = self._custom_preprocess_handler(raw_input, self._logger)
        else:
            transformed_record = self._custom_preprocess_handler(raw_input)
        return json.dumps(transformed_record)
# Instantiate the wrapper and invoke the preprocessor
record_format = os.getenv('RECORD_FORMAT')
logging.basicConfig(filename=os.getenv('PREPROCESSING_LOG'), level=logging.DEBUG)
preprocessing_logger = logging.getLogger('Preprocessing')
pp = PreProcessorWrapper(os.getenv('PREPROCESSOR_SCRIPT_PATH'), record_format, preprocessing_logger)
exception_message = "There is an exception raised in preprocessor script. Please double check the preprocessor script."
for line in sys.stdin:
    if record_format == 'SAGEMAKER_CAPTURE_JSON':
        inference_event_record = json.loads(line)
        # Continue to the next line if the record is from a shadow variant
        if inference_event_record.get(_EVENT_METADATA_FIELD, {}).get(_INVOCATION_SOURCE_FIELD) == _SHADOW_INVOCATION_SOURCE_NAME:
            continue
        
        try :
            return_data = pp.perform_preprocess(inference_event_record)
            # DO NOT CHANGE THIS PRINT STATEMENT - return value has to to go stdout
            print(return_data)
        except Exception as ex:
            preprocessing_logger.error(f'Caught exception in preprocessor script: {ex}')
            raise Exception(exception_message)
    elif record_format == "CSV":
        try:
            return_data = pp.perform_raw_preprocess(line)
            # DO NOT CHANGE THIS PRINT STATEMENT - return value has to to go stdout
            print(return_data)
        except Exception as ex:
            preprocessing_logger.error(f'Caught exception in preprocessor script: {ex}')
            raise Exception(exception_message)
    else:
        json_record = json.loads(line)
        try:
            return_data = pp.perform_raw_preprocess(json_record)
            # DO NOT CHANGE THIS PRINT STATEMENT - return value has to to go stdout
            print(return_data)
        except Exception as ex:
            preprocessing_logger.error(f'Caught exception in preprocessor script: {ex}')
            raise Exception(exception_message)
---
import bootstrap
import logging
import subprocess
import sys
import time
from importlib import util
from .data_analyzer import DataAnalyzer
from .report import ReportGenerator
class DefaultDataAnalyzer(DataAnalyzer):
    logger = logging.getLogger("DefaultDataAnalyzer")
    application_jar = '/opt/amazon/sagemaker-data-analyzer-1.0-jar-with-dependencies.jar'
    spark_job_config_path = '/tmp/spark_job_config.json'
    def bootstrap_yarn(self):
        DefaultDataAnalyzer.logger.info("Bootstrapping yarn")
        bootstrap.copy_aws_jars()
        bootstrap.copy_cluster_config()
        bootstrap.write_runtime_cluster_config()
        bootstrap.start_daemons()
    def write_json_file(self, spark_job_config_path, analytics_input):
        with open(spark_job_config_path, 'w') as file:
            self.logger.debug(f"Writing Analytics Input to {spark_job_config_path} - {analytics_input.toJSON()}")
            file.write(analytics_input.toJSON())
    def spark_submit(self, analytics_input):
        self.write_json_file(self.spark_job_config_path, analytics_input)
        input_path_arg = "--analytics_input " + self.spark_job_config_path
        cmd = ['bin/spark-submit',
               '--master',
               'yarn',
               '--deploy-mode',
               'client',
               '--conf',
               'spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
               '--conf',
               'spark.serializer=org.apache.spark.serializer.KryoSerializer'
               ]
        cmd.extend([self.application_jar, input_path_arg])
        cmd_string = " ".join(cmd)
        DefaultDataAnalyzer.logger.info("Running command: {}".format(cmd_string))
        completed_process = subprocess.run(cmd_string, check=True, shell=True)
        # subprocess.run() does not raise an exception if the underlying process errors
        DefaultDataAnalyzer.logger.info(
            "Completed spark-submit with return code : {}".format(completed_process.returncode))
        if completed_process.returncode != 0:
            raise Exception('Spark job to perform the analysis of your data failed with return code : {}. '
                            'Please check the CloudWatch logs for the ProcessingJob for more details.'
                            .format(completed_process.returncode))
    def _import_post_proc_script(self, post_analytics_processor_script):
        # inspect and load customer script
        spec = util.spec_from_file_location('postprocessor', post_analytics_processor_script)
        postprocessor = util.module_from_spec(spec)
        spec.loader.exec_module(postprocessor)
        if not hasattr(postprocessor, 'postprocess_handler'):
            # TODO: Convert this to correct exception, write to message file
            raise NotImplementedError('PostProcess Handler not implemented correctly in customer analysis script.')
        return postprocessor.postprocess_handler
    def validate_preprocessor_script(self, preprocessor_script_path, dataset_format):
        format_keys = [k for k in list(dataset_format.keys()) if k != 'compression']
        if len(format_keys) == 0:
            raise NotImplementedError("Unable to run preprocessor script. No supported format specified.")
        elif len(format_keys) == 1:
            if format_keys[0] not in ['sagemakerCaptureJson', 'sagemakerMergeJson', 'csv', 'json']:
                raise NotImplementedError("Preprocessor script not supported for format {}".format(format_keys[0]))
        else:
            raise NotImplementedError(
                "Unable to run preprocessor script. Multiple formats specified {}".format(format_keys.join(","))
            )
        
        spec = util.spec_from_file_location('preprocessor', preprocessor_script_path)
        preprocessor = util.module_from_spec(spec)
        spec.loader.exec_module(preprocessor)
        if not hasattr(preprocessor, 'preprocess_handler'):
            raise NotImplementedError('Preprocess Handler not implemented correctly in customer script.')
    def exit_signal(self, signal_number, frame):
        DefaultDataAnalyzer.logger.info("Received SIGTERM, exiting.")
        sys.exit()
    def perform_analysis(self, analytics_input):
        DefaultDataAnalyzer.logger.info("Performing analysis with input: {}".format(analytics_input.toJSON()))
        if analytics_input.record_preprocessor_script is not None:
            self.validate_preprocessor_script(analytics_input.record_preprocessor_script, 
                                              analytics_input.dataset_format)
        postproc_handler = None
        postproc_script = analytics_input.post_analytics_processor_script
        if postproc_script is not None:
            postproc_handler = self._import_post_proc_script(postproc_script)
        self.bootstrap_yarn()
        resource_config = bootstrap.get_resource_config()
        master_host = resource_config['hosts'][0]
        current_host = resource_config['current_host']
        total_hosts = len(resource_config['hosts'])
        DefaultDataAnalyzer.logger.info("Total number of hosts in the cluster: {}".format(str(total_hosts)))
        if current_host == master_host:
            time.sleep(10)
            # Kick off the Spark job
            self.spark_submit(analytics_input)
            # give the control to the post proc script if available
            if postproc_handler is not None:
                DefaultDataAnalyzer.logger.info("Invoking customer's post-processing script...")
                postproc_handler()
            if analytics_input.report_enabled:
                report_generator = ReportGenerator(analytics_input)
                report_generator.generate_model_quality_report()
                report_generator.save_reports(analytics_input.output_path)
            if total_hosts == 1:
                DefaultDataAnalyzer.logger.info("Spark job completed.")
                sys.exit(0)
            cmd_hdfs_mkdir = bootstrap.HADOOP_PATH + '/bin/hdfs dfs -mkdir -p /sagemaker'
            cmd_hdfs_touchz = bootstrap.HADOOP_PATH + '/bin/hdfs dfs -touchz /sagemaker/end_of_job'
            # Create a hdfs directory
            DefaultDataAnalyzer.logger.info("Running command: {}".format(cmd_hdfs_mkdir))
            hdfs_mkdir_status = subprocess.run(cmd_hdfs_mkdir, shell=True)
            if hdfs_mkdir_status is not None and  hdfs_mkdir_status.returncode == 0:
                # Create an end of job marker file
                DefaultDataAnalyzer.logger.info("Running command: {}".format(cmd_hdfs_touchz))
                hdfs_touchz_status = subprocess.run(cmd_hdfs_touchz, shell=True)
                if hdfs_touchz_status is not None and hdfs_touchz_status.returncode != 0:
                    DefaultDataAnalyzer.logger.info("Failed to write end of job marker file, command: {}, return code {}, error {}".format(cmd_hdfs_touchz, hdfs_touchz_status.returncode, hdfs_touchz_status.stderr))
                    # Fail fast, but don't fail the master as spark has already successfully completed.
                    sys.exit(0)
            else:
                DefaultDataAnalyzer.logger.info("Failed to create hdfs directory, command: {}, return code {}, error {}".format(cmd_hdfs_mkdir, hdfs_mkdir_status.returncode, hdfs_mkdir_status.stderr))
                # Fail fast, but don't fail the master as spark has already successfully completed.
                sys.exit(0)
            DefaultDataAnalyzer.logger.info("Spark job completed. Waiting for workers (if any) to exit.")
            # Sleeping 65 seconds, waiting for workers to exit
            time.sleep(65)
            sys.exit(0)
        else:
            DefaultDataAnalyzer.logger.info("Worker {} waiting for end of job signal.".format(current_host))
            cmd_hdfs_get = bootstrap.HADOOP_PATH + '/bin/hdfs dfs -get /sagemaker/end_of_job local_eoj'
            # Poll every 20 seconds to check the existence of the end of job marker file.
            while True:
                DefaultDataAnalyzer.logger.info("Running command: {}".format(cmd_hdfs_get))
                hdfs_get_status = subprocess.run(cmd_hdfs_get, shell=True)
                DefaultDataAnalyzer.logger.info("Return value from command: {} is {}".format(cmd_hdfs_get, hdfs_get_status.returncode))
                if hdfs_get_status is not None and hdfs_get_status.returncode == 0:
                    DefaultDataAnalyzer.logger.info("Worker {} received end of job signal.".format(current_host))
                    break
                time.sleep(20)
            DefaultDataAnalyzer.logger.info("Worker {} done.".format(current_host))
            sys.exit(0)
    def __init__(self):
        super().__init__()
