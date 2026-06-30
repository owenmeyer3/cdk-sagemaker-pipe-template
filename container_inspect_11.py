# response = sm_client.create_processing_job(
#     ProcessingJobName='inspect-model-monitor-container-11',
#     AppSpecification={
#         'ImageUri': '156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer',
#         'ContainerEntrypoint': ['/bin/bash', '-c'],
#         'ContainerArguments': [
#             'cat /opt/amazon/program/analysis/report.py  2>/dev/null'
#         ]
#     },
#     ProcessingResources={
#         'ClusterConfig': {
#             'InstanceCount': 1,
#             'InstanceType': 'ml.m5.large',
#             'VolumeSizeInGB': 20
#         }
#     },
#     ProcessingOutputConfig={
#         'Outputs': [{
#             'OutputName': 'inspection_output',
#             'S3Output': {
#                 'S3Uri': 's3://omm-test-bucket/pipelines/abalone/container-inspect',
#                 'LocalPath': '/opt/ml/processing/output',
#                 'S3UploadMode': 'EndOfJob'
#             }
#         }]
#     },
#     RoleArn="arn:aws:iam::088461143167:role/SageMakerExecutionRole-1",
#     StoppingCondition={'MaxRuntimeInSeconds': 300}
# )


"""
Generating notebook reports from analysis results
"""
import base64
import enum
import heapq
import itertools
import logging
import math
import os
from subprocess import check_call, CalledProcessError
from typing import Any, List, AnyStr
import nbformat
import pkg_resources
from nbformat.v4 import new_code_cell, new_markdown_cell, new_notebook
import warnings
from jinja2 import Environment, select_autoescape, FileSystemLoader, StrictUndefined
import json
import numpy as np
import matplotlib.pyplot as plt
from io import BytesIO
from .analytics_input import AnalyticsInput
# Suppress warnings from jupyter/matplotlib
warnings.filterwarnings("ignore")
logger = logging.getLogger(__name__)
_REPORT_TEMPLATES_PATH = pkg_resources.resource_filename(__name__, 'notebook_templates/report')
_STATS_FILENAME = 'statistics.json'
_IMG_HTML_PLACEHOLDER = "<img src='data:image/png;base64,{encoded_img}'>"
class CellType(enum.Enum):
    CODE = "code"
    DOC = "doc"
class CellTemplate:
    def __init__(self, cell_type, content):
        self.cell_type = cell_type
        self.content = content
class ReportEnvParam(enum.Enum):
    AUTO_ML_CANDIDATE_NAME = "auto_ml_candidate_name"
    AUTO_ML_JOB_NAME = "auto_ml_job_name"
    AUTO_ML_JOB_OBJECTIVE_TYPE = "auto_ml_job_objective_type"
    AUTO_ML_OBJECTIVE_METRIC_NAME = "objective_metric_name"
class ProblemType(enum.Enum):
    BINARY_CLASSIFICATION = "BinaryClassification"
    MULTICLASS_CLASSIFICATION = "MulticlassClassification"
    REGRESSION = "Regression"
class AutoMlJobDetail:
    def __init__(
            self,
            candidate_name: str,
            job_name: str,
            objective_metric_name: str,
            objective_type: str,
            problem_type: str,
            automl_problem_name: str
    ):
        self.candidate_name = candidate_name
        self.job_name = job_name
        self.objective_metric_name = objective_metric_name
        self.objective_type = objective_type
        self.problem_type = problem_type
        self.automl_problem_name = automl_problem_name
class ReportGenerator:
    def __init__(
            self,
            analysis_input: AnalyticsInput,
            name: str = None,
            title: str = None,
    ):
        """
        Constructor
        @param analysis_input: AnalysisInput object from Model Monitor input
        @param name: File name for the report file.
        @param title: Title string used for the report
        @param path: Path for saving the files for the job artifacts
        """
        self.analysis_input = analysis_input
        self.name = name if name else "report"
        self.title = title if title else "SageMaker Model Quality Report"
        self.notebook = new_notebook()
        self._init_notebook(self.title)
        self._auto_ml_job_detail = analysis_input.auto_ml_job_detail
        self._environment = Environment(
            autoescape=select_autoescape(['html', 'htm', 'xml']),
            loader=FileSystemLoader(searchpath=_REPORT_TEMPLATES_PATH),
            trim_blocks=True,
            lstrip_blocks=True,
            undefined=StrictUndefined)
    def _add_cells(self, cells):
        for cell in cells:
            if cell is None:
                continue
            if cell.cell_type is CellType.CODE:
                notebook_cell = new_code_cell(cell.content)
            elif cell.cell_type is CellType.DOC:
                notebook_cell = new_markdown_cell(cell.content)
            else:
                raise ValueError(f"Cell type {cell.cell_type} not defined")
            self.notebook.cells.append(notebook_cell)
    def _init_notebook(self, title):
        self.notebook.metadata["kernelspec"] = {
            "display_name": "conda_python3",
            "language": "python",
            "name": "conda_python3",
        }
        self._add_cells([self._display_markdown(f"# {title}")])
    def save_reports(self, path: str, name: str = None) -> None:
        """
        generates report in jupyter notebook (ipynb), html and pdf formats sequentially
        :param path: output path where reports are saved
        :param name: file name to be used for reports
        :return: None
        """
        if not path:
            raise ValueError("report generation path not specified.")
        report_name = name if name else self.name
        if path:
            os.makedirs(path, exist_ok=True)
        nb_report_path = os.path.join(path, f"{report_name}.ipynb")
        try:
            # Generate Jupyter Notebook
            with open(nb_report_path, "w", encoding="utf-8") as f:
                nbformat.write(self.notebook, f)
            try:
                # Generate HTML Report
                html_report = os.path.join(path, f"{report_name}.html")
                # output refers to the ipynb directory, must not contain additional paths.
                nbconvert_command = f"jupyter nbconvert --to html --template classic" \
                                    f" --output {html_report} {nb_report_path}"
                logger.info(nbconvert_command)
                check_call([nbconvert_command], shell=True)
                logger.info(f"HTML report '{os.path.join(path, html_report)}' generated successfully.")
            except CalledProcessError as e:
                # Do not fail if HTML not generated, just log the exception
                logger.exception("Failed to generate HTML report")
            try:
                # Generate PDF Report
                pdf_report_path = os.path.join(path, f"{report_name}.pdf")
                html_report_path = os.path.join(path, f"{report_name}.html")
                custom_css_path = os.path.join(_REPORT_TEMPLATES_PATH, "custom.css")
                html_to_pdf_command = f"wkhtmltopdf --javascript-delay 2000 --print-media-type " \
                                      f"--encoding UTF-8 --dpi 300 --user-style-sheet " \
                                      f"file://{custom_css_path} {html_report_path} {pdf_report_path}"
                logger.info(html_to_pdf_command)
                check_call([html_to_pdf_command], shell=True)
                logger.info(f"PDF report '{pdf_report_path}' generated successfully.")
            except CalledProcessError as e:
                # Do not fail if PDF not generated, just log the exception
                logger.exception("Failed to generate PDF report")
        finally:
            if os.path.exists(nb_report_path):
                os.remove(nb_report_path)
    # Step Methods
    @staticmethod
    def _display_markdown(content=None):
        return CellTemplate(CellType.DOC, content)
    @staticmethod
    def _display_code(content=None):
        return CellTemplate(CellType.CODE, content)
    @staticmethod
    def _convert_confusion_matrix_to_numpy_array(stats_confusion_matrix, max_label_count=15) -> (np.array, List):
        """
        This method converts the confusion matrix from statistics.json format to numpy array. If the number of labels
        are more than max_label_count it only shows top max_label_count confused labels which are outside of diagonal
        of original matrix.
        """
        labels = list(stats_confusion_matrix.keys())
        max_label_count = min(len(labels), max_label_count)
        # Handle edge case if matrix only includes one label
        if len(labels) == 1:
            return labels, np.array([[stats_confusion_matrix[labels[0]][labels[0]]]])
        confusion_heap = []
        for label in labels:
            for prediction_label in labels:
                # Select confusions outside of diagonal
                if label != prediction_label:
                    heapq.heappush(confusion_heap,
                                   (-1 * stats_confusion_matrix[label][prediction_label], label, prediction_label))
        selected_labels = set()
        while len(selected_labels) < max_label_count:
            confusion = heapq.heappop(confusion_heap)
            selected_labels.update([confusion[1], confusion[2]])
        selected_labels = sorted(selected_labels)
        confusion_matrix = []
        for label in selected_labels:
            confusion_matrix.append([stats_confusion_matrix[label][pred_label] for pred_label in selected_labels])
        return np.array(confusion_matrix), selected_labels
    @staticmethod
    def _display_confusion_matrix(stats_confusion_matrix):
        cm_raw, labels = ReportGenerator._convert_confusion_matrix_to_numpy_array(stats_confusion_matrix)
        cmap = plt.get_cmap('Blues')
        cm =  cm_raw.astype('float') / cm_raw.sum()
        fig, ax = plt.subplots(figsize=(15, 15))
        cax = ax.imshow(cm_raw, interpolation='nearest', cmap=cmap)
        ax.set_title("Confusion Matrix")
        cbar = fig.colorbar(cax, shrink=0.6, aspect=50)
        tick_marks = np.arange(len(labels))
        # truncate with ... for labels with length more than 17 (total of 20 chars)
        limit = 17
        labels = list(map(lambda label: label[:limit] + '...' * (len(label) > limit), labels))
        plt.xticks(tick_marks, labels, rotation=90)
        plt.yticks(tick_marks, labels)
        thresh = cm.max() / 1.5
        for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
            if math.isnan(cm[i, j]):
                plt.text(j, i, "Nan", horizontalalignment="center", color="black")
            else:
                plt.text(j, i, "{0}\n({1:.2%})".format(cm_raw[i, j], cm[i, j]),
                         horizontalalignment="center",
                         color="white" if cm[i, j] > thresh else "black")
        plt.tight_layout()
        plt.ylabel('Actual label')
        plt.xlabel('Predicted label')
        tmp_file = BytesIO()
        fig.savefig(tmp_file, format="png", bbox_inches="tight")
        tmp_file.seek(0)
        encoded = base64.b64encode(tmp_file.getvalue()).decode("utf-8")
        return _IMG_HTML_PLACEHOLDER.format(encoded_img=encoded)
    @staticmethod
    def _display_roc_curve(false_positive_rate, true_positive_rate, auc):
        fig = plt.figure()
        plt.title('Receiver Operating Characteristic (ROC)')
        plt.plot(false_positive_rate, true_positive_rate, 'b', label=f'ROC Curve (AUC = {auc:0.2f})')
        plt.legend(loc='lower right')
        plt.plot([0, 1], [0, 1], 'r--')
        plt.xlim([0, 1])
        plt.ylim([0, 1.05])
        plt.ylabel('True Positive Rate')
        plt.xlabel('False Positive Rate')
        tmp_file = BytesIO()
        fig.savefig(tmp_file, format="png", bbox_inches="tight")
        tmp_file.seek(0)
        encoded = base64.b64encode(tmp_file.getvalue()).decode("utf-8")
        return _IMG_HTML_PLACEHOLDER.format(encoded_img=encoded)
    @staticmethod
    def _display_precision_recall_curve(precisions, recalls, auprc):
        fig = plt.figure()
        plt.title('Precision Recall Curve')
        plt.plot(recalls, precisions, 'b', label=f'Precision-Recall Curve (AUPRC = {auprc:0.2f})')
        plt.legend(loc='lower right')
        plt.xlim([0, 1])
        plt.ylim([0, 1.05])
        plt.ylabel('Precision')
        plt.xlabel('Recall')
        tmp_file = BytesIO()
        fig.savefig(tmp_file, format="png", bbox_inches="tight")
        tmp_file.seek(0)
        encoded = base64.b64encode(tmp_file.getvalue()).decode("utf-8")
        return _IMG_HTML_PLACEHOLDER.format(encoded_img=encoded)
    @staticmethod
    def _get_metrics(problem_type, stats) -> List:
        metrics = {}
        if problem_type == ProblemType.BINARY_CLASSIFICATION.value:
            metrics = stats['binary_classification_metrics']
        elif problem_type == ProblemType.MULTICLASS_CLASSIFICATION.value:
            metrics = stats['multiclass_classification_metrics']
        elif problem_type == ProblemType.REGRESSION.value:
            metrics = stats['regression_metrics']
        converted_metrics = [
            {
                'name': m,
                'value': metrics[m]['value'],
                'standard_deviation': metrics[m]['standard_deviation']
            } for m in metrics if set(['standard_deviation', 'value']).issubset(metrics[m].keys())
        ]
        return converted_metrics
    def _get_stats(self):
        with open(f'{self.analysis_input.output_path}/{_STATS_FILENAME}') as json_file:
            return json.load(json_file)
    def _render_templates(self, context) -> List[Any]:
        cells: List[Any] = list()
        with open(f'{_REPORT_TEMPLATES_PATH}/report.json') as json_file:
            report_templates = json.load(json_file)
        for template in report_templates:
            for cell in template['cells']:
                content = self._environment.get_template(cell['template']).render(**context)
                if cell['type'] == CellType.DOC.value:
                    cells.append(self._display_markdown(content))
                elif cell['type'] == CellType.CODE.value:
                    cells.append(self._display_code(content))
        return cells
    def generate_model_quality_report(self):
        stats = self._get_stats()
        metrics = ReportGenerator._get_metrics(self._auto_ml_job_detail.problem_type, stats)
        context = {
            'auto_ml_job_detail': self._auto_ml_job_detail,
            'metrics': metrics,
            'dataset_row_count': stats['dataset']['item_count'],
            'dataset_evaluation_time': stats['dataset']['evaluation_time'],
            'confusion_matrix': None,
            'roc_curve': None,
            'auc': None,
            'precision': None,
            'recall': None,
            'precision_recall_curve': None,
            'label_count': None,
        }
        if self._auto_ml_job_detail.problem_type == ProblemType.BINARY_CLASSIFICATION.value:
            auc = stats['binary_classification_metrics']['auc']['value']
            precision = stats['binary_classification_metrics']['precision']['value']
            recall = stats['binary_classification_metrics']['recall']['value']
            context['label_count'] = len(stats['binary_classification_metrics']['confusion_matrix'].keys())
            context['auc'] = auc
            context['precision'] = precision
            context['recall'] = recall
            context['confusion_matrix'] = ReportGenerator._display_confusion_matrix(
                stats['binary_classification_metrics']['confusion_matrix'])
            context['roc_curve'] = ReportGenerator._display_roc_curve(
                stats['binary_classification_metrics']['receiver_operating_characteristic_curve'][
                    'false_positive_rates'],
                stats['binary_classification_metrics']['receiver_operating_characteristic_curve'][
                    'true_positive_rates'],
                auc,
            )
            context['precision_recall_curve'] = ReportGenerator._display_precision_recall_curve(
                stats['binary_classification_metrics']['precision_recall_curve']['precisions'],
                stats['binary_classification_metrics']['precision_recall_curve']['recalls'],
                stats['binary_classification_metrics']['au_prc']['value'],
            )
        elif self._auto_ml_job_detail.problem_type == ProblemType.MULTICLASS_CLASSIFICATION.value:
            context['label_count'] = len(stats['multiclass_classification_metrics']['confusion_matrix'].keys())
            context['confusion_matrix'] = ReportGenerator._display_confusion_matrix(
                stats['multiclass_classification_metrics']['confusion_matrix'])
        self._add_cells(self._render_templates(context))
