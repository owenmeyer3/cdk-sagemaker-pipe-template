import aws_cdk as core
import aws_cdk.assertions as assertions

from stacks.sagemaker_pipe_template_stack import CdkSagemakerPipeTemplateStack

# example tests. To run these tests, uncomment this file along with the example
# resource in cdk_sagemaker_pipe_template/cdk_sagemaker_pipe_template_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = CdkSagemakerPipeTemplateStack(app, "cdk-sagemaker-pipe-template")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
