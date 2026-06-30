FROM public.ecr.aws/lambda/python:3.12

# sudo docker build -t baseline-image -f infrastructure/baseline_image/Dockerfile .
# sudo docker tag baseline-image:latest 088461143167.dkr.ecr.us-east-1.amazonaws.com/baseline-image:latest
# sudo docker push 088461143167.dkr.ecr.us-east-1.amazonaws.com/baseline-image:latest
# aws lambda update-function-code --function-name BaselineLambda --image-uri 088461143167.dkr.ecr.us-east-1.amazonaws.com/baseline-lambda:latest

WORKDIR /var/task

RUN mkdir -p baseline
COPY code/baseline_lambda/ ./baseline_lambda/

COPY infrastructure/baseline_image/requirements.txt .

RUN pip install -r requirements.txt