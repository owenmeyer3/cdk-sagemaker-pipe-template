FROM public.ecr.aws/lambda/python:3.12
# login
# aws ecr get-login-password --region us-east-1 | sudo docker login --username AWS --password-stdin 088461143167.dkr.ecr.us-east-1.amazonaws.com

# create repo
# aws ecr create-repository --repository-name baseline-image --region us-east-1

# build and push
# sudo docker build -t baseline-image -f infrastructure/baseline_image/Dockerfile .
# sudo docker tag baseline-image:latest 088461143167.dkr.ecr.us-east-1.amazonaws.com/baseline-image:latest
# sudo docker push 088461143167.dkr.ecr.us-east-1.amazonaws.com/baseline-image:latest

# update function
# aws lambda update-function-code --function-name abalone-mb-baseline-fn --image-uri 088461143167.dkr.ecr.us-east-1.amazonaws.com/baseline-image:latest
# aws lambda update-function-code --function-name abalone-dq-baseline-fn --image-uri 088461143167.dkr.ecr.us-east-1.amazonaws.com/baseline-image:latest
# aws lambda update-function-code --function-name abalone-mq-baseline-fn --image-uri 088461143167.dkr.ecr.us-east-1.amazonaws.com/baseline-image:latest
# aws lambda update-function-code --function-name abalone-me-baseline-fn --image-uri 088461143167.dkr.ecr.us-east-1.amazonaws.com/baseline-image:latest

# aws lambda update-function-code --function-name abalone-prep-baseline-sets --image-uri 088461143167.dkr.ecr.us-east-1.amazonaws.com/baseline-image:latest
# aws lambda update-function-code --function-name abalone-me-baseline-fn --image-uri 088461143167.dkr.ecr.us-east-1.amazonaws.com/baseline-image:latest

WORKDIR /var/task

RUN mkdir -p baseline
COPY code/baseline_lambda/ ./baseline_lambda/
COPY code/baseline_ecs/ ./baseline_ecs/

COPY infrastructure/baseline_image/requirements.txt .

RUN pip install sagemaker --no-deps \
    && pip install -r requirements.txt \
    && pip cache purge \
    && rm -rf /tmp/*