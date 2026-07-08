FROM public.ecr.aws/lambda/python:3.12
# login
# aws ecr get-login-password --region us-east-1 | sudo docker login --username AWS --password-stdin 088461143167.dkr.ecr.us-east-1.amazonaws.com

# create repo
# aws ecr create-repository --repository-name baseline-image --region us-east-1

# build and push
# sudo docker build -t baseline-image -f infrastructure/baseline_image/Dockerfile .
# sudo docker tag baseline-image:latest 088461143167.dkr.ecr.us-east-1.amazonaws.com/baseline-image:latest
# sudo docker push 088461143167.dkr.ecr.us-east-1.amazonaws.com/baseline-image:latest
# sudo docker build -t baseline-image -f infrastructure/baseline_image/Dockerfile . && sudo docker tag baseline-image:latest 088461143167.dkr.ecr.us-east-1.amazonaws.com/baseline-image:latest && sudo docker push 088461143167.dkr.ecr.us-east-1.amazonaws.com/baseline-image:latest
# sudo docker build -t baseline-image -f infrastructure/baseline_image/Dockerfile . && sudo docker tag baseline-image:latest 088461143167.dkr.ecr.us-east-1.amazonaws.com/baseline-image:latest && sudo docker push 088461143167.dkr.ecr.us-east-1.amazonaws.com/baseline-image:latest && python3 tests/deploy.py

# view files
# sudo docker pull 088461143167.dkr.ecr.us-east-1.amazonaws.com/baseline-image:latest
# sudo docker run --rm 088461143167.dkr.ecr.us-east-1.amazonaws.com/baseline-image:latest ls -la /var/task/create_baseline_jobs/

# update function
# aws lambda update-function-code --function-name <fn> --image-uri 088461143167.dkr.ecr.us-east-1.amazonaws.com/<img>:latest

WORKDIR /var/task

RUN mkdir -p baseline
COPY code/create_baseline_jobs/ ./create_baseline_jobs/

COPY infrastructure/baseline_image/requirements.txt .

RUN pip install sagemaker --no-deps \
    && pip install -r requirements.txt \
    && pip cache purge \
    && rm -rf /tmp/*