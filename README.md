# Xlya - SAM
This repository contain the SAM template for the Xlya's Development and production.

Here is the diagram of the architecture of the Xlya

xlya-sam/
│
├── template.yaml                 # Root
│
├── cognito/
│   └── template.yaml             # UserPool + Trigger config
│
├── dynamodb/
│   └── template.yaml             # users-table
│
└── iam/
|  └── template.yaml             # IAM Role & Policies
|
|__ auth/
   |__ template.yaml


### Docker Code

docker run --rm -it --entrypoint /bin/sh -v "C:\Users\Islams Dream\Desktop\foldename:/var/task" public.ecr.aws/lambda/python:3.13 -c "python3.13 -m pip install --upgrade pip && python3.13 -m pip install requests google-api-python-client google-auth google-auth-oauthlib google-auth-httplib2 tzdata groq -t /var/task/python"