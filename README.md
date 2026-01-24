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