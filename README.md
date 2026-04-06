# Embucket Demo: Snowplow Web Analytics with dbt

Run the full [dbt-snowplow-web](https://hub.getdbt.com/snowplow/snowplow_web/latest/) analytics pipeline on [Embucket](https://github.com/Embucket/embucket), deployed as an AWS Lambda.

## Prerequisites

- AWS account with permissions to create CloudFormation stacks (Lambda, IAM)
- An AWS S3 Table Bucket in `us-east-2` ([create one](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets-create.html) if you don't have one)
- AWS CLI configured (`aws configure`)
- Python 3.10+
- Git

## Quick Start

### 1. Deploy Embucket Lambda

```bash
aws cloudformation deploy \
  --template-file deploy/embucket-lambda.cfn.yaml \
  --stack-name embucket-demo \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-2 \
  --parameter-overrides S3TableBucketArn=YOUR_TABLE_BUCKET_ARN
```

Grab the Lambda ARN:

```bash
aws cloudformation describe-stacks --stack-name embucket-demo \
  --query 'Stacks[0].Outputs[?OutputKey==`LambdaFunctionArn`].OutputValue' \
  --output text --region us-east-2
```

### 2. Install dbt

```bash
pip install dbt-core dbt-embucket
```

### 3. Configure profile

```bash
cp profiles.yml.example profiles.yml
```

Edit `profiles.yml` and replace `YOUR_LAMBDA_ARN_HERE` with the ARN from step 1.

### 4. Install dbt packages

```bash
dbt deps
./scripts/patch_snowplow.sh
```

### 5. Load source data

```bash
python scripts/load_data.py YOUR_LAMBDA_ARN_HERE
```

Replace `YOUR_LAMBDA_ARN_HERE` with the ARN from step 1. This creates the required schemas, the `atomic.events` table, and loads ~28 MB of synthetic Snowplow web event data.

### 6. Run the models

```bash
dbt run
```

This builds the full Snowplow web analytics pipeline: page views, sessions, and users.

### 7. Query the results

```bash
dbt show --inline "SELECT * FROM demo.atomic.snowplow_web_page_views LIMIT 10"
dbt show --inline "SELECT * FROM demo.atomic.snowplow_web_sessions LIMIT 10"
dbt show --inline "SELECT * FROM demo.atomic.snowplow_web_users LIMIT 10"
```

## Cleanup

```bash
aws cloudformation delete-stack --stack-name embucket-demo --region us-east-2
```

## How it works

```
dbt (your machine)
  │
  │  boto3.invoke()
  ▼
AWS Lambda (Embucket)
  │
  │  Apache Iceberg
  ▼
AWS S3 Table Bucket
```

- **dbt-embucket** adapter calls Lambda directly via AWS IAM — no public endpoints
- **Embucket** is a Snowflake-compatible query engine built on Apache DataFusion + Apache Iceberg
- Data is stored as Iceberg tables in your S3 Table Bucket
