# Embucket Demo: Snowplow Web Analytics with dbt

Run the full [dbt-snowplow-web](https://hub.getdbt.com/snowplow/snowplow_web/latest/) analytics pipeline on [Embucket](https://github.com/Embucket/embucket), deployed as an AWS Lambda.

## Prerequisites

- AWS account with permissions to create CloudFormation stacks (Lambda, IAM)
- An AWS S3 Table Bucket in `us-east-2` ([create one](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets-create.html) if you don't have one)
- AWS CLI configured (`aws configure`)
- [uv](https://docs.astral.sh/uv/) (or Python 3.10+ with pip)
- Git

## Quick Start

Set your variables:

```bash
STACK_NAME="embucket-demo-$(whoami)-$(date +%s)"
BUCKET_ARN="arn:aws:s3tables:us-east-2:YOUR_ACCOUNT:bucket/YOUR_BUCKET"
```

### 1. Deploy Embucket Lambda

```bash
aws cloudformation deploy \
  --template-file deploy/embucket-lambda.cfn.yaml \
  --stack-name $STACK_NAME \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-2 \
  --parameter-overrides S3TableBucketArn=$BUCKET_ARN
```

Grab the Lambda ARN:

```bash
LAMBDA_ARN=$(aws cloudformation describe-stacks --stack-name $STACK_NAME \
  --query 'Stacks[0].Outputs[?OutputKey==`LambdaFunctionArn`].OutputValue' \
  --output text --region us-east-2)
echo $LAMBDA_ARN
```

### 2. Install dependencies

```bash
uv sync
```

### 3. Configure profile

```bash
cp profiles.yml.example profiles.yml
sed -i'' -e "s|YOUR_LAMBDA_ARN_HERE|$LAMBDA_ARN|" profiles.yml
```

### 4. Install dbt packages

```bash
uv run dbt deps
./scripts/patch_snowplow.sh
```

### 5. Load source data

```bash
uv run python scripts/load_data.py $LAMBDA_ARN
```

This creates the required schemas, the `atomic.events` table, and loads ~28 MB of synthetic Snowplow web event data.

### 6. Load seeds and run the models

```bash
uv run dbt seed
uv run dbt run
```

This loads reference tables (GA4 source categories, geo/language mappings) and builds the full Snowplow web analytics pipeline: page views, sessions, and users.

### 7. Query the results

```bash
uv run dbt show --inline "SELECT * FROM demo.atomic_derived.snowplow_web_page_views LIMIT 10"
uv run dbt show --inline "SELECT * FROM demo.atomic_derived.snowplow_web_sessions LIMIT 10"
uv run dbt show --inline "SELECT * FROM demo.atomic_derived.snowplow_web_users LIMIT 10"
```

## Cleanup

```bash
aws cloudformation delete-stack --stack-name $STACK_NAME --region us-east-2
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
