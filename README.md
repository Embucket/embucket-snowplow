# Embucket Demo: Snowplow Web Analytics with dbt

[Embucket](https://github.com/Embucket/embucket) is a Snowflake-compatible query engine built on Apache DataFusion and Apache Iceberg. This demo runs the full [dbt-snowplow-web](https://hub.getdbt.com/snowplow/snowplow_web/latest/) analytics pipeline on Embucket deployed as an AWS Lambda — no Snowflake account required.

## Prerequisites

- AWS account with permissions to create CloudFormation stacks (Lambda, IAM, DynamoDB, S3Tables)
- An [AWS S3 Table Bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets-create.html)
- AWS CLI configured (`aws configure`)
- [uv](https://docs.astral.sh/uv/) (or Python 3.10+ with pip)
- Git

## Quick Start

```bash
git clone https://github.com/Embucket/embucket-example.git
cd embucket-example
```

Set your variables:

```bash
STACK_NAME="embucket-demo-$(whoami)-$(date +%s)"
BUCKET_ARN="arn:aws:s3tables:us-east-2:YOUR_ACCOUNT:bucket/YOUR_BUCKET"
```

> **Note:** `BUCKET_ARN` is an [S3 Table Bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets.html) ARN (not a regular S3 bucket). The format is `arn:aws:s3tables:REGION:ACCOUNT:bucket/NAME`. Make sure your AWS CLI is configured for the same region as your S3 Table Bucket.

### 1. Deploy Embucket Lambda

```bash
aws cloudformation deploy \
  --template-file deploy/embucket-lambda.cfn.yaml \
  --stack-name $STACK_NAME \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides S3TableBucketArn=$BUCKET_ARN
```

This creates a Lambda function, an IAM role, and a DynamoDB state table. Takes 2-3 minutes.

Grab the Lambda ARN:

```bash
LAMBDA_ARN=$(aws cloudformation describe-stacks --stack-name $STACK_NAME \
  --query 'Stacks[0].Outputs[?OutputKey==`LambdaFunctionArn`].OutputValue' \
  --output text)
echo $LAMBDA_ARN
```

### 2. Install dependencies

```bash
uv sync
```

### 3. Configure profile

```bash
cp profiles.yml.example profiles.yml
sed -i '' "s|YOUR_LAMBDA_ARN_HERE|$LAMBDA_ARN|" profiles.yml
```

### 4. Install dbt packages

```bash
uv run dbt deps --profiles-dir .
./scripts/patch_snowplow.sh
```

The dbt-snowplow-web package doesn't natively recognize the `embucket` adapter type. The patch script adds `embucket` alongside `snowflake` in the package's target-type checks. You need to re-run this script after every `dbt deps`.

### 5. Load source data

```bash
uv run python scripts/load_data.py $LAMBDA_ARN
```

This creates the required schemas and the `atomic.events` table, then loads ~28 MB of synthetic Snowplow web event data from S3.

### 6. Load seeds and run the models

```bash
uv run dbt seed --profiles-dir .
uv run dbt run --profiles-dir .
```

This loads reference tables (GA4 source categories, geo/language mappings) and builds the full Snowplow web analytics pipeline. Runs 18 models in about 45 seconds, producing page views, sessions, and users tables.

### 7. Query the results

```bash
uv run dbt show --profiles-dir . --inline "SELECT * FROM demo.atomic_derived.snowplow_web_page_views" --limit 10
uv run dbt show --profiles-dir . --inline "SELECT * FROM demo.atomic_derived.snowplow_web_sessions" --limit 10
uv run dbt show --profiles-dir . --inline "SELECT * FROM demo.atomic_derived.snowplow_web_users" --limit 10
```

## Cleanup

Delete the CloudFormation stack (Lambda, IAM role, DynamoDB table):

```bash
aws cloudformation delete-stack --stack-name $STACK_NAME
```

> **Note:** This does not delete data in your S3 Table Bucket. Iceberg tables created by Embucket persist there until you remove them manually.

If you lost the `$STACK_NAME` variable, find it with:

```bash
aws cloudformation list-stacks --stack-status-filter CREATE_COMPLETE \
  --query 'StackSummaries[?starts_with(StackName,`embucket-demo`)].StackName' --output table
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
