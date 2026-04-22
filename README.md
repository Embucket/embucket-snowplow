# Snowplow Web Analytics with dbt

[Embucket](https://github.com/Embucket/embucket) is a Snowflake-compatible data engine based on Apache DataFusion and iceberg-rust. This repo is an example on how to run Snowflake flavour of [dbt-snowplow-web](https://hub.getdbt.com/snowplow/snowplow_web/latest/) pipeline on AWS Lambda with AWS S3 Tables as Iceberg data store

## Prerequisites

- Access to AWS account with permissions to create CloudFormation stacks: Lambda, IAM, DynamoDB, S3Tables
- Access to [AWS S3 Table Bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets-create.html)
- AWS CLI configured (`aws configure`) locally 
- [uv](https://docs.astral.sh/uv/) (or Python 3.10+ with pip)
- Git

## Quick Start

```bash
git clone https://github.com/Embucket/embucket-snowplow.git
cd embucket-snowplow
```

Set your variables:

```bash
STACK_NAME="embucket-demo-$(whoami)-$(date +%s)"
BUCKET_ARN="arn:aws:s3tables:us-east-2:YOUR_ACCOUNT:bucket/YOUR_BUCKET"
```

> **Note:** `BUCKET_ARN` is an [S3 Table Bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets.html) ARN (not a regular S3 bucket). The format is `arn:aws:s3tables:REGION:ACCOUNT:bucket/NAME`. Make sure your AWS CLI is configured for the same region as your S3 Table Bucket.

### 1. Deploy supporting infrastructure

```bash
aws cloudformation deploy \
  --template-file deploy/embucket-lambda.cfn.yaml \
  --stack-name $STACK_NAME \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides S3TableBucketArn=$BUCKET_ARN
```

This creates the IAM execution role and the DynamoDB state table. Takes 1-2 minutes.

Grab the role ARN and state table name from the stack outputs:

```bash
ROLE_ARN=$(aws cloudformation describe-stacks --stack-name $STACK_NAME \
  --query 'Stacks[0].Outputs[?OutputKey==`LambdaExecutionRoleArn`].OutputValue' \
  --output text)
STATESTORE_TABLE=$(aws cloudformation describe-stacks --stack-name $STACK_NAME \
  --query 'Stacks[0].Outputs[?OutputKey==`StateStoreTableName`].OutputValue' \
  --output text)
```

### 2. Create the Embucket Lambda function

Download the Lambda artifact from the [Embucket GitHub release](https://github.com/Embucket/embucket/releases):

```bash
EMBUCKET_VERSION="v0.2.2"
curl -LO "https://github.com/Embucket/embucket/releases/download/${EMBUCKET_VERSION}/embucket-lambda-${EMBUCKET_VERSION}-arm64.zip"
```

Create the Lambda function from the downloaded zip:

```bash
LAMBDA_NAME="embucket-demo-$STACK_NAME"
aws lambda create-function \
  --function-name $LAMBDA_NAME \
  --runtime provided.al2023 \
  --architectures arm64 \
  --handler bootstrap \
  --memory-size 3008 \
  --timeout 300 \
  --role $ROLE_ARN \
  --zip-file "fileb://embucket-lambda-${EMBUCKET_VERSION}-arm64.zip" \
  --environment "Variables={RUST_LOG=info,METASTORE_CONFIG=config/metastore.yaml,VOLUME_TYPE=s3tables,VOLUME_DATABASE=demo,VOLUME_ARN=$BUCKET_ARN,AUTH_DEMO_USER=demo_user,AUTH_DEMO_PASSWORD=demo_password_2026,JWT_SECRET=secret,LOG_FORMAT=json,MEM_POOL_TYPE=greedy,MEM_POOL_SIZE_MB=2048,QUERY_TIMEOUT_SECS=240,STATESTORE_TABLE_NAME=$STATESTORE_TABLE}"

LAMBDA_ARN=$(aws lambda get-function --function-name $LAMBDA_NAME \
  --query 'Configuration.FunctionArn' --output text)
echo $LAMBDA_ARN
```

> **Note:** IAM role propagation can take a few seconds. If `create-function` fails with an assume-role error, wait 10 seconds and retry.

### 3. Install dependencies

```bash
uv sync
```

### 4. Configure profile

```bash
cp profiles.yml.example profiles.yml
sed -i '' "s|YOUR_LAMBDA_ARN_HERE|$LAMBDA_ARN|" profiles.yml
```

### 5. Install dbt packages

```bash
uv run dbt deps --profiles-dir .
./scripts/patch_snowplow.sh
```

The dbt-snowplow-web package doesn't natively recognize the `embucket` adapter type. The patch script adds `embucket` alongside `snowflake` in the package's target-type checks. You need to re-run this script after every `dbt deps`.

### 6. Load source data

```bash
uv run python scripts/load_data.py $LAMBDA_ARN
```

This creates the required schemas and the `atomic.events` table, then loads ~28 MB of synthetic Snowplow web event data from S3.

The larger parquet source used for benchmarking is `s3://embucket-testdata/dbt_snowplow_data/synthetic_web_analytics_1g_fixed.parquet`. It is currently about 4.67 million rows and spans `collector_tstamp` from `2022-08-19 00:00:00.836` through `2022-08-26 00:05:36.751`.

### 7. Load seeds and run the models

```bash
uv run dbt seed --profiles-dir .
uv run dbt run --profiles-dir .
```

This loads reference tables (GA4 source categories, geo/language mappings) and builds the full Snowplow web analytics pipeline. Runs 18 models in about 45 seconds, producing page views, sessions, and users tables.

### 8. Query the results

```bash
uv run dbt show --profiles-dir . --inline "SELECT * FROM demo.atomic_derived.snowplow_web_page_views" --limit 10
uv run dbt show --profiles-dir . --inline "SELECT * FROM demo.atomic_derived.snowplow_web_sessions" --limit 10
uv run dbt show --profiles-dir . --inline "SELECT * FROM demo.atomic_derived.snowplow_web_users" --limit 10
```

## Cleanup

Delete the Lambda function and the CloudFormation stack (IAM role, DynamoDB table):

```bash
aws lambda delete-function --function-name embucket-demo-$STACK_NAME
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
dbt (your machine or dbt orchestrator)
  │
  │  boto3.invoke()
  ▼
AWS Lambda (embucket-lambda)
  │
  │  Apache Iceberg
  ▼
AWS S3 Table Bucket
```

- **dbt-embucket** adapter calls Lambda directly via AWS IAM — no public endpoints
- **Embucket** is a Snowflake-compatible query engine built on Apache DataFusion + Apache Iceberg
- Data is stored as Iceberg tables in your S3 Table Bucket

## Comparing Embucket vs Snowflake

Runs dbt-snowplow-web on both engines against the same S3 Tables Iceberg
source (loaded in two 30-minute Athena batches) and diffs the three
headline derived tables after each run to surface semantic drift.

Prerequisites:
- `~/.snowflake/connections.toml` has a `[connections.default]` entry with
  credentials for an account where you have ACCOUNTADMIN (or a role that
  can create catalog integrations and iceberg tables).
- IAM role `snowflake-table-bucket-access` has `glue:Get*` on
  `arn:aws:glue:us-east-2:<account>:catalog/s3tablescatalog/snowplow` and
  `s3tables:*` on the snowplow bucket.
- Lake Formation `DESCRIBE` + `SELECT` granted to that role on
  `s3tablescatalog/snowplow.atomic.events_0416` (grant once after the
  first `load_from_glue.py init`):
  ```bash
  aws lakeformation grant-permissions \
    --principal DataLakePrincipalIdentifier=arn:aws:iam::767397688925:role/snowflake-table-bucket-access \
    --resource '{"Table":{"CatalogId":"767397688925:s3tablescatalog/snowplow","DatabaseName":"atomic","Name":"events_0416"}}' \
    --permissions DESCRIBE SELECT
  ```
- `profiles.yml` has a `snowflake` output under `embucket_demo.outputs`
  (see `profiles.yml.example`).

Run flow:

```bash
# 1. One-time Snowflake setup (catalog integration + iceberg table)
uv run python scripts/snowflake_setup.py

# 2. Reset the shared Athena-managed source table
uv run python scripts/load_from_glue.py init

# 3. Load batch 1
uv run python scripts/load_from_glue.py insert \
  --start '2026-04-22 15:00:00' --end '2026-04-22 15:30:00'

# 4. Run dbt on both engines (Snowflake needs a metadata refresh first)
uv run dbt run --profiles-dir . --target dev
uv run python scripts/snowflake_refresh.py
uv run dbt run --profiles-dir . --target snowflake

# 5. Parity check
uv run python scripts/parity.py   # exits 0 on zero diffs

# 6. Load batch 2 (append, do not re-init)
uv run python scripts/load_from_glue.py insert \
  --start '2026-04-22 15:30:00' --end '2026-04-22 16:00:00'

# 7. Second dbt run exercises the incremental path
uv run dbt run --profiles-dir . --target dev
uv run python scripts/snowflake_refresh.py
uv run dbt run --profiles-dir . --target snowflake

# 8. Parity check again
uv run python scripts/parity.py
```

A non-zero exit from `parity.py` means the engines produced different
output on the same input — that's the interesting signal the harness
exists to surface. Rowcount-only mismatches hint at incremental-window or
JOIN semantics divergence; hash mismatches with matching rowcounts hint at
cast, NULL, or ordering divergence.
