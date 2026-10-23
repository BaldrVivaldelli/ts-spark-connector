# AWS environment for the real-AWS suite

`template.yaml` is a CloudFormation template with the smallest environment
that `npm run test:aws` needs. It was deployed and the suite passed against it
in us-east-1 on 2026-10-23.

## What it creates

- An EMR Serverless Spark application (`emr-7.13.0`, the first release with
  Spark Connect sessions, running Spark 3.5.6). Sessions are enabled, it
  starts on demand, stops after 15 idle minutes and is capped at 16 vCPU,
  64 GB of memory and 200 GB of disk.
- An EMR Serverless execution role with read-only Glue Data Catalog access, so
  `spark.catalog` calls work.
- A Glue role with the `AWSGlueServiceRole` managed policy.
- An Athena workgroup on the Apache Spark 3.5 engine, with its execution role.
  The role's policy follows AWS's Athena Spark examples; it covers the
  suite's queries, which read no tables, so add data access for real work.
- Optionally, an IAM role for the manual GitHub workflow
  (`.github/workflows/aws-e2e.yml`). It is created only when
  `GitHubOidcProviderArn` is set, trusts only `GitHubRepository`, and allows
  exactly the calls the three helpers make plus `iam:PassRole` on the three
  roles above.

Lake Formation fine-grained access control needs emr-7.14.0 or later and is
out of scope here.

## Deploy

```sh
aws cloudformation deploy \
  --template-file infra/aws-e2e/template.yaml \
  --stack-name ts-spark-connector-e2e \
  --capabilities CAPABILITY_IAM
```

The template creates no named IAM resources, so `CAPABILITY_IAM` is enough.
Parameters: `NamePrefix` (default `ts-spark-connector-e2e`),
`GitHubRepository` (default `BaldrVivaldelli/ts-spark-connector`) and
`GitHubOidcProviderArn` (default empty).

### GitHub Actions role (optional)

To let the workflow assume a role, pass the ARN of the account's GitHub OIDC
provider:

```sh
aws cloudformation deploy \
  --template-file infra/aws-e2e/template.yaml \
  --stack-name ts-spark-connector-e2e \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides GitHubOidcProviderArn=arn:aws:iam::111122223333:oidc-provider/token.actions.githubusercontent.com
```

An account has at most one GitHub OIDC provider, so the template does not
create it. If the account has none, create it once:

```sh
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com
```

Then copy the stack outputs into the repository secrets the workflow reads:
`AWS_E2E_ROLE_ARN` (output `GitHubActionsRoleArn`),
`AWS_E2E_EMR_APPLICATION_ID`, `AWS_E2E_EMR_EXECUTION_ROLE_ARN`,
`AWS_E2E_GLUE_ROLE_ARN` and `AWS_E2E_ATHENA_WORKGROUP`; the region goes in the
`AWS_E2E_REGION` variable. They are secrets because ARNs carry the account id
and this repository's workflow logs are public: set them with
`gh secret set AWS_E2E_ROLE_ARN` (it prompts for the value, so it stays out of
your shell history), never as variables.

## Run the suite

Export the variables from the stack outputs, then run the suite. Use the
region the stack was deployed to.

```sh
export AWS_REGION=us-east-1
export TS_SPARK_AWS_EMR_APPLICATION_ID=$(aws cloudformation describe-stacks --stack-name ts-spark-connector-e2e --query "Stacks[0].Outputs[?OutputKey=='EmrApplicationId'].OutputValue" --output text)
export TS_SPARK_AWS_EMR_EXECUTION_ROLE_ARN=$(aws cloudformation describe-stacks --stack-name ts-spark-connector-e2e --query "Stacks[0].Outputs[?OutputKey=='EmrExecutionRoleArn'].OutputValue" --output text)
export TS_SPARK_AWS_GLUE_ROLE_ARN=$(aws cloudformation describe-stacks --stack-name ts-spark-connector-e2e --query "Stacks[0].Outputs[?OutputKey=='GlueRoleArn'].OutputValue" --output text)
export TS_SPARK_AWS_ATHENA_WORKGROUP=$(aws cloudformation describe-stacks --stack-name ts-spark-connector-e2e --query "Stacks[0].Outputs[?OutputKey=='AthenaWorkGroup'].OutputValue" --output text)

npm run test:aws
```

A service's tests run only when its variables are set; otherwise they skip.
The snippet sets the variables without printing them; keep it that way, and
redact ARNs and ids before sharing test output.

## Cost

- Nothing in the stack bills while idle. Sessions bill only while they run.
- The suite stops every session it starts. The idle timeouts (15 minutes on
  the EMR Serverless application) are the safety net if a run is killed.
- After a failed run, check for leftover sessions:

  ```sh
  aws emr-serverless list-sessions --application-id "$TS_SPARK_AWS_EMR_APPLICATION_ID"
  aws glue list-sessions
  aws athena list-sessions --work-group "$TS_SPARK_AWS_ATHENA_WORKGROUP"
  ```

## Teardown

```sh
aws cloudformation delete-stack --stack-name ts-spark-connector-e2e
```

Stop any running sessions first (see the list-sessions commands above), so
the deletion does not fail or leave them billing.
