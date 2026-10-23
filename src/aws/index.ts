/**
 * Connects ts-spark-connector to AWS-managed Spark Connect endpoints: EMR
 * Serverless, AWS Glue interactive sessions and Amazon Athena for Apache
 * Spark. Each helper needs only its own AWS SDK client package, loaded when
 * it is called.
 *
 * @packageDocumentation
 * @module ts-spark-connector/aws
 */
export { emrServerlessSession } from "./emrServerless";
export type { EmrServerlessSessionOptions } from "./emrServerless";
export { glueSession } from "./glue";
export type { GlueSessionOptions } from "./glue";
export { athenaSession } from "./athena";
export type { AthenaSessionOptions } from "./athena";
export { AwsSparkSessionError } from "./managedSession";
export type { AwsSdkClient, AwsSparkConnection, AwsSparkSessionOptions } from "./managedSession";
