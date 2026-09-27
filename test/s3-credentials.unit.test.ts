import { afterEach, describe, expect, it } from "vitest";
import { SparkSession } from "../src";
import type { S3Credentials } from "../src";

const originalSparkConnectUrl = process.env.SPARK_CONNECT_URL;

afterEach(() => {
  if (originalSparkConnectUrl === undefined) {
    delete process.env.SPARK_CONNECT_URL;
  } else {
    process.env.SPARK_CONNECT_URL = originalSparkConnectUrl;
  }
});

const secureBuilder = () => SparkSession.builder()
  .config("spark.connect.url", "scs://spark:15002")
  .enableTLS({ trustStorePath: "/certs/ca.pem" });

describe("withS3Credentials", () => {
  it("forwards temporary credentials as S3A configuration with the temporary provider", () => {
    const session = secureBuilder()
      .withS3Credentials({
        accessKeyId: "AKIAIOSFODNN7EXAMPLE",
        secretAccessKey: "wJalrXUtnFEMI/K7MDENG",
        sessionToken: "FwoGZXIvYXdzEBc",
      })
      .getOrCreate();

    expect(session.getConnectionConfig().sessionConfig).toMatchObject({
      "spark.hadoop.fs.s3a.access.key": "AKIAIOSFODNN7EXAMPLE",
      "spark.hadoop.fs.s3a.secret.key": "wJalrXUtnFEMI/K7MDENG",
      "spark.hadoop.fs.s3a.session.token": "FwoGZXIvYXdzEBc",
      "spark.hadoop.fs.s3a.aws.credentials.provider":
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    });
  });

  it("leaves the S3A provider chain untouched for long-lived credentials", () => {
    const session = secureBuilder()
      .withS3Credentials({
        accessKeyId: "AKIAIOSFODNN7EXAMPLE",
        secretAccessKey: "wJalrXUtnFEMI/K7MDENG",
      })
      .getOrCreate();

    const sessionConfig = session.getConnectionConfig().sessionConfig ?? {};
    expect(sessionConfig["spark.hadoop.fs.s3a.access.key"]).toBe("AKIAIOSFODNN7EXAMPLE");
    expect(sessionConfig["spark.hadoop.fs.s3a.session.token"]).toBeUndefined();
    expect(sessionConfig["spark.hadoop.fs.s3a.aws.credentials.provider"]).toBeUndefined();
  });

  it("rejects missing or empty key material", () => {
    const withCredentials = (credentials: unknown) => () =>
      secureBuilder().withS3Credentials(credentials as S3Credentials);

    expect(withCredentials({ accessKeyId: 42, secretAccessKey: "s" }))
      .toThrow("non-empty accessKeyId and secretAccessKey");
    expect(withCredentials({ accessKeyId: "  ", secretAccessKey: "s" }))
      .toThrow("non-empty accessKeyId and secretAccessKey");
    expect(withCredentials({ accessKeyId: "a", secretAccessKey: 42 }))
      .toThrow("non-empty accessKeyId and secretAccessKey");
    expect(withCredentials({ accessKeyId: "a", secretAccessKey: "" }))
      .toThrow("non-empty accessKeyId and secretAccessKey");
    expect(withCredentials({ accessKeyId: "a", secretAccessKey: "s", sessionToken: 42 }))
      .toThrow("sessionToken must be a non-empty string");
    expect(withCredentials({ accessKeyId: "a", secretAccessKey: "s", sessionToken: " " }))
      .toThrow("sessionToken must be a non-empty string");
  });
});

describe("withS3Credentials transport guard", () => {
  const credentials: S3Credentials = {
    accessKeyId: "AKIAIOSFODNN7EXAMPLE",
    secretAccessKey: "wJalrXUtnFEMI/K7MDENG",
  };

  it("refuses a plaintext sc:// channel", () => {
    expect(() => SparkSession.builder()
      .config("spark.connect.url", "sc://spark:15002")
      .withS3Credentials(credentials)
      .getOrCreate())
      .toThrow(/Refusing to send S3 credentials over an insecure Spark Connect channel/);
  });

  it("refuses the implicit default address", () => {
    delete process.env.SPARK_CONNECT_URL;
    expect(() => SparkSession.builder()
      .withS3Credentials(credentials)
      .getOrCreate())
      .toThrow(/Refusing to send S3 credentials/);
  });

  it("accepts an scs:// address without explicit TLS material", () => {
    expect(() => SparkSession.builder()
      .config("spark.connect.url", "scs://spark:15002")
      .withS3Credentials(credentials)
      .getOrCreate())
      .not.toThrow();
  });

  it("accepts an scs:// address supplied through the environment", () => {
    process.env.SPARK_CONNECT_URL = "scs://spark:15002";
    expect(() => SparkSession.builder()
      .withS3Credentials(credentials)
      .getOrCreate())
      .not.toThrow();
  });

  it("accepts explicit TLS on an sc:// address", () => {
    expect(() => SparkSession.builder()
      .config("spark.connect.url", "sc://spark:15002")
      .enableTLS({ trustStorePath: "/certs/ca.pem" })
      .withS3Credentials(credentials)
      .getOrCreate())
      .not.toThrow();
  });

  it("honours the explicit local-development opt-out", () => {
    expect(() => SparkSession.builder()
      .config("spark.connect.url", "sc://localhost:15002")
      .allowInsecureAuth()
      .withS3Credentials(credentials)
      .getOrCreate())
      .not.toThrow();
  });
});
