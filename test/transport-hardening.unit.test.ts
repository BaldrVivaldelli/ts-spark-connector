import { afterEach, describe, expect, it, vi } from "vitest";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { SparkSession } from "../src";
import {
  DEFAULT_GRPC_MAX_MESSAGE_BYTES,
  assertRpcResponseSessionIntegrity,
  assertSecureAuthTransport,
  buildChannelCredentials,
  buildChannelOptions,
  getClientCacheKey,
  getClientReferenceCount,
  sparkGrpcClient,
} from "../src/client/sparkClient";

const originalSparkConnectUrl = process.env.SPARK_CONNECT_URL;

afterEach(() => {
  vi.restoreAllMocks();
  if (originalSparkConnectUrl === undefined) {
    delete process.env.SPARK_CONNECT_URL;
  } else {
    process.env.SPARK_CONNECT_URL = originalSparkConnectUrl;
  }
});

describe("Spark Connect channel identity", () => {
  it("never shares a cached client between sc:// and scs:// for the same target", () => {
    expect(getClientCacheKey({ address: "sc://spark:15002" }))
      .not.toBe(getClientCacheKey({ address: "scs://spark:15002" }));
  });

  it("includes TLS credential and authority configuration in the cache identity", () => {
    const first = getClientCacheKey({
      address: "scs://spark:15002",
      tls: { trustStorePath: "/certs/ca-a.pem", serverNameOverride: "spark-a" },
    });
    const second = getClientCacheKey({
      address: "scs://spark:15002",
      tls: { trustStorePath: "/certs/ca-b.pem", serverNameOverride: "spark-b" },
    });

    expect(first).not.toBe(second);
    expect(first).not.toContain("/certs/ca-a.pem");
  });

  it("uses the explicit address instead of an unrelated TLS environment default", () => {
    process.env.SPARK_CONNECT_URL = "scs://secure-default:15002";
    expect(buildChannelCredentials({ address: "sc://explicit:15002" })._isSecure()).toBe(false);
  });

  it("recognizes scs:// supplied through sessionConfig", () => {
    const credentials = buildChannelCredentials({
      sessionConfig: { "spark.connect.url": "scs://spark:15002" },
    });
    expect(credentials._isSecure()).toBe(true);
  });

  it("captures the environment address when the session is created", () => {
    process.env.SPARK_CONNECT_URL = "sc://first.example:15002";
    const session = SparkSession.builder().getOrCreate();
    const retainedAddress = session.getConnectionConfig().address;

    process.env.SPARK_CONNECT_URL = "sc://second.example:15002";

    expect(retainedAddress).toBe("sc://first.example:15002");
    expect(session.getConnectionConfig().address).toBe(retainedAddress);
  });

  it("uses 128 MiB gRPC defaults and includes overrides in channel identity", () => {
    expect(buildChannelOptions()).toMatchObject({
      "grpc.max_receive_message_length": DEFAULT_GRPC_MAX_MESSAGE_BYTES,
      "grpc.max_send_message_length": DEFAULT_GRPC_MAX_MESSAGE_BYTES,
    });

    const defaultKey = getClientCacheKey({ address: "sc://spark:15002" });
    const largerKey = getClientCacheKey({
      address: "sc://spark:15002",
      grpcMaxReceiveMessageBytes: DEFAULT_GRPC_MAX_MESSAGE_BYTES + 1,
    });
    expect(largerKey).not.toBe(defaultKey);
    expect(() => buildChannelOptions({ grpcMaxReceiveMessageBytes: 0 })).toThrow(RangeError);
  });

  it("rejects unary responses from a different client or server session", () => {
    const request = {
      session_id: "00112233-4455-4677-8899-aabbccddeeff",
      client_observed_server_side_session_id: "server-session-1",
    };
    expect(() => assertRpcResponseSessionIntegrity(request, {
      session_id: request.session_id,
      server_side_session_id: "server-session-1",
    }, "Config")).not.toThrow();
    expect(() => assertRpcResponseSessionIntegrity(request, {
      session_id: "11112233-4455-4677-8899-aabbccddeeff",
      server_side_session_id: "server-session-1",
    }, "Config")).toThrow(/response session_id/i);
    try {
      assertRpcResponseSessionIntegrity(request, {
        session_id: request.session_id,
        server_side_session_id: "server-session-2",
      }, "Config");
      throw new Error("expected identity validation to fail");
    } catch (error) {
      expect(error).toMatchObject({ errorClass: "INVALID_HANDLE.SESSION_CHANGED" });
    }
  });

  it("fingerprints TLS file contents while pinning an already-retained session", async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "spark-tls-cache-"));
    const caPath = path.join(directory, "ca.pem");
    fs.writeFileSync(caPath, "first-ca-material");
    const unpinned = {
      address: "scs://spark:15002",
      tls: { trustStorePath: caPath },
    };
    const firstKey = getClientCacheKey(unpinned);
    const session = SparkSession.builder()
      .config("spark.connect.url", "scs://spark:15002")
      .enableTLS({ trustStorePath: caPath })
      .getOrCreate();
    const releaseSession = vi.spyOn(sparkGrpcClient, "releaseSession")
      .mockImplementation(async request => ({ session_id: request.session_id }));

    try {
      session.markRemoteTouched();
      const pinnedConfig = session.getConnectionConfig();
      expect(getClientReferenceCount(pinnedConfig)).toBe(1);

      fs.writeFileSync(caPath, "second-ca-material");
      const rotatedKey = getClientCacheKey(unpinned);
      expect(rotatedKey).not.toBe(firstKey);
      expect(getClientCacheKey(pinnedConfig)).toBe(firstKey);
      expect(firstKey).not.toContain("first-ca-material");

      await session.close();
      expect(getClientReferenceCount(pinnedConfig)).toBe(0);
      expect(releaseSession).toHaveBeenCalledTimes(1);
    } finally {
      fs.rmSync(directory, { recursive: true, force: true });
    }
  });
});

describe("plaintext authentication guard", () => {
  it.each([
    { type: "token" as const, token: "secret" },
    { type: "basic" as const, username: "alice", password: "secret" },
  ])("rejects $type authentication over sc://", auth => {
    expect(() => assertSecureAuthTransport({
      address: "sc://spark:15002",
      auth,
    })).toThrow(/Refusing to send (Bearer|Basic).*insecure/i);
  });

  it("enforces the guard on the real RPC entry point before opening a channel", async () => {
    await expect(sparkGrpcClient.explain({}, {
      address: "sc://spark:15002",
      auth: { type: "token", token: "secret" },
    })).rejects.toThrow(/Bearer.*insecure/i);
  });

  it("also rejects Basic/Bearer credentials supplied as a custom authorization header", () => {
    expect(() => assertSecureAuthTransport({
      address: "sc://spark:15002",
      sessionConfig: {
        "spark.connect.header.Authorization": "Bearer secret",
      },
    })).toThrow(/Bearer.*insecure/i);
  });

  it("allows authentication on TLS and an explicit plaintext development opt-in", () => {
    expect(() => assertSecureAuthTransport({
      address: "scs://spark:15002",
      auth: { type: "token", token: "secret" },
    })).not.toThrow();
    expect(() => assertSecureAuthTransport({
      address: "sc://localhost:15002",
      auth: { type: "token", token: "secret" },
      allowInsecureAuth: true,
    })).not.toThrow();
  });

  it("exposes the plaintext opt-in through SparkSession.builder", () => {
    const session = SparkSession.builder()
      .config("spark.connect.url", "sc://localhost:15002")
      .withAuth({ type: "token", token: "dev-only" })
      .allowInsecureAuth()
      .getOrCreate();

    expect(session.getConnectionConfig().allowInsecureAuth).toBe(true);
  });
});

describe("RPC execution controls", () => {
  it("validates and exposes per-RPC timeouts and abort signals", () => {
    const controller = new AbortController();
    const session = SparkSession.builder()
      .withRpcTimeout(5_000)
      .withAbortSignal(controller.signal)
      .getOrCreate();

    expect(session.getConnectionConfig()).toMatchObject({
      rpcTimeoutMs: 5_000,
      signal: controller.signal,
    });
    expect(() => SparkSession.builder().withRpcTimeout(0)).toThrow(RangeError);
    expect(() => SparkSession.builder().withRpcTimeout(1.5)).toThrow(RangeError);
  });

  it("keeps a shared channel owned until the final session closes", async () => {
    const address = "sc://refcount-test.invalid:15002";
    const releaseSession = vi.spyOn(sparkGrpcClient, "releaseSession").mockResolvedValue({});
    const first = SparkSession.builder().config("spark.connect.url", address).getOrCreate();
    const second = SparkSession.builder().config("spark.connect.url", address).getOrCreate();

    first.markRemoteTouched();
    first.markRemoteTouched();
    second.markRemoteTouched();
    expect(getClientReferenceCount(first.getConnectionConfig())).toBe(2);

    await first.close();
    expect(getClientReferenceCount(second.getConnectionConfig())).toBe(1);
    await second.close();
    expect(getClientReferenceCount({ address })).toBe(0);
    expect(releaseSession).toHaveBeenCalledTimes(2);
  });
});
