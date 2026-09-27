import { afterEach, describe, expect, it, vi } from "vitest";
import { SparkSession } from "../src";
import type { SparkCredentialProvider } from "../src";
import {
  DEFAULT_REFRESH_SKEW_MS,
  NO_EXPIRY_CACHE_TTL_MS,
  resolveProviderCredentials,
} from "../src/client/credentialProvider";
import {
  assertSecureAuthTransport,
  buildMetadata,
  getClientCacheKey,
} from "../src/client/sparkClient";
import { normalizeConnectionConfig } from "../src/client/sessionConfig";

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

const bearer = (token: string, expiresAt?: number) => ({
  headers: { authorization: `Bearer ${token}` },
  ...(expiresAt === undefined ? {} : { expiresAt }),
});

describe("credential provider caching", () => {
  it("reuses credentials until the refresh window opens", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const provider = vi.fn(() => bearer("first", 120_000));

    await resolveProviderCredentials(provider);
    vi.setSystemTime(60_000);
    const cached = await resolveProviderCredentials(provider);

    expect(provider).toHaveBeenCalledTimes(1);
    expect(cached.headers.authorization).toBe("Bearer first");
  });

  it("refreshes once the credentials fall inside the skew window", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    let issued = 0;
    const provider = vi.fn(() => bearer(`token-${++issued}`, Date.now() + 120_000));

    await resolveProviderCredentials(provider);
    // 120s lifetime minus the default 30s skew means staleness begins at 90s.
    vi.setSystemTime(90_000);
    const refreshed = await resolveProviderCredentials(provider);

    expect(provider).toHaveBeenCalledTimes(2);
    expect(refreshed.headers.authorization).toBe("Bearer token-2");
  });

  it("honours a custom refresh skew", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const provider = vi.fn(() => bearer("t", 120_000));

    await resolveProviderCredentials(provider, 90_000);
    vi.setSystemTime(45_000);
    await resolveProviderCredentials(provider, 90_000);

    expect(provider).toHaveBeenCalledTimes(2);
  });

  it("holds a result without expiresAt only long enough to absorb one burst", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const provider = vi.fn(() => bearer("no-expiry"));

    await resolveProviderCredentials(provider);
    await resolveProviderCredentials(provider);
    expect(provider).toHaveBeenCalledTimes(1);

    vi.setSystemTime(NO_EXPIRY_CACHE_TTL_MS);
    await resolveProviderCredentials(provider);
    expect(provider).toHaveBeenCalledTimes(2);
  });

  it("collapses concurrent refreshes into a single provider call", async () => {
    let release: (() => void) | undefined;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const provider = vi.fn(async () => {
      await gate;
      return bearer("shared", Date.now() + 600_000);
    });

    const inFlight = Promise.all([
      resolveProviderCredentials(provider),
      resolveProviderCredentials(provider),
      resolveProviderCredentials(provider),
    ]);
    release?.();
    const results = await inFlight;

    expect(provider).toHaveBeenCalledTimes(1);
    expect(results.map(r => r.headers.authorization))
      .toEqual(["Bearer shared", "Bearer shared", "Bearer shared"]);
  });

  it("retries after a failed refresh instead of caching the rejection", async () => {
    const provider = vi.fn()
      .mockRejectedValueOnce(new Error("token endpoint down"))
      .mockResolvedValueOnce(bearer("recovered", Date.now() + 600_000));

    await expect(resolveProviderCredentials(provider)).rejects.toThrow("token endpoint down");
    const recovered = await resolveProviderCredentials(provider);

    expect(recovered.headers.authorization).toBe("Bearer recovered");
    expect(provider).toHaveBeenCalledTimes(2);
  });

  it("defaults the refresh skew to thirty seconds", () => {
    expect(DEFAULT_REFRESH_SKEW_MS).toBe(30_000);
  });
});

describe("stale credential fallback", () => {
  it("serves the held credential when a refresh fails before real expiry", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const provider = vi.fn()
      .mockResolvedValueOnce(bearer("held", 120_000))
      .mockRejectedValueOnce(new Error("token endpoint down"));
    const onDegraded = vi.fn();

    await resolveProviderCredentials(provider, undefined, onDegraded);
    // Inside the skew window (staleAt 90s) but before expiry (120s).
    vi.setSystemTime(100_000);
    const fallback = await resolveProviderCredentials(provider, undefined, onDegraded);

    expect(fallback.headers.authorization).toBe("Bearer held");
    expect(onDegraded).toHaveBeenCalledTimes(1);
    expect(provider).toHaveBeenCalledTimes(2);
  });

  it("delivers the fallback to callers piggybacking on the shared refresh", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    let release: (() => void) | undefined;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const provider = vi.fn()
      .mockResolvedValueOnce(bearer("held", 120_000))
      .mockImplementation(async () => {
        await gate;
        throw new Error("token endpoint down");
      });
    const onDegraded = vi.fn();

    await resolveProviderCredentials(provider, undefined, onDegraded);
    vi.setSystemTime(100_000);
    const sharers = Promise.all([
      resolveProviderCredentials(provider, undefined, onDegraded),
      resolveProviderCredentials(provider, undefined, onDegraded),
    ]);
    release?.();
    const results = await sharers;

    expect(results.map(r => r.headers.authorization))
      .toEqual(["Bearer held", "Bearer held"]);
    expect(provider).toHaveBeenCalledTimes(2);
    expect(onDegraded).toHaveBeenCalledTimes(1);
  });

  it("propagates the refresh failure once the held credential truly expired", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const provider = vi.fn()
      .mockResolvedValueOnce(bearer("held", 120_000))
      .mockRejectedValueOnce(new Error("token endpoint down"));

    await resolveProviderCredentials(provider);
    vi.setSystemTime(121_000);
    await expect(resolveProviderCredentials(provider))
      .rejects.toThrow("token endpoint down");
  });

  it("never falls back to a held credential of unknown validity", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const provider = vi.fn()
      .mockResolvedValueOnce(bearer("no-expiry"))
      .mockRejectedValueOnce(new Error("token endpoint down"));

    await resolveProviderCredentials(provider);
    vi.setSystemTime(NO_EXPIRY_CACHE_TTL_MS + 500);
    await expect(resolveProviderCredentials(provider))
      .rejects.toThrow("token endpoint down");
  });
});

describe("credential provider validation", () => {
  const rejects = (value: unknown) =>
    expect(resolveProviderCredentials((() => value) as SparkCredentialProvider));

  it("rejects a non-function provider", async () => {
    await expect(resolveProviderCredentials("nope" as unknown as SparkCredentialProvider))
      .rejects.toThrow("auth.provider must be a function.");
  });

  it("rejects a negative or fractional refresh skew", async () => {
    const provider = () => bearer("t");
    await expect(resolveProviderCredentials(provider, -1)).rejects.toThrow(RangeError);
    await expect(resolveProviderCredentials(provider, 1.5)).rejects.toThrow(RangeError);
  });

  it("rejects malformed credential shapes", async () => {
    await rejects(null).rejects.toThrow("must resolve to a SparkCredentials object");
    await rejects({}).rejects.toThrow("must resolve `headers` to a string record");
    await rejects({ headers: [] }).rejects.toThrow("must resolve `headers` to a string record");
    await rejects({ headers: {} }).rejects.toThrow("must resolve at least one header");
  });

  it("rejects header names that are not valid gRPC metadata keys", async () => {
    await rejects({ headers: { "bad header": "v" } }).rejects.toThrow("invalid header name");
    await rejects({ headers: { "": "v" } }).rejects.toThrow("invalid header name");
  });

  it("rejects header values carrying CRLF or NUL without echoing the value", async () => {
    const smuggled = "Bearer good\r\nx-injected: evil";
    await rejects({ headers: { authorization: smuggled } })
      .rejects.toThrow(/invalid value for header "authorization"/);

    let message = "";
    try {
      await resolveProviderCredentials(
        (() => ({ headers: { authorization: smuggled } })) as SparkCredentialProvider,
      );
    } catch (error) {
      message = (error as Error).message;
    }
    expect(message).toContain("authorization");
    expect(message).not.toContain("evil");
  });

  it("rejects a non-finite expiry", async () => {
    await rejects({ headers: { authorization: "Bearer t" }, expiresAt: Number.NaN })
      .rejects.toThrow("non-finite `expiresAt`");
  });

  it("lowercases header names before they reach the wire", async () => {
    const resolved = await resolveProviderCredentials(
      () => ({ headers: { "X-Api-Key": "abc" } }),
    );
    expect(resolved.headers).toEqual({ "x-api-key": "abc" });
  });
});

describe("provider credentials on the wire", () => {
  it("attaches resolved headers to the call metadata", async () => {
    const metadata = await buildMetadata({
      address: "scs://spark:15002",
      tls: { trustStorePath: "/certs/ca.pem" },
      auth: { type: "provider", provider: () => bearer("wire-token") },
    });

    expect(metadata.get("authorization")).toEqual(["Bearer wire-token"]);
  });

  it("rejects a static header colliding with a provider-supplied one", async () => {
    await expect(buildMetadata({
      address: "scs://spark:15002",
      tls: { trustStorePath: "/certs/ca.pem" },
      auth: { type: "provider", provider: () => bearer("from-provider") },
      sessionConfig: { "spark.connect.header.Authorization": "Bearer pinned" },
    })).rejects.toThrow(/conflicts with a header supplied by the credential provider/);
  });

  it("still allows static headers disjoint from the provider's", async () => {
    const metadata = await buildMetadata({
      address: "scs://spark:15002",
      tls: { trustStorePath: "/certs/ca.pem" },
      auth: { type: "provider", provider: () => bearer("from-provider") },
      sessionConfig: { "spark.connect.header.x-trace-id": "trace-1" },
    });

    expect(metadata.get("authorization")).toEqual(["Bearer from-provider"]);
    expect(metadata.get("x-trace-id")).toEqual(["trace-1"]);
  });

  it("reports a degraded refresh through redacted telemetry and keeps the RPC alive", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const provider = vi.fn()
      .mockResolvedValueOnce(bearer("held", 120_000))
      .mockRejectedValueOnce(new Error("exchange rejected Bearer super-secret"));
    const events: Array<{ name: string; attributes: Record<string, unknown> }> = [];
    const config = {
      address: "scs://spark:15002",
      tls: { trustStorePath: "/certs/ca.pem" },
      auth: { type: "provider", provider } as const,
      logger: (event: { name: string; attributes: Record<string, unknown> }) => {
        events.push({ name: event.name, attributes: event.attributes });
      },
    };

    await buildMetadata(config);
    vi.setSystemTime(100_000);
    const metadata = await buildMetadata(config);

    expect(metadata.get("authorization")).toEqual(["Bearer held"]);
    const fallback = events.filter(e => e.name === "spark.auth.refresh_fallback");
    expect(fallback).toHaveLength(1);
    expect(String(fallback[0]?.attributes.reason)).toContain("exchange rejected");
    expect(String(fallback[0]?.attributes.reason)).not.toContain("super-secret");
  });

  it("surfaces a provider failure to the caller", async () => {
    await expect(buildMetadata({
      address: "scs://spark:15002",
      tls: { trustStorePath: "/certs/ca.pem" },
      auth: {
        type: "provider",
        provider: () => { throw new Error("STS AssumeRole denied"); },
      },
    })).rejects.toThrow("STS AssumeRole denied");
  });

  it("keeps the channel identity stable across a token rotation", () => {
    const base = { address: "scs://spark:15002", tls: { trustStorePath: "/certs/ca.pem" } };
    const first = getClientCacheKey({
      ...base,
      auth: { type: "provider", provider: () => bearer("a") },
    });
    const second = getClientCacheKey({
      ...base,
      auth: { type: "provider", provider: () => bearer("b") },
    });

    expect(first).toBe(second);
  });
});

describe("provider credentials and transport security", () => {
  const provider: SparkCredentialProvider = () => bearer("t");

  it("refuses provider credentials on a plaintext channel", () => {
    expect(() => assertSecureAuthTransport({
      address: "sc://spark:15002",
      auth: { type: "provider", provider },
    })).toThrow(/Refusing to send provider-supplied credentials/);
  });

  it("allows provider credentials once TLS is configured", () => {
    expect(() => assertSecureAuthTransport({
      address: "scs://spark:15002",
      tls: { trustStorePath: "/certs/ca.pem" },
      auth: { type: "provider", provider },
    })).not.toThrow();
  });

  it("allows an explicit insecure opt-out for local development", () => {
    expect(() => assertSecureAuthTransport({
      address: "sc://localhost:15002",
      auth: { type: "provider", provider },
      allowInsecureAuth: true,
    })).not.toThrow();
  });
});

describe("builder and config integration", () => {
  const provider: SparkCredentialProvider = () => bearer("t");

  it("carries a provider through the builder into the connection config", () => {
    const session = SparkSession.builder()
      .config("spark.connect.url", "scs://spark:15002")
      .enableTLS({ trustStorePath: "/certs/ca.pem" })
      .withAuth({ type: "provider", provider, refreshSkewMs: 5_000 })
      .getOrCreate();

    expect(session.getConnectionConfig().auth)
      .toEqual({ type: "provider", provider, refreshSkewMs: 5_000 });
  });

  it("does not let an incomplete legacy auth key discard a provider", () => {
    const session = SparkSession.builder()
      .config("spark.connect.url", "scs://spark:15002")
      .enableTLS({ trustStorePath: "/certs/ca.pem" })
      .withAuth({ type: "provider", provider })
      .config("spark.auth.token", "stray-value")
      .getOrCreate();

    expect(session.getConnectionConfig().auth).toMatchObject({ type: "provider" });
  });

  it("lets a complete legacy auth configuration replace a provider", () => {
    const session = SparkSession.builder()
      .config("spark.connect.url", "scs://spark:15002")
      .enableTLS({ trustStorePath: "/certs/ca.pem" })
      .withAuth({ type: "provider", provider })
      .config("spark.auth.type", "token")
      .config("spark.auth.token", "explicit-token")
      .getOrCreate();

    expect(session.getConnectionConfig().auth)
      .toEqual({ type: "token", token: "explicit-token" });
  });

  it("rejects an invalid provider when the connection config is normalized", () => {
    expect(() => normalizeConnectionConfig({
      address: "scs://spark:15002",
      auth: { type: "provider", provider: "nope" as unknown as SparkCredentialProvider },
    })).toThrow("auth.provider must be a function.");

    expect(() => normalizeConnectionConfig({
      address: "scs://spark:15002",
      auth: { type: "provider", provider, refreshSkewMs: -5 },
    })).toThrow(RangeError);
  });
});
