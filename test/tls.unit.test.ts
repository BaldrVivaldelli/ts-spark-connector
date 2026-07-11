import path from "node:path";
import fs from "node:fs";
import { describe, expect, it } from "vitest";
import { buildChannelCredentials } from "../src/client/sparkClient";

// grpc-js cannot consume PKCS#12 keystores via createSsl, but the client now
// loads them through tls.createSecureContext({ pfx }) + createFromSecureContext,
// so no external dependency is needed. These tests exercise credential
// construction directly (no network), which is where the keystore is parsed.
const KEYSTORE = path.resolve(__dirname, "../spark-server/certs/keystore.p12");
const hasKeystore = fs.existsSync(KEYSTORE);

describe("buildChannelCredentials - PKCS#12", () => {
  it.runIf(hasKeystore)("loads a real .p12 keystore and builds secure credentials", () => {
    const creds = buildChannelCredentials({
      tls: { keyStorePath: KEYSTORE, keyStorePassword: "password" },
    });
    expect(creds._isSecure()).toBe(true);
  });

  it.runIf(hasKeystore)("throws a clear error when the keystore password is wrong", () => {
    expect(() =>
      buildChannelCredentials({
        tls: { keyStorePath: KEYSTORE, keyStorePassword: "definitely-wrong" },
      })
    ).toThrow(/PKCS#12 keystore/i);
  });

  it("throws when the keystore file does not exist", () => {
    expect(() =>
      buildChannelCredentials({
        tls: { keyStorePath: "/nonexistent/keystore.p12", keyStorePassword: "x" },
      })
    ).toThrow(/not found/i);
  });

  it("rejects a PKCS#12 truststore (CA must be PEM)", () => {
    if (!hasKeystore) return;
    expect(() =>
      buildChannelCredentials({
        tls: {
          keyStorePath: KEYSTORE,
          keyStorePassword: "password",
          trustStorePath: "/some/ca.p12",
        },
      })
    ).toThrow(/PKCS#12/i);
  });

  it.each([
    { keyStorePath: "/certs/client.jks" },
    { trustStorePath: "/certs/trust.jks" },
  ])("rejects unsupported JKS stores before reading them", tls => {
    expect(() => buildChannelCredentials({ tls })).toThrow(/JKS.*not supported/i);
  });

  it("rejects a non-PKCS#12 keyStorePath instead of silently ignoring it", () => {
    expect(() => buildChannelCredentials({
      tls: { keyStorePath: "/certs/client.pem" },
    })).toThrow(/keyStorePath supports only PKCS#12/i);
  });

  it("rejects a PKCS#12 truststore even when no client keystore is configured", () => {
    expect(() => buildChannelCredentials({
      tls: { trustStorePath: "/certs/trust.p12" },
    })).toThrow(/PEM CA.*PKCS#12.*not supported/i);
  });

  it.each([
    { certChainPath: "/certs/client.crt" },
    { privateKeyPath: "/certs/client.key" },
  ])("requires a complete PEM mTLS cert/key pair", tls => {
    expect(() => buildChannelCredentials({ tls })).toThrow(/requires both certChainPath and privateKeyPath/i);
  });

  it("rejects ambiguous PKCS#12 and PEM client credentials", () => {
    expect(() => buildChannelCredentials({
      tls: {
        keyStorePath: KEYSTORE,
        keyStorePassword: "password",
        certChainPath: "/certs/client.crt",
        privateKeyPath: "/certs/client.key",
      },
    })).toThrow(/either a PKCS#12.*or PEM.*not both/i);
  });
});

describe("buildChannelCredentials - non-TLS / PEM", () => {
  it("returns insecure credentials when TLS is not enabled", () => {
    const creds = buildChannelCredentials({});
    expect(creds._isSecure()).toBe(false);
  });
});
