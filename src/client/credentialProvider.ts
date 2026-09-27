import type { SparkCredentialProvider, SparkCredentials } from "./session";

/** Refresh window applied before `expiresAt` when the provider sets no skew. */
export const DEFAULT_REFRESH_SKEW_MS = 30_000;

/**
 * Hold applied to results without `expiresAt`, long enough to absorb the RPC
 * burst of a single query while leaving the provider authoritative beyond it.
 */
export const NO_EXPIRY_CACHE_TTL_MS = 1_000;

type CacheEntry = {
    credentials: SparkCredentials;
    /** Epoch ms after which the entry triggers a refresh attempt. */
    staleAt: number;
};

type ProviderState = {
    cached?: CacheEntry;
    inFlight?: Promise<SparkCredentials>;
};

// Keyed by provider identity so sibling sessions sharing one provider also
// share a single in-flight refresh instead of stampeding the token endpoint.
const providerStates = new WeakMap<SparkCredentialProvider, ProviderState>();

const HEADER_NAME = /^[a-z0-9][a-z0-9_.-]*$/;
const HEADER_VALUE_FORBIDDEN = /[\r\n\0]/;

export function validateRefreshSkew(refreshSkewMs?: number): number {
    if (refreshSkewMs === undefined) return DEFAULT_REFRESH_SKEW_MS;
    if (!Number.isSafeInteger(refreshSkewMs) || refreshSkewMs < 0) {
        throw new RangeError("auth.refreshSkewMs must be a non-negative safe integer.");
    }
    return refreshSkewMs;
}

// Error messages name headers but never echo their values, so a malformed
// token cannot reach a log through the validation path.
function validateCredentials(value: unknown): SparkCredentials {
    if (value == null || typeof value !== "object") {
        throw new TypeError("Credential provider must resolve to a SparkCredentials object.");
    }

    const { headers, expiresAt } = value as SparkCredentials;
    if (headers == null || typeof headers !== "object" || Array.isArray(headers)) {
        throw new TypeError("Credential provider must resolve `headers` to a string record.");
    }

    const normalized: Record<string, string> = {};
    for (const [rawName, rawValue] of Object.entries(headers)) {
        const name = rawName.trim().toLowerCase();
        if (!HEADER_NAME.test(name)) {
            throw new TypeError(
                `Credential provider returned an invalid header name: ${JSON.stringify(rawName)}.`
            );
        }
        if (typeof rawValue !== "string" || HEADER_VALUE_FORBIDDEN.test(rawValue)) {
            throw new TypeError(
                `Credential provider returned an invalid value for header "${name}".`
            );
        }
        normalized[name] = rawValue;
    }

    if (Object.keys(normalized).length === 0) {
        throw new TypeError("Credential provider must resolve at least one header.");
    }
    if (expiresAt !== undefined && (!Number.isFinite(expiresAt) || expiresAt <= 0)) {
        throw new TypeError("Credential provider returned a non-finite `expiresAt`.");
    }

    return expiresAt === undefined
        ? { headers: normalized }
        : { headers: normalized, expiresAt };
}

/**
 * @internal Resolves provider credentials for one RPC.
 *
 * Credentials with `expiresAt` are cached until the refresh window opens; a
 * refresh that fails inside that window falls back to the held credential for
 * as long as it is genuinely valid, reporting the failure through
 * `onDegraded`. Results without `expiresAt` are held only briefly
 * ({@link NO_EXPIRY_CACHE_TTL_MS}), so a self-caching provider stays
 * authoritative and never earns a fallback of unknown validity.
 *
 * `onDegraded` must not throw; only the caller that initiates a refresh is
 * reported to, since piggybacking callers share the same settled promise.
 */
export async function resolveProviderCredentials(
    provider: SparkCredentialProvider,
    refreshSkewMs?: number,
    onDegraded?: (error: unknown) => void,
): Promise<SparkCredentials> {
    if (typeof provider !== "function") {
        throw new TypeError("auth.provider must be a function.");
    }
    const skew = validateRefreshSkew(refreshSkewMs);

    let state = providerStates.get(provider);
    if (!state) {
        state = {};
        providerStates.set(provider, state);
    }
    const providerState = state;

    if (providerState.cached && Date.now() < providerState.cached.staleAt) {
        return providerState.cached.credentials;
    }
    if (providerState.inFlight) return providerState.inFlight;

    // The fallback lives inside the shared promise: callers piggybacking on
    // `inFlight` would otherwise observe the raw rejection.
    const pending = (async (): Promise<SparkCredentials> => {
        let credentials: SparkCredentials;
        try {
            credentials = validateCredentials(await provider());
        } catch (error) {
            const held = providerState.cached;
            if (held?.credentials.expiresAt !== undefined
                && Date.now() < held.credentials.expiresAt) {
                onDegraded?.(error);
                return held.credentials;
            }
            throw error;
        }
        providerState.cached = {
            credentials,
            staleAt: credentials.expiresAt === undefined
                ? Date.now() + NO_EXPIRY_CACHE_TTL_MS
                : credentials.expiresAt - skew,
        };
        return credentials;
    })();

    // Callers arriving while this is set share `pending` and never reassign it,
    // so the slot still holds `pending` by the time this settles.
    providerState.inFlight = pending;
    try {
        return await pending;
    } finally {
        providerState.inFlight = undefined;
    }
}
