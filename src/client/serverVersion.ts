/**
 * Raised before any RPC when a feature needs a newer Spark Connect server
 * than the one this session is connected to.
 * @public
 */
export class UnsupportedServerVersionError extends Error {
    readonly feature: string;
    readonly requiredVersion: string;
    readonly serverVersion: string;

    constructor(feature: string, requiredVersion: string, serverVersion: string) {
        super(`${feature} requires Spark Connect ${requiredVersion} or newer; the server runs ${serverVersion}.`);
        this.name = "UnsupportedServerVersionError";
        this.feature = feature;
        this.requiredVersion = requiredVersion;
        this.serverVersion = serverVersion;
    }
}

/**
 * Parses the numeric `major.minor.patch` prefix of a Spark version such as
 * `4.1.3` or `4.2.0-SNAPSHOT`; missing components count as zero.
 */
export function parseSparkVersion(version: string): [number, number, number] {
    const match = /^(\d+)(?:\.(\d+))?(?:\.(\d+))?/.exec(version.trim());
    if (!match) throw new TypeError(`Unrecognised Spark version: ${JSON.stringify(version)}.`);
    return [Number(match[1]), Number(match[2] ?? 0), Number(match[3] ?? 0)];
}

/** Negative when `a` is older than `b`, zero when equal, positive when newer. */
export function compareSparkVersions(a: string, b: string): number {
    const left = parseSparkVersion(a);
    const right = parseSparkVersion(b);
    for (let i = 0; i < 3; i += 1) {
        const delta = left[i]! - right[i]!;
        if (delta !== 0) return delta;
    }
    return 0;
}
