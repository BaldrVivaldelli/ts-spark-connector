const PROTO_INT32_MAX = 2_147_483_647;

export function assertInteger(name: string, value: number, minimum: number): void {
    if (!Number.isSafeInteger(value) || value < minimum || value > PROTO_INT32_MAX) {
        throw new RangeError(
            `${name} must be an integer between ${minimum} and ${PROTO_INT32_MAX}.`
        );
    }
}

export function assertOptionalSeed(name: string, seed?: number): void {
    if (seed !== undefined && !Number.isSafeInteger(seed)) {
        throw new RangeError(`${name} seed must be a safe integer.`);
    }
}

export function assertNonEmptyString(name: string, value: string): void {
    if (typeof value !== "string" || !value.trim()) {
        throw new TypeError(`${name} must be a non-empty string.`);
    }
}

export function freshSparkSeed(): number {
    // Spark Connect accepts int64 here, but a positive int32 is exactly
    // representable by JavaScript, protobufjs and Spark on every supported
    // runtime. Resolve this while building the lazy DataFrame so repeated
    // interpretations remain immutable.
    return Math.floor(Math.random() * PROTO_INT32_MAX);
}

let nextRelationPlanId = 1;

export function freshRelationPlanId(): number {
    const planId = nextRelationPlanId;
    nextRelationPlanId = (nextRelationPlanId % PROTO_INT32_MAX) + 1;
    return planId;
}
