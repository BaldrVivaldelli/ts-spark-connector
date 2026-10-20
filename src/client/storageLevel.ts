/**
 * Spark storage levels as the `StorageLevel` proto message
 * (`common.proto`), shared by persist/unpersist and `catalog.cacheTable`.
 */

/** @internal */
export type StorageLevelProto = {
    use_disk: boolean;
    use_memory: boolean;
    use_off_heap: boolean;
    deserialized: boolean;
    replication: number;
};

const STORAGE_LEVELS: Record<string, StorageLevelProto> = {
    NONE: { use_disk: false, use_memory: false, use_off_heap: false, deserialized: false, replication: 1 },
    DISK_ONLY: { use_disk: true, use_memory: false, use_off_heap: false, deserialized: false, replication: 1 },
    MEMORY_ONLY: { use_disk: false, use_memory: true, use_off_heap: false, deserialized: true, replication: 1 },
    MEMORY_AND_DISK: { use_disk: true, use_memory: true, use_off_heap: false, deserialized: true, replication: 1 },
    MEMORY_ONLY_SER: { use_disk: false, use_memory: true, use_off_heap: false, deserialized: false, replication: 1 },
    MEMORY_AND_DISK_SER: { use_disk: true, use_memory: true, use_off_heap: false, deserialized: false, replication: 1 },
    OFF_HEAP: { use_disk: true, use_memory: true, use_off_heap: true, deserialized: false, replication: 1 },
};

/** Storage level names accepted by `persist()` and `catalog.cacheTable()`. */
export const STORAGE_LEVEL_NAMES: readonly string[] = Object.keys(STORAGE_LEVELS);

/** Maps a Spark storage level name (any case) to its proto message. */
export function toProtoStorageLevel(level: string): StorageLevelProto {
    const storage = STORAGE_LEVELS[level.toUpperCase()];
    if (!storage) {
        throw new RangeError(
            `Unsupported storage level "${level}". Expected one of: ${STORAGE_LEVEL_NAMES.join(", ")}.`
        );
    }
    return storage;
}

/** A Spark storage level, as `df.storageLevel()` reports it. @public */
export type StorageLevel = {
    useDisk: boolean;
    useMemory: boolean;
    useOffHeap: boolean;
    deserialized: boolean;
    replication: number;
    /** Spark's name for this combination (`MEMORY_AND_DISK`, `NONE`, ...), when it has one. */
    name?: string;
};

/** @internal Decodes the StorageLevel message of an AnalyzePlan response. */
export function storageLevelFromProto(value: unknown): StorageLevel {
    const proto = (value ?? {}) as Partial<Record<keyof StorageLevelProto, unknown>>;
    const level: StorageLevel = {
        useDisk: proto.use_disk === true,
        useMemory: proto.use_memory === true,
        useOffHeap: proto.use_off_heap === true,
        deserialized: proto.deserialized === true,
        replication: Number(proto.replication ?? 1),
    };
    const name = STORAGE_LEVEL_NAMES.find(key => {
        const candidate = STORAGE_LEVELS[key]!;
        return candidate.use_disk === level.useDisk
            && candidate.use_memory === level.useMemory
            && candidate.use_off_heap === level.useOffHeap
            && candidate.deserialized === level.deserialized
            && candidate.replication === level.replication;
    });
    return name === undefined ? level : { ...level, name };
}
