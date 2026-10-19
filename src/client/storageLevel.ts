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
