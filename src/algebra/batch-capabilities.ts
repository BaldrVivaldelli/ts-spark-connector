import { JoinHintName } from "../engine/sparkConnectEnums";

// Gestión de cache/persist
export interface CacheCap<R> {
    cache(plan: R): R;
    persist(plan: R, level?: string): R;
    unpersist(plan: R, blocking?: boolean): R;
}

// Particionamiento
export interface RepartitionCap<R> {
    repartition(plan: R, numPartitions: number, shuffle: boolean): R;
    coalesce(plan: R, numPartitions: number): R;
}

// SQL ad-hoc (nota: en tu API original no recibe plan)
export interface SqlCap<R> {
    sql(query: string): R;
}

// Hints
export interface HintCap<R> {
    hint(plan: R, name: JoinHintName | string, params?: any[]): R;
}

// Muestreo
export interface SamplingCap<R> {
    sample(
        plan: R,
        lowerBound: number,
        upperBound: number,
        withReplacement?: boolean,
        seed?: number,
        deterministicOrder?: boolean
    ): R;
}

// Estadísticos (si querés que el backend los implemente)
// Si preferís desacoplarlos, podés mover describe/summary a helpers de alto nivel.
export interface DescribeCap<R, E> {
    describe(plan: R, columns: E[]): R;
}

export interface SummaryCap<R, E> {
    summary(plan: R, metrics: E[], columns: E[]): R;
}
