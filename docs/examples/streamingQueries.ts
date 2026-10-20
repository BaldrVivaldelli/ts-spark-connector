import { withExampleSession } from "./_session.js";
import type { QueryProgressEvent, QueryTerminatedEvent } from "ts-spark-connector";

void withExampleSession(async spark => {
    // Listeners receive the lifecycle events of every query of the session.
    const listener = {
        onQueryProgress: (event: QueryProgressEvent) => {
            console.log(`batch ${event.progress.batchId}: ${event.progress.numInputRows ?? 0} rows`);
        },
        onQueryTerminated: (event: QueryTerminatedEvent) => {
            console.log(`query ${event.id} terminated${event.exception ? ` with ${event.exception}` : ""}`);
        },
    };
    await spark.streams.addListener(listener);

    const query = await spark
        .readStream("rate", { rowsPerSecond: "5", numPartitions: "1" })
        .select("value")
        .writeStream()
        .format("memory")
        .outputMode("append")
        .trigger({ processingTime: "500 milliseconds" })
        .queryName("example_rate")
        .start();

    // Spark's StreamingQuery introspection: status, progress and plans.
    await query.awaitTermination(2_000);
    await query.processAllAvailable();
    console.log("status:", await query.status());
    console.log("last batch:", (await query.lastProgress())?.batchId);

    // The StreamingQueryManager lists the session's running queries.
    for (const active of await spark.streams.active()) {
        console.log(`active: ${active.name} (${active.id})`);
    }

    await query.stop();
    await query.awaitTermination();
    await spark.streams.removeListener(listener);
    await spark.table("example_rate").limit(5).show();
});
