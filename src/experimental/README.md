# Archived typed-DataFrame prototype

This directory is retained only as design history. It is excluded from the
TypeScript build, the test compilation, and the npm tarball; application code
must not import it.

The schema-aware API was unified with the public tagless-final DataFrame:

```ts
import { SparkSession, schema } from "ts-spark-connector";

const People = schema({ id: "long", name: "string", age: "int?" });
const session = SparkSession.builder().getOrCreate();
const people = session.read.readWith(People, "parquet", "/data/people");

const adults = people
  .filter(columns => columns.age.gte(18))
  .select("id", "name");
```

The maintained implementation now lives in:

- `src/schema/` — schema declarations and type-level transformations.
- `src/typed/` — typed columns, functions, aggregations, and Arrow rows.
- `src/read/readChainedDataFrame.ts` — the single public DataFrame surface.

See the root `README.md` and public tests under `test/typed/`. The remaining
files here may be removed in a future major release once no downstream design
references depend on them.
