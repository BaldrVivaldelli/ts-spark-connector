import { describe, expect, it } from "vitest";
import { SparkSession } from "../src";
import { schema } from "../src/schema/schema";

// Una única declaración alimenta el tipo (InferSchema) y el DDL (toDDL) que se
// inyecta en Read.DataSource.schema. Requirement 4.2.
const People = schema({
  id: "int",
  name: "string",
  age: "int?",
  active: "boolean",
});

const sess = SparkSession.builder().getOrCreate();
const people = () =>
  sess.read.readWith(People, "csv", "/tmp/people.csv", { header: "true" });

describe("DataFrameReaderTF.readWith() inyecta el DDL en el plan", () => {
  it("setea data_source.schema con el DDL declarado (sin inferencia del server)", () => {
    const proto = JSON.parse(people().toProtoJSON());
    expect(proto.read.data_source).toMatchObject({
      format: "csv",
      paths: ["/tmp/people.csv"],
      options: { header: "true" },
      schema: "id INT NOT NULL, name STRING NOT NULL, age INT, active BOOLEAN NOT NULL",
    });
  });

  it("combina las options del reader con las del readWith", () => {
    const df = sess.read
      .option("sep", ";")
      .readWith(People, "csv", "/tmp/people.csv", { header: "true" });
    const proto = JSON.parse(df.toProtoJSON());
    expect(proto.read.data_source.options).toMatchObject({ sep: ";", header: "true" });
  });

  it("mantiene el schema inferido usable downstream (select serializa)", () => {
    const proto = JSON.parse(people().select("name", "age").toProtoJSON());
    expect(proto.project.expressions).toEqual([
      { unresolved_attribute: { unparsed_identifier: "name" } },
      { unresolved_attribute: { unparsed_identifier: "age" } },
    ]);
  });

  it("preserva el case del provider al construir el plan", () => {
    const proto = JSON.parse(
      sess.read.readWith(People, "MyProvider", "/tmp/people.data").toProtoJSON()
    );
    expect(proto.read.data_source.format).toBe("MyProvider");
  });

  it("rechaza table/sql porque esos planes no pueden imponer el DDL declarado", () => {
    expect(() => sess.read.readWith(People, "table", "catalog.people"))
      .toThrow(/cannot enforce.*table/i);
    expect(() => sess.read.readWith(People, "sql", "SELECT * FROM people"))
      .toThrow(/cannot enforce.*sql/i);
  });
});
