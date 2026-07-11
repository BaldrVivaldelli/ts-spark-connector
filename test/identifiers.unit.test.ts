import { describe, expect, it } from "vitest";
import { SparkSession } from "../src";
import { validateTableIdentifier } from "../src/utils/identifiers";

describe("validateTableIdentifier", () => {
  it("accepts simple and qualified identifiers", () => {
    expect(validateTableIdentifier("events")).toBe("events");
    expect(validateTableIdentifier("db.events")).toBe("db.events");
    expect(validateTableIdentifier("catalog.db.events")).toBe("catalog.db.events");
    expect(validateTableIdentifier("`weird name`")).toBe("`weird name`");
    expect(validateTableIdentifier("db.`weird name`")).toBe("db.`weird name`");
    expect(validateTableIdentifier("`name.with.dots`")).toBe("`name.with.dots`");
    expect(validateTableIdentifier("catalog.`schema.with.dots`.`table.name`"))
      .toBe("catalog.`schema.with.dots`.`table.name`");
    expect(validateTableIdentifier("db.`escaped``tick.and.dot`"))
      .toBe("db.`escaped``tick.and.dot`");
  });

  it("rejects identifiers that could break out of the query", () => {
    const bad = [
      "users; DROP TABLE users",
      "users WHERE 1=1",
      "users--comment",
      "users/*x*/",
      "db.",
      ".table",
      "",
      "   ",
      "a b",
      "`unterminated",
      "db.`unterminated",
      "`closed`.tail`",
      "db..table",
    ];
    for (const name of bad) {
      expect(() => validateTableIdentifier(name)).toThrow();
    }
  });
});

describe("SparkSession.table", () => {
  it("compiles a safe SELECT for a valid identifier", () => {
    const session = SparkSession.builder().getOrCreate();
    const proto = JSON.parse(session.table("db.events").toProtoJSON());
    expect(proto.read.named_table.unparsed_identifier).toBe("db.events");
  });

  it("preserves dots and escaped backticks inside quoted parts", () => {
    const session = SparkSession.builder().getOrCreate();
    const identifier = "catalog.`schema.with.dot`.`events``archive`";
    const proto = JSON.parse(session.table(identifier).toProtoJSON());
    expect(proto.read.named_table.unparsed_identifier).toBe(identifier);
  });

  it("rejects an injection attempt before building a plan", () => {
    const session = SparkSession.builder().getOrCreate();
    expect(() => session.table("users; DROP TABLE users")).toThrow(/invalid identifier/i);
  });
});
