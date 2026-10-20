package io.tsspark.e2e;

import org.apache.spark.sql.api.java.UDF1;

/** Upper-cases its input and appends "!"; registered by the session E2E suite. */
public final class Shout implements UDF1<String, String> {
    @Override
    public String call(String value) {
        return value == null ? null : value.toUpperCase() + "!";
    }
}
