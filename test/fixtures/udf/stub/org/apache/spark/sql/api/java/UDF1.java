package org.apache.spark.sql.api.java;

import java.io.Serializable;

/**
 * Compile-time stand-in for Spark's UDF1 with the same binary signature, so
 * the fixture builds without the Spark SQL jar. It is not packaged: at
 * runtime the class links against the real interface on the server.
 */
public interface UDF1<T1, R> extends Serializable {
    R call(T1 t1) throws Exception;
}
