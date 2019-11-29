package com.talend.beam;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

/** Tries to validates double aggregations in streaming mode. */
public class SparkStructuredStreamingDoubleAggregation {
  public static final class WindowedValue implements Serializable {
    private long key;
    private long value;
    private long window;

    public long getKey() {
      return key;
    }

    public void setKey(long key) {
      this.key = key;
    }

    public long getValue() {
      return value;
    }

    public void setValue(long value) {
      this.value = value;
    }

    public long getWindow() {
      return window;
    }

    public void setWindow(long window) {
      this.window = window;
    }

    public WindowedValue() {}

    @Override
    public String toString() {
      return "WindowedValue{" + "key=" + key + ", value=" + value + ", window=" + window + '}';
    }
  }

  public static void main(String[] args) {
    SparkSession spark =
        SparkSession.builder()
            .appName("com.talend.beam.SparkStreamingDoubleAggregation")
            .master("local")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate();

    Dataset<Row> lines =
        spark.readStream().format("socket").option("host", "localhost").option("port", 9999).load();

    Dataset<Row> values =
        lines
            .as(Encoders.STRING())
            .map(
                (MapFunction<String, WindowedValue>)
                    line -> {
                      String[] words = line.split(" ");
                      if (words.length != 3) {
                        return null;
                      }
                      WindowedValue wv = new WindowedValue();
                      wv.setKey(Long.parseLong(words[0]));
                      wv.setValue(Long.parseLong(words[1]));
                      wv.setWindow(Long.parseLong(words[2]));
                      // timestamp is set afterwards could not find a simple way to do it here
                      return wv;
                    },
                Encoders.bean(WindowedValue.class))
            .withColumn("eventTime", functions.current_timestamp());
    // we disable the watermark to test if double aggregations are possible without it.
    //            .withWatermark("eventTime", "3 seconds");
    values.printSchema();

    Dataset<Row> sumPerKeyAndWindow = values.groupBy("key", "window").sum("value").alias("sum");
    sumPerKeyAndWindow.printSchema();

    // If we comment the following lines (the second aggregation) and instead write the stream of
    // sumPerKeyAndWindow everything works

    Dataset<Row> sumPerKey = sumPerKeyAndWindow.groupBy("key").count();
    sumPerKey.printSchema();

    StreamingQuery query = sumPerKey.writeStream().outputMode("update").format("console").start();
    //    StreamingQuery query =
    //        sumPerKeyAndWindow.writeStream().outputMode("update").format("console").start();
    try {
      query.awaitTermination();
    } catch (StreamingQueryException e) {
      e.printStackTrace();
    }
  }
}
