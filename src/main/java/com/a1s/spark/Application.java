package com.a1s.spark;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String args[]) {

        String file = "dataset/less.csv";

        SparkConf sparkConf = new SparkConf().
                setAppName("SOME APP NAME").
                setMaster("local[2]").
                set("spark.executor.memory","4g");

        SparkSession spark = SparkSession.
                builder().
                appName("Java Spark SQL basic example").
                config(sparkConf).
                getOrCreate();

        SQLContext context = new SQLContext(spark);

        StructType schema = DataTypes.
                createStructType( new StructField[] {
                    new StructField("date", DataTypes.DateType, false, Metadata.empty()),
                    new StructField("dateTime", DataTypes.TimestampType, false, Metadata.empty()),
                    new StructField("c", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("q", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("w", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("e", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("component", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("t", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("y", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("u", DataTypes.StringType, true, Metadata.empty())
                });

        Dataset<org.apache.spark.sql.Row> ds = context.read().
                format("csv").
                option("header", "false").
                option("footer", "false").
                option("mode", "PERMISSIVE").
                option("delimiter", ",").
                schema(schema).
                load(file);

        Object r = ds.filter("component like '%oDisconnect%'").count();

        log.info("###={}", r);

        spark.stop();
    }

    private void calcUseRdd() {

        String logFile = "dataset/export.csv";

        SparkConf sparkConf = new SparkConf().
                setAppName("SOME APP NAME").
                setMaster("local[2]").
                set("spark.executor.memory","4g");

        JavaSparkContext spark = new JavaSparkContext(sparkConf);

        JavaRDD<Row> csv = spark.
                textFile(logFile).map(
                (Function<String, Row>) Row::new).
                cache();

        final DateTime now = new DateTime();
        final DateTime lastMonthStart = now.minusMonths(1);
        final DateTime twoMonthStart = now.minusMonths(2);

        long lastMonthCount = csv.
                filter(s ->
                    s.getComponent().contains("InitialDP") &&
                            s.getDate().getMillis() < now.getMillis() &&
                            s.getDate().getMillis() > lastMonthStart.getMillis()
                ).
                count();

        long twoMonthCount = csv.
                filter(s ->
                        s.getComponent().contains("InitialDP") &&
                                s.getDate().getMillis() < lastMonthStart.getMillis() &&
                                s.getDate().getMillis() > twoMonthStart.getMillis()
                ).
                count();

        log.info("### lastMonthCount={}, twoMonthCount={}", lastMonthCount, twoMonthCount);

        spark.stop();
    }

    public static class Row implements Serializable {

        private static DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
        private static DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

        private DateTime date;
        private DateTime dateTime;
        private String component;

        public Row(String raw) {
            final String parts[] = raw.split(",");

            this.date = dateFormatter.parseDateTime(trim(parts[0]));
            this.dateTime =  dateTimeFormatter.parseDateTime(trim(parts[1]));
            this.component = trim(parts[6]);
        }

        public DateTime getDate() {
            return date;
        }

        public DateTime getDateTime() {
            return dateTime;
        }

        public String getComponent() {
            return component;
        }

        @Override
        public String toString() {
            return "Row{" +
                    "date=" + date +
                    ", dateTime=" + dateTime +
                    ", component='" + component + '\'' +
                    '}';
        }

        private String trim(String str) {
            if (str.startsWith("\"") && str.endsWith("\"")) {
                return str.substring(1, str.lastIndexOf("\""));
            }
            return str;
        }
    }
}
