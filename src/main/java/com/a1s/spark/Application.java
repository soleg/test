package com.a1s.spark;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

//https://github.com/apache/spark/tree/master/examples/src/main/java/org/apache/spark/examples

//"2017-10-26",
// "2017-10-26 15:05:56", "",
// "7016a08c",
// "td4117095",
// "END",
// "ApplyChargingReportArg",
// "77007312047",
//  0,
// 0,
// 0,
// 0,
// 0,
// "77010523809",
// 0,
// 0,
// 0,
// 0
public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String args[]) {

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
                    new StructField("date_", DataTypes.DateType, false, Metadata.empty()),
                    new StructField("dateTime", DataTypes.TimestampType, false, Metadata.empty()),
                    new StructField("wrt", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("stid", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("dtid", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("part", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("component", DataTypes.StringType , true, Metadata.empty()),
                    new StructField("aNumber", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("a1", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("a2", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("a3", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("a4", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("a5", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("bNumber", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("a6", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("a7", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("a8", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("a9", DataTypes.StringType, true, Metadata.empty()),
                });

        String file = "dataset/less.csv";

        Dataset<Row> ds = context.read().
                format("csv").
                option("header", "false").
                option("footer", "false").
                option("mode", "FAILFAST").
                option("delimiter", ",").
                schema(schema).
                load(file);

        ds.createOrReplaceTempView("raw");

        DateTime cals30Start = new DateTime();
        DateTime cals30End = new DateTime().minusDays(220);

        Dataset<Row> calls30 = context.sql(
                "SELECT * " +
                "FROM raw " +
                "WHERE date_ < cast('" + cals30Start.toString("yyyy-MM-dd") + "' as date) AND " +
                    "date_  > cast('" + cals30End.toString("yyyy-MM-dd") + "' as date)");

        log.info("###={}", calls30.count());

//        calls30.createOrReplaceTempView("calls30");
//
//        Dataset<Row> dataset = context.sql(
//                "SELECT A.aNumber, B.amount " +
//                "FROM (SELECT DISTINCT aNumber FROM raw) A " +
//                "LEFT OUTER JOIN " +
//                        "(SELECT aNumber, count(*) amount " +
//                        "FROM calls30 " +
//                        "WHERE " +
//                            "component LIKE 'InitialDP%' " +
//                        "GROUP BY aNumber) B " +
//                        "ON B.aNumber = A.aNumber " +
//                "WHERE B.amount is not null");
//
//        dataset.show();
//
//        log.info("###={}", dataset.count());

        spark.stop();
    }

//    private void calcUseRdd() {
//
//        String logFile = "dataset/export.csv";
//
//        SparkConf sparkConf = new SparkConf().
//                setAppName("SOME APP NAME").
//                setMaster("local[2]").
//                set("spark.executor.memory","4g");
//
//        JavaSparkContext spark = new JavaSparkContext(sparkConf);
//
//        JavaRDD<CsvRow> csv = spark.
//                textFile(logFile).map(
//                (Function<String, CsvRow>) CsvRow::new).
//                cache();
//
//        final DateTime now = new DateTime();
//        final DateTime lastMonthStart = now.minusMonths(1);
//        final DateTime twoMonthStart = now.minusMonths(2);
//
//        long lastMonthCount = csv.
//                filter(s ->
//                    s.getComponent().contains("InitialDP") &&
//                            s.getDate().getMillis() < now.getMillis() &&
//                            s.getDate().getMillis() > lastMonthStart.getMillis()
//                ).
//                count();
//
//        long twoMonthCount = csv.
//                filter(s ->
//                        s.getComponent().contains("InitialDP") &&
//                                s.getDate().getMillis() < lastMonthStart.getMillis() &&
//                                s.getDate().getMillis() > twoMonthStart.getMillis()
//                ).
//                count();
//
//        log.info("### lastMonthCount={}, twoMonthCount={}", lastMonthCount, twoMonthCount);
//
//        String file = "dataset/less.csv";
//
//        SparkConf sparkConf = new SparkConf().
//                setAppName("SOME APP NAME").
//                setMaster("local[2]").
//                set("spark.executor.memory","4g");
//
//        JavaSparkContext spark = new JavaSparkContext(sparkConf);
//
//        JavaRDD<CsvRow> csvRdd = spark.
//                textFile(file).map(
//                (Function<String, CsvRow>) CsvRow::new).
//                cache();
//
//        SQLContext context = new SQLContext(spark);
//
//        Dataset<Row> df = context.createDataFrame(csvRdd, CsvRow.class);
//
//        df.createOrReplaceTempView("raw");
//
//        Dataset<Row> dataset = context.sql("SELECT count(*) FROM raw");
//
//        log.debug("###={}", dataset.take(1));
//
//        spark.stop();
//        spark.stop();
//    }
//
    public static class CsvRow implements Serializable {

        private static DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
        private static DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

        private DateTime date;
        private DateTime dateTime;
        private String component;
        private Long abonentA;
        private Long abonentB;

        public CsvRow(String raw) {
            final String parts[] = raw.split(",");

            this.date = dateFormatter.parseDateTime(trim(parts[0]));
            this.dateTime =  dateTimeFormatter.parseDateTime(trim(parts[1]));
            this.component = trim(parts[6]);

            final String abonentA = trim(parts[7]);
            if (abonentA != null && !abonentA.equals("")) {
                this.abonentA = Long.parseLong(abonentA);
            }
            final String abonentB = trim(parts[12]);
            if (abonentB != null && !abonentB.equals("")) {
                this.abonentB = Long.parseLong(abonentB);
            }
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

        public Long getAbonentA() {
            return abonentA;
        }

        public void setAbonentA(Long abonentA) {
            this.abonentA = abonentA;
        }

        public Long getAbonentB() {
            return abonentB;
        }

        public void setAbonentB(Long abonentB) {
            this.abonentB = abonentB;
        }

        @Override
        public String toString() {
            return "CsvRow{" +
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
