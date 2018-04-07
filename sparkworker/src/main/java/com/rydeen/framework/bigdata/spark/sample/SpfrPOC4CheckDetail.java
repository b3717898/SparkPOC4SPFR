package com.rydeen.framework.bigdata.spark.sample;

import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple3;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SpfrPOC4CheckDetail {
    //
    private static final Pattern SPACE = Pattern.compile(" ");

    private static final Pattern comma = Pattern.compile(",");

    public static void main(String[] args) throws Exception {

//        if (args.length < 1) {
//            System.err.println("Usage: JavaWordCount <file>");
//            System.exit(1);
//        }
        String args1= "hdfs://10.30.5.40:9000/AIFCST/SPFR2/POC/spfr_boh_detail.txt";
        String args2= "hdfs://10.30.5.40:9000/AIFCST/SPFR2/POC/spfr_alipay_detail.txt";
//        SparkConf conf = new SparkConf().setMaster("spark://10.30.5.40:7077").setAppName("HelloSpark");
        SparkSession spark = SparkSession
                .builder()
                .appName("SpfrPOC4CheckDetail")
//                .config("spark.driver.host","10.30.5.40")
//                .config("spark.repl.class.uri","spark://10.30.5.40:49000/classes")
                .master("spark://mango40:7077")
                //.config("spark.jars", "/workspace/boh2g_src_idea/DS_NEW/RydeenFramework/sparkworker/target/spark-worker-1.0.jar")
                //.config("spark.yarn.jars","/opt/spark-2.3.0/examples/jars/spark-worker-1.0.jar")
                .getOrCreate();

        JavaRDD<String> checkData = spark.read().textFile(args1,args2).javaRDD();

        JavaPairRDD<Tuple2<String, String>, CheckResult> extracted =
                checkData.mapToPair(s -> {
                    return new Tuple2<>(extractKey(s), extractStats(s));
                });

        JavaPairRDD<Tuple2<String, String>, CheckResult> counts =
                extracted.reduceByKey(CheckResult::merge);

        JavaPairRDD<Tuple2<String, String>, CheckResult> difference =
                counts.filter((r) -> r._2.sales!=0 && r._1._2.equals("A"));

        List<Tuple2<Tuple2<String, String>, CheckResult>> output = difference.collect();
        for (Tuple2<?,?> t : output) {
            System.out.println("SpfrPOC4CheckDetail_WithFilter:" + t._1() + "\t" + t._2());
        }
        spark.stop();
    }

    public static class CheckResult implements Serializable {

        private final int count;
        private final double sales;

        public CheckResult(int count, double sales) {
            this.count = count;
            this.sales = sales;
        }
        public CheckResult merge(CheckResult other) {
            return new CheckResult(count + other.count, sales - other.sales);
        }

        public String toString() {
            return String.format("checkResult=%s\tcount=%s", sales, count);
        }
    }

    public static Tuple2<String, String> extractKey(String line) {
        Iterator<String> results = Arrays.asList(comma.split(line)).iterator();
        if (results!=null && results.hasNext()) {
            String checkID = results.next();
            Double sales = new Double(results.next());
            String checkType = null;
            if(results.hasNext()) {
                checkType = results.next();
            }
            else {
                checkType = "A";
            }
            return new Tuple2<>(checkID,checkType);
        }
        return new Tuple2<>(null, null);
    }

    public static CheckResult extractStats(String line) {
        Iterator<String> results = Arrays.asList(comma.split(line)).iterator();
        if (results!=null && results.hasNext()) {
            String checkID = results.next();
            Double sales = new Double(results.next());
            return new CheckResult(1, sales);
        } else {
            return new CheckResult(1, 0);
        }
    }

}
