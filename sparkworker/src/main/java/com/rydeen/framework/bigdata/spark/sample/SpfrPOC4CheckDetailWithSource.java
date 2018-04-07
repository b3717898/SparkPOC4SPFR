package com.rydeen.framework.bigdata.spark.sample;

import com.yum.boh.core.model.Code;
import com.yum.boh.core.util.LogService;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class SpfrPOC4CheckDetailWithSource {
//    public static LogService logger = LogService.getLogger(SpfrPOC4CheckDetailWithSource.class);
    //
    private static final Pattern SPACE = Pattern.compile(" ");

    private static final Pattern comma = Pattern.compile(",");

    public static void main(String[] args) throws Exception {
        PropertyConfigurator.configure("/home/spfr/conf/log4j.properties");
        LogService logService = LogService.getLogger(SpfrPOC4CheckDetailWithSource.class);
//        String taskid = "default";
//        String channel = "";
//        Integer startDate = 0;
//        Integer endDate = 0;
//        if (args.length<4) {
//            logService.error("SPFR");
//        }
        String args1= "hdfs://10.30.5.40:9000/AIFCST/SPFR2/POC/spfr_boh_detail2.txt";
        String args2= "hdfs://10.30.5.40:9000/AIFCST/SPFR2/POC/spfr_alipay_detail2.txt";
//        SparkConf conf = new SparkConf().setMaster("spark://10.30.5.40:7077").setAppName("HelloSpark");
        SparkSession spark = SparkSession
                .builder()
                .appName("SpfrPOC4CheckDetailWithSource")
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
            String log = String.format("log4j3:SpfrPOC4CheckDetail_WithFilterWithSource:" + t._1() + "\t" + t._2());
            logService.error(log);
            System.out.println("SpfrPOC4CheckDetail_WithFilterWithSource:" + t._1() + "\t" + t._2());
        }
        spark.stop();
    }

    public static class CheckResult implements Serializable {

        private final int count;
        private final double sales;
        private final String source;

        public CheckResult(int count, double sales, String source) {
            this.count = count;
            this.sales = sales;
            this.source = source;
        }
        public CheckResult merge(CheckResult other) {
            return new CheckResult(count + other.count, sales - other.sales, source);
        }

        public String toString() {
            return String.format("source=%s\tcheckResult=%s\tcount=%s",source, sales, count);
        }
    }

    public static Tuple2<String, String> extractKey(String line) {
        Iterator<String> results = Arrays.asList(comma.split(line)).iterator();
        if (results!=null && results.hasNext()) {
            String source = results.next();
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
            String source = results.next();
            String checkID = results.next();
            Double sales = new Double(results.next());
            return new CheckResult(1, sales, source);
        } else {
            return new CheckResult(1, 0, "NA");
        }
    }

}
