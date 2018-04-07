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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;


public class SpfrPOC {
    //
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

//        if (args.length < 1) {
//            System.err.println("Usage: JavaWordCount <file>");
//            System.exit(1);
//        }
        String args1= "hdfs://10.30.5.40:9000/AIFCST/SPFR2/POC/spfrpoc1.txt";
        String args2= "hdfs://10.30.5.40:9000/AIFCST/SPFR2/POC/spfrpoc2.txt";
//        SparkConf conf = new SparkConf().setMaster("spark://10.30.5.40:7077").setAppName("HelloSpark");
        SparkSession spark = SparkSession
                .builder()
                .appName("SpfrPOC")
//                .config("spark.driver.host","10.30.5.40")
//                .config("spark.repl.class.uri","spark://10.30.5.40:49000/classes")
                .master("spark://mango40:7077")
                //.config("spark.jars", "/workspace/boh2g_src_idea/DS_NEW/RydeenFramework/sparkworker/target/spark-worker-1.0.jar")
                //.config("spark.yarn.jars","/opt/spark-2.3.0/examples/jars/spark-worker-1.0.jar")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args1,args2).javaRDD();

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println("spfr2:" + tuple._1() + ": " + tuple._2());
        }
        spark.stop();
    }
}
