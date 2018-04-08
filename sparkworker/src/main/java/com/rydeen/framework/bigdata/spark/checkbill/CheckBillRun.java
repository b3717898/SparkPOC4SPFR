package com.rydeen.framework.bigdata.spark.checkbill;

import com.yum.boh.core.util.LogService;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple4;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class CheckBillRun {
//    public static LogService logger = LogService.getLogger(SpfrPOC4CheckDetailWithSource.class);
    //
//    private static final Pattern SPACE = Pattern.compile(" ");
//
//    private static final Pattern comma = Pattern.compile(",");

  

    public static void main(String[] args) throws Exception {
        PropertyConfigurator.configure("/home/spfr/conf/log4j.properties");
        LogService logService = LogService.getLogger(CheckBillRun.class);
        String taskid = "default";
        String channel = "";
        Integer startDate = 0;
        Integer endDate = 0;
        if (args.length<4) {
            logService.error("SPFR poc error:" + "param is null or less than 4 args");
            System.out.println("SPFR poc error:" + "param is null or less than 4 args");
            return;
        }
        try {
            taskid = args[0];
            channel = args[1];
            startDate = Integer.parseInt(args[2]);
            endDate = Integer.parseInt(args[3]);
            //
            CheckBillTask checkBillTask = new CheckBillTask(taskid,channel,startDate,endDate);
            checkBillTask.start();

        }
        catch (Exception ex) {
            logService.error("SPFR poc exception:" + ex.getMessage(),ex);
            System.out.println("SPFR poc exception:" + ex.getMessage());
        }
    }

//    public static class CheckResult implements Serializable {
//
//        private final int count;
//        private final double sales;
//        private final String source;
//
//        public CheckResult(int count, double sales, String source) {
//            this.count = count;
//            this.sales = sales;
//            this.source = source;
//        }
//        public CheckResult merge(CheckResult other) {
//            return new CheckResult(count + other.count, sales - other.sales, source);
//        }
//
//        public String toString() {
//            return String.format("source=%s\tcheckResult=%s\tcount=%s",source, sales, count);
//        }
//    }
//    //storecode,bizdate,channel,orderno
//    public static Tuple4<String,Integer,String,String> extractKey(String line) {
//
//    }
//
//    public static Tuple2<String, String> extractKey(String line) {
//        Iterator<String> results = Arrays.asList(comma.split(line)).iterator();
//        if (results!=null && results.hasNext()) {
//            String source = results.next();
//            String checkID = results.next();
//            Double sales = new Double(results.next());
//            String checkType = null;
//            if(results.hasNext()) {
//                checkType = results.next();
//            }
//            else {
//                checkType = "A";
//            }
//            return new Tuple2<>(checkID,checkType);
//        }
//        return new Tuple2<>(null, null);
//    }
//
//    public static CheckResult extractStats(String line) {
//        Iterator<String> results = Arrays.asList(comma.split(line)).iterator();
//        if (results!=null && results.hasNext()) {
//            String source = results.next();
//            String checkID = results.next();
//            Double sales = new Double(results.next());
//            return new CheckResult(1, sales, source);
//        } else {
//            return new CheckResult(1, 0, "NA");
//        }
//    }

}
