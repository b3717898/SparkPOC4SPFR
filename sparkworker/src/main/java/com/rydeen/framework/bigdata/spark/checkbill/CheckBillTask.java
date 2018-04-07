package com.rydeen.framework.bigdata.spark.checkbill;

import com.rydeen.framework.bigdata.spark.sample.SpfrPOC4CheckDetailWithSource;
import com.rydeen.framework.bigdata.spark.util.WriteResultUtil;
import com.yum.boh.core.util.FileUtil;
import com.yum.boh.core.util.LogService;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.codehaus.janino.Java;
import scala.*;

import java.io.File;
import java.io.Serializable;
import java.lang.Double;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class CheckBillTask {
    private LogService logger = LogService.getLogger(this.getClass());
    private String hdfsPrefixUrl = "hdfs://10.30.5.40:9000/SPFR/POC/";
    private String logPrefixUrl = "/home/spfr/poc/result/";
    private final Pattern SEMI = Pattern.compile(";");
    //boh's regex
    //CHS053;2018-03-07 00:00:00.000;2018-03-24 10:53:05.000;3030;
    // {E3EAAB30-6D82-4D52-A0B3-37F58E9ECC72};2;3;微信顾客支付;NULL;67.5000;67.5000;
    // tendered;3;NULL;CHS053-20180324-3030-4TC8po40567-USERPAY;
    // CHS053-20180324-3030-4TC8po40567;5304;NULL;NULL;NULL;SchDaemon;2018-03-24 16:24:27.000;
    // SchDaemon;2018-03-24 16:24:27.000
    //ali:13,wx:15
    //CHS053;2018-03-07 00:00:00.000;2018-03-24 10:52:47.000;5006;{64AAF808-2A00-4900-9AA3-FB8C61A0FBA0};
    // 2;3;手机支付宝支付;NULL;115.0000;115.0000;tendered;5;NULL;2018-03-07AFPCHS0531521859946231046658;NULL;
    // 333;NULL;NULL;NULL;SchDaemon;2018-03-24 16:24:27.000;SchDaemon;2018-03-24 16:24:27.000

//    ALIPAY：37,38,40
//    2088911787545094;ZGZ368;;;2018-03-24;1;82781;2018-03-24 08:37:16;2018032421001004710567116642;
// *辉(152****7800);14.50;;支付宝;0.00;;支付宝;0.00;;0.00;0.00;14.50;0.00;14.50;37693f1238ff4225b97e08ac0e20369c;
// 0.00;西广场;KFC;86024;QGD;QGD;肯德基;郑州肯德基有限公司;肯德基青岛市场;青岛财务;N;0.00;2018-03-07;
// 2018-03-07AFPZGZ3681521851827812033726;2088802656589715;2;86890024;郑州肯德基有限公司
//
//
//
//    WECHATPAY：
//    1313776801;SHY980;pos1:1;8468;2018-03-24 11:26:07;2018-03-24;10113;shy980-20180324-10113-oWrmq0962;
// o1Z-rjpa7HzuYB3fn71ot9jI8qC0;10.50;;;;;;;;;0.00;10.50;0.00;10.50;1;0.00;;1;龙之梦店KFC;KFC;86013;KSHY;SHY;
// 肯德基;百胜餐饮(沈阳)有限公司;肯德基东北市场;沈阳财务;;;2018-03-07
//ALL,ALI,WX
    public CheckBillTask(String taskid, String channel, Integer startDate, Integer endDate) {
        this.taskid = taskid;
        this.channel = channel;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    String taskid = "default";
    String channel = "";
    Integer startDate = 0;
    Integer endDate = 0;

    public void start() throws Exception {
        String log = String.format("taskid:%s,channel:%s,startDate:%d,endDate:%d",taskid,
                channel,startDate,endDate);
        this.doLog(log + " starting...");
        String totalResult = "SUCCESS";
//        System.setProperty("spark.cores.max", "4");
        SparkSession spark = SparkSession
                .builder()
                .appName(log)
//                .config("spark.driver.host","10.30.5.40")
//                .config("spark.repl.class.uri","spark://10.30.5.40:49000/classes")
                .master("spark://mango40:7077")
                //.config("spark.jars", "/workspace/boh2g_src_idea/DS_NEW/RydeenFramework/sparkworker/target/spark-worker-1.0.jar")
                //.config("spark.yarn.jars","/opt/spark-2.3.0/examples/jars/spark-worker-1.0.jar")
                .getOrCreate();
        try {
            File sd = new File(String.format("%s%s",
                    logPrefixUrl,this.taskid));
            if(sd.isDirectory()) {
                File[] files = sd.listFiles();//声明目录下所有的文件 files[];
                Arrays.stream(files).forEach(s -> s.delete());
            }
            for (int index = startDate; index <= endDate; index++) {
                log = String.format("taskid:%s,channel:%s,currentDate:%d starting...", taskid,
                        channel, index);
                this.doLog(log);
                //todo list
                //1 规整数据
                String boh = String.format("%s%s/%s", this.hdfsPrefixUrl, "BOH", index + "cashless.csv");//"hdfs://10.30.5.40:9000/AIFCST/SPFR2/POC/spfr_boh_detail2.txt";
                String ali = String.format("%s%s/%s", this.hdfsPrefixUrl, "ALI", index + "ali_res_trans_detail.csv");
                String wx = String.format("%s%s/%s", this.hdfsPrefixUrl, "WX", index + "wx_res_trans_detail.csv");

                //1.1 规整BOH的数据
                //1.2 规整ALI的数据
                //1.3 规整WX的数据
                //2 过滤数据，根据渠道
                //3 按店汇总(两种渠道分别汇总）
                JavaRDD<String> bohData = spark.read().textFile(boh).javaRDD().cache();
//                System.out.println("bohData count:" + bohData.count());
//                StringBuffer sb1 = new StringBuffer();
//                bohData = bohData.map(s->s+"1").cache();
//                System.out.println("bohData count:" + bohData.count() +
//                        ",content:" + sb1.toString());
//                List<String> bohResult = bohData.collect();
//                System.out.println("bohResult collect");
//                bohResult.forEach(s->System.out.println(s));

                JavaRDD<String> aliData = null;
                JavaRDD<String> wxData = null;
                JavaPairRDD<Tuple3<String,Integer,String>,CheckResult> bohTuple3 = null;
                JavaPairRDD<Tuple3<String,Integer,String>,CheckResult> aliTuple3 = null;
                JavaPairRDD<Tuple3<String,Integer,String>,CheckResult> wxTuple3 = null;

                JavaPairRDD<Tuple4<String,Integer,String,String>,CheckResult> bohTuple4 = null;
                JavaPairRDD<Tuple4<String,Integer,String,String>,CheckResult> aliTuple4 = null;
                JavaPairRDD<Tuple4<String,Integer,String,String>,CheckResult> wxTuple4 = null;

                if(this.channel.equals("ALL")) {
                    aliData = spark.read().textFile(ali).javaRDD().cache();
//                    System.out.println("aliData count:" + aliData.count());
                    wxData = spark.read().textFile(wx).javaRDD().cache();
//                    System.out.println("wxData count:" + wxData.count());
                    if(bohData!=null) {//过滤掉无用数据，保留支付宝和微信数据
                        bohTuple3 =
                                bohData.mapToPair(s -> new Tuple2<>(CheckBillKey.extractKey3BOH(s), CheckBillKey.extractStats4BOH(s)))
                        .filter(r -> r._1._3().equals("A") || r._1._3().equals("W"))
                        .reduceByKey(CheckResult::merge);

//                        System.out.println("bohTuple3 count:" + bohTuple3.count());
                    }
                    if(aliData!=null) {
                        aliTuple3 =
                                aliData.mapToPair(s -> new Tuple2<>(CheckBillKey.extractKey3ALI(s), CheckBillKey.extractStats4ALI(s)))
                                .filter(r -> r._1._3().equals("A")).reduceByKey(CheckResult::merge);
//                        System.out.println("aliTuple3 count:" + aliTuple3.count());

                    }
                    if(wxData!=null) {
                        wxTuple3 =
                                wxData.mapToPair(s -> new Tuple2<>(CheckBillKey.extractKey3WX(s), CheckBillKey.extractStats4WX(s)))
                                .reduceByKey(CheckResult::merge);
//                        System.out.println("wxTuple3 count:" + wxTuple3.count());
                    }


                    JavaRDD<Tuple6<String,Integer,String,Double,Double,Double>> boh_ali_result =
                            bohTuple3.join(aliTuple3).map(s -> new Tuple6<>(s._1._1(),s._1._2(),s._1._3(),
                                    s._2._1().sales - s._2._2().sales,s._2._1().sales,s._2._2().sales)).cache();
                    //(x,y) -> x+y
//                    System.out.println("boh_ali_result:" + boh_ali_result.count());
//                    boh_ali_result.collect().forEach(s->System.out.println("boh_ali_result content:" + s.toString()));

                    Tuple6<String,Integer,String,Double,Double,Double> boh_ali_totalResult =
                            boh_ali_result.reduce((x,y) -> new Tuple6<>(x._1(),x._2(),x._3(),x._4()+y._4()
                            ,x._5() + y._5(), x._6() + y._6()));
//                    System.out.println("boh_ali_totalResult:" + boh_ali_totalResult.toString());

                    JavaRDD<Tuple6<String,Integer,String,Double,Double,Double>> boh_wx_result =
                            bohTuple3.join(wxTuple3).map(s -> new Tuple6<>(s._1._1(),s._1._2(),s._1._3(),
                                    s._2._1().sales - s._2._2().sales,s._2._1().sales,s._2._2.sales)).cache();
//                    System.out.println("boh_wx_result:" + boh_wx_result.count());
//                    boh_wx_result.collect().forEach(s->System.out.println("boh_wx_result content:" + s.toString()));

                    Tuple6<String,Integer,String,Double,Double,Double> boh_wx_totalResult =
                            boh_wx_result.reduce((x,y) -> new Tuple6<>(x._1(),x._2(),x._3(),x._4()+y._4(),
                                    x._5() + y._5(), x._6() + y._6()));
//                    System.out.println("boh_wx_result:" + boh_wx_totalResult.toString());
                    //3.1 记录汇总结果
                    if(boh_ali_totalResult._4()!=0 || boh_wx_totalResult._4()!=0) {
                        totalResult = String.format("FAIL:ALI_DIFFERENCE:%s;WX_DIFFERENCE:%s",
                                String.valueOf(boh_ali_totalResult._4()),String.valueOf(boh_wx_totalResult._4()));
                        StringBuffer sb = new StringBuffer();

                        bohTuple4 =
                                bohData.mapToPair(s -> new Tuple2<>(CheckBillKey.extractKey4BOH(s), CheckBillKey.extractStats4BOH(s))).cache();
                        //4 比较店天汇总数据是否一致
                        if(boh_ali_totalResult._4()!=0) {//alipay
//                            JavaRDD<Tuple6<String,Integer,String,Double,Double,Double>> boh_ali_result_filter =
                            boh_ali_result.filter(rdd -> rdd._4()!=0).collect().
                                    forEach(s-> sb.append(String.format("%s,%d,%s,%s,%s,%s%s",
                                            s._1(), s._2(),s._3(),String.valueOf(s._4()),String.valueOf(s._5()),
                                            String.valueOf(s._6()),System.getProperty("line.separator"))));
                            System.out.println("boh_ali_result.filter:end");

//                            boh_ali_result_filter.foreach(rdd -> sb.append(String.format("%s,%d,%s,%s,%s,%s%s",
//                                    rdd._1(), rdd._2(),rdd._3(),String.valueOf(rdd._4()),String.valueOf(rdd._5()),
//                                    String.valueOf(rdd._6()),System.getProperty("line.separator"))));
                            //明细级核对
                            //5 如果不一致比较明细（两种渠道分别比较
                            JavaPairRDD<Tuple4<String,Integer,String,String>,CheckResult> bohTuple4_ali =
                                    bohTuple4.filter(r -> r._1._3().equals("A"))
                                            .cache();
//                            System.out.println("bohTuple4.count:" + bohTuple4_ali.count());
                            aliTuple4 = aliData.mapToPair(s -> new Tuple2<>(CheckBillKey.extractKey4ALI(s), CheckBillKey.extractStats4ALI(s)))
                                    .filter(r -> r._1._3().equals("A")).cache();
//                            System.out.println("aliTuple4.count:" + aliTuple4.count());
                            this.doDetailCheck(boh_ali_result.filter(rdd -> rdd._4()!=0),bohTuple4_ali,aliTuple4,index);
                        }
                        if(boh_wx_totalResult._4()!=0) {//wechat
//                            JavaRDD<Tuple6<String,Integer,String,Double,Double,Double>> boh_wx_result_filter =
                            boh_wx_result.filter(rdd -> rdd._4()!=0).collect().
                                    forEach(s-> sb.append(String.format("%s,%d,%s,%s,%s,%s%s",
                                            s._1(), s._2(),s._3(),String.valueOf(s._4()),String.valueOf(s._5()),
                                            String.valueOf(s._6()),System.getProperty("line.separator"))));
                            System.out.println("boh_wx_result.filter:end");

//                            boh_wx_result_filter.foreach(rdd -> sb.append(String.format("%s,%d,%s,%s,%s,%s%s",
//                                    rdd._1(), rdd._2(),rdd._3(),String.valueOf(rdd._4()),String.valueOf(rdd._5()),
//                                    String.valueOf(rdd._6()),System.getProperty("line.separator"))));
                            //明细级核对
                            JavaPairRDD<Tuple4<String,Integer,String,String>,CheckResult> bohTuple4_wx =
                                    bohTuple4.filter(r -> r._1._3().equals("W")).cache();

//                            System.out.println("bohTuple4.count:" + bohTuple4_wx.count());
                            wxTuple4 = wxData.mapToPair(s -> new Tuple2<>(CheckBillKey.extractKey4WX(s), CheckBillKey.extractStats4WX(s)))
                                    .cache();
//                            System.out.println("wxTuple4.count:" + wxTuple4.count());
                            this.doDetailCheck(boh_wx_result.filter(rdd -> rdd._4()!=0),bohTuple4_wx,wxTuple4,index);
                        }
                        if(sb.length()>0) {
                            File sdResult = new File(String.format("%s%s/checkresult_sd.csv",
                                    logPrefixUrl,this.taskid));
                            WriteResultUtil.writeResult(sdResult,sb.toString());
                        }
                    }
                    //
                }
                else if(this.channel.equals("ALI")) {
                    aliData = spark.read().textFile(ali).javaRDD().cache();
//                    System.out.println("aliData count:" + aliData.count());
                    if(bohData!=null) {//过滤掉无用数据，保留支付宝和微信数据
                        bohTuple3 =
                                bohData.mapToPair(s -> new Tuple2<>(CheckBillKey.extractKey3BOH(s), CheckBillKey.extractStats4BOH(s)))
                                        .filter(r -> r._1._3().equals("A"))
                                        .reduceByKey(CheckResult::merge);

//                        System.out.println("bohTuple3 count:" + bohTuple3.count());
                    }
                    if(aliData!=null) {
                        aliTuple3 =
                                aliData.mapToPair(s -> new Tuple2<>(CheckBillKey.extractKey3ALI(s), CheckBillKey.extractStats4ALI(s)))
                                        .filter(r -> r._1._3().equals("A")).reduceByKey(CheckResult::merge);
//                        System.out.println("aliTuple3 count:" + aliTuple3.count());

                    }


                    JavaRDD<Tuple6<String,Integer,String,Double,Double,Double>> boh_ali_result =
                            bohTuple3.join(aliTuple3).map(s -> new Tuple6<>(s._1._1(),s._1._2(),s._1._3(),
                                    s._2._1().sales - s._2._2().sales,s._2._1().sales,s._2._2().sales)).cache();
                    //(x,y) -> x+y
//                    System.out.println("boh_ali_result:" + boh_ali_result.count());
//                    boh_ali_result.collect().forEach(s->System.out.println("boh_ali_result content:" + s.toString()));

                    Tuple6<String,Integer,String,Double,Double,Double> boh_ali_totalResult =
                            boh_ali_result.reduce((x,y) -> new Tuple6<>(x._1(),x._2(),x._3(),x._4()+y._4()
                                    ,x._5() + y._5(), x._6() + y._6()));
//                    System.out.println("boh_ali_totalResult:" + boh_ali_totalResult.toString());

                    //3.1 记录汇总结果
                    if(boh_ali_totalResult._4()!=0) {
                        totalResult = String.format("FAIL:ALI_DIFFERENCE:%s",
                                String.valueOf(boh_ali_totalResult._4()));
                        StringBuffer sb = new StringBuffer();

                        bohTuple4 =
                                bohData.mapToPair(s -> new Tuple2<>(CheckBillKey.extractKey4BOH(s), CheckBillKey.extractStats4BOH(s))).cache();
                        //4 比较店天汇总数据是否一致
                        if(boh_ali_totalResult._4()!=0) {//alipay
//                            JavaRDD<Tuple6<String,Integer,String,Double,Double,Double>> boh_ali_result_filter =
                            boh_ali_result.filter(rdd -> rdd._4()!=0).collect().
                                    forEach(s-> sb.append(String.format("%s,%d,%s,%s,%s,%s%s",
                                            s._1(), s._2(),s._3(),String.valueOf(s._4()),String.valueOf(s._5()),
                                            String.valueOf(s._6()),System.getProperty("line.separator"))));
                            System.out.println("boh_ali_result.filter:end");

//                            boh_ali_result_filter.foreach(rdd -> sb.append(String.format("%s,%d,%s,%s,%s,%s%s",
//                                    rdd._1(), rdd._2(),rdd._3(),String.valueOf(rdd._4()),String.valueOf(rdd._5()),
//                                    String.valueOf(rdd._6()),System.getProperty("line.separator"))));
                            //明细级核对
                            //5 如果不一致比较明细（两种渠道分别比较
                            JavaPairRDD<Tuple4<String,Integer,String,String>,CheckResult> bohTuple4_ali =
                                    bohTuple4.filter(r -> r._1._3().equals("A"))
                                            .cache();
//                            System.out.println("bohTuple4.count:" + bohTuple4_ali.count());
                            aliTuple4 = aliData.mapToPair(s -> new Tuple2<>(CheckBillKey.extractKey4ALI(s), CheckBillKey.extractStats4ALI(s)))
                                    .filter(r -> r._1._3().equals("A")).cache();
//                            System.out.println("aliTuple4.count:" + aliTuple4.count());
                            this.doDetailCheck(boh_ali_result.filter(rdd -> rdd._4()!=0),bohTuple4_ali,aliTuple4,index);
                        }
                        if(sb.length()>0) {
                            File sdResult = new File(String.format("%s%s/checkresult_sd.csv",
                                    logPrefixUrl,this.taskid));
                            WriteResultUtil.writeResult(sdResult,sb.toString());
                        }
                    }
                }
                else if(this.channel.equals("WX")) {
                    wxData = spark.read().textFile(wx).javaRDD().cache();
//                    System.out.println("wxData count:" + wxData.count());
                    if(bohData!=null) {//过滤掉无用数据，保留支付宝和微信数据
                        bohTuple3 =
                                bohData.mapToPair(s -> new Tuple2<>(CheckBillKey.extractKey3BOH(s), CheckBillKey.extractStats4BOH(s)))
                                        .filter(r -> r._1._3().equals("W"))
                                        .reduceByKey(CheckResult::merge);

//                        System.out.println("bohTuple3 count:" + bohTuple3.count());
                    }

                    if(wxData!=null) {
                        wxTuple3 =
                                wxData.mapToPair(s -> new Tuple2<>(CheckBillKey.extractKey3WX(s), CheckBillKey.extractStats4WX(s)))
                                        .reduceByKey(CheckResult::merge);
//                        System.out.println("wxTuple3 count:" + wxTuple3.count());
                    }

                    JavaRDD<Tuple6<String,Integer,String,Double,Double,Double>> boh_wx_result =
                            bohTuple3.join(wxTuple3).map(s -> new Tuple6<>(s._1._1(),s._1._2(),s._1._3(),
                                    s._2._1().sales - s._2._2().sales,s._2._1().sales,s._2._2.sales)).cache();
//                    System.out.println("boh_wx_result:" + boh_wx_result.count());
//                    boh_wx_result.collect().forEach(s->System.out.println("boh_wx_result content:" + s.toString()));

                    Tuple6<String,Integer,String,Double,Double,Double> boh_wx_totalResult =
                            boh_wx_result.reduce((x,y) -> new Tuple6<>(x._1(),x._2(),x._3(),x._4()+y._4(),
                                    x._5() + y._5(), x._6() + y._6()));
//                    System.out.println("boh_wx_result:" + boh_wx_totalResult.toString());
                    //3.1 记录汇总结果
                    if(boh_wx_totalResult._4()!=0) {
                        totalResult = String.format("FAIL:WX_DIFFERENCE:%s",
                                String.valueOf(boh_wx_totalResult._4()));
                        StringBuffer sb = new StringBuffer();

                        bohTuple4 =
                                bohData.mapToPair(s -> new Tuple2<>(CheckBillKey.extractKey4BOH(s), CheckBillKey.extractStats4BOH(s))).cache();
                        //4 比较店天汇总数据是否一致
                        if(boh_wx_totalResult._4()!=0) {//wechat
//                            JavaRDD<Tuple6<String,Integer,String,Double,Double,Double>> boh_wx_result_filter =
                            boh_wx_result.filter(rdd -> rdd._4()!=0).collect().
                                    forEach(s-> sb.append(String.format("%s,%d,%s,%s,%s,%s%s",
                                            s._1(), s._2(),s._3(),String.valueOf(s._4()),String.valueOf(s._5()),
                                            String.valueOf(s._6()),System.getProperty("line.separator"))));
                            System.out.println("boh_wx_result.filter:end");

//                            boh_wx_result_filter.foreach(rdd -> sb.append(String.format("%s,%d,%s,%s,%s,%s%s",
//                                    rdd._1(), rdd._2(),rdd._3(),String.valueOf(rdd._4()),String.valueOf(rdd._5()),
//                                    String.valueOf(rdd._6()),System.getProperty("line.separator"))));
                            //明细级核对
                            JavaPairRDD<Tuple4<String,Integer,String,String>,CheckResult> bohTuple4_wx =
                                    bohTuple4.filter(r -> r._1._3().equals("W")).cache();

//                            System.out.println("bohTuple4.count:" + bohTuple4_wx.count());
                            wxTuple4 = wxData.mapToPair(s -> new Tuple2<>(CheckBillKey.extractKey4WX(s), CheckBillKey.extractStats4WX(s)))
                                    .cache();
//                            System.out.println("wxTuple4.count:" + wxTuple4.count());
                            this.doDetailCheck(boh_wx_result.filter(rdd -> rdd._4()!=0),bohTuple4_wx,wxTuple4,index);
                        }
                        if(sb.length()>0) {
                            File sdResult = new File(String.format("%s%s/checkresult_sd.csv",
                                    logPrefixUrl,this.taskid));
                            WriteResultUtil.writeResult(sdResult,sb.toString());
                        }
                    }
                }
                //下一天
                log = String.format("taskid:%s,channel:%s,currentDate:%d end", taskid,
                        channel, index);
                this.doLog(log);
            }
            log = String.format("taskid:%s,channel:%s,startDate:%d,endDate:%d end", taskid,
                    channel, startDate, endDate);
            this.doLog(log);
        }
        catch (Exception ex) {
            this.doLog("CheckBillTask error:" + ex.getMessage());
            totalResult = String.format("Exception:" + ex.getMessage());
            throw ex;
//            this.doLog(ex.getLocalizedMessage(df);
        }
        finally {
            //6 记录总结果
            File fileTotalResult = new File(String.format("%s%s/checkresult.txt",
                    logPrefixUrl,this.taskid));
            if(fileTotalResult.exists()) {
                fileTotalResult.delete();
            }
            if(!totalResult.equals("")) {
                WriteResultUtil.writeResult(fileTotalResult, totalResult);
            }
            spark.stop();
        }
    }

    /**
     SHA159,20180307,A,45.30000000000018,4816.8,4771.5
     SZH091,20180307,A,23.09999999999991,2696.1,2673.0

     SHA159,20180307,W,70.01999999999998,3582.02,3512.0
     CHS053,20180307,W,-43.5,2942.0,2985.5
     * check detail
     * @param boh_3rd_result_filter storecode,bizdate,channel,difference,bohsales,3rdsales
     * @param bohTuple4 storecode,bizdate,channel,orderno:sales,count
     * @param threeRDTuple4 storecode,bizdate,channel,orderno:saels,count
     */
    private void doDetailCheck(JavaRDD<Tuple6<String, Integer, String, Double, Double, Double>> boh_3rd_result_filter,
                               JavaPairRDD<Tuple4<String,Integer,String,String>,CheckResult> bohTuple4,
                               JavaPairRDD<Tuple4<String, Integer, String, String>, CheckResult> threeRDTuple4,Integer bizDate) throws  Exception{
        //5.1 记录明细比较结果
        StringBuffer sb = new StringBuffer();
        Integer index = new Integer(0);
        boh_3rd_result_filter.collect().forEach((s) -> {
            String storeCode = s._1();
            JavaPairRDD<Tuple4<String, Integer, String, String>, CheckResult> bohTuple4Filter =
                    bohTuple4.filter(f -> f._1._1().equals(s._1()) && f._1._2().equals(s._2()) &&
                            f._1._3().equals(s._3())).cache();
//            System.out.println("bohTuple4Filter.count:" + bohTuple4Filter.count());
            JavaPairRDD<Tuple4<String, Integer, String, String>, CheckResult> threeRDTuple4Filter =
                    threeRDTuple4.filter(f -> f._1._1().equals(s._1()) && f._1._2().equals(s._2()) &&
                            f._1._3().equals(s._3())).cache();
//            System.out.println("threeRDTuple4Filter.count:" + bohTuple4Filter.count());

//            JavaRDD<Tuple6<String,Integer,String,Double,Double,Double>> boh_ali_result =
//                    bohTuple3.join(aliTuple3).map(s -> new Tuple6<>(s._1._1(),s._1._2(),s._1._3(),
//                            s._2._1().sales - s._2._2().sales,s._2._1().sales,s._2._2().sales)).cache();
//            JavaRDD<Tuple8<String, Integer, String, String, Double, Integer, Double, Double>> boh_3rd_resultnew =
//                    bohTuple4Filter.join(threeRDTuple4Filter).map(m -> new Tuple8<>(
//                            m._1._1(), m._1._2(), m._1._3(), m._1._4(), m._2._1().get().sales - m._2._2().get().sales,
//                            m._2._1().get().count == 0 ? -1 : m._2._1().get().count + m._2._2().get().count,
//                            m._2._1().get().sales, m._2._2().get().sales));
//            JavaPairRDD<Tuple4<String, Integer, String, String>, CheckResult> unionResult =




//            bohTuple4Filter.union(threeRDTuple4Filter).collect().forEach(rdd -> sb.append(String.format("%s,%s,%d,%s,%s,%d,%s%s",
//                            rdd._1._4(),rdd._1._1(), rdd._1._2(),rdd._1._3(),String.valueOf(rdd._2.sales),rdd._2.count,
//                            rdd._2.source,System.getProperty("line.separator"))));

                bohTuple4Filter.union(threeRDTuple4Filter).reduceByKey(CheckResult::diff)
                    .filter(r->r._2.sales!=0).mapToPair(m ->
                            new Tuple2<>(new Tuple4<>(m._1._1(),m._1._2(),m._1._3(),m._1._4()),
                                    new CheckResult(
                                            ((m._2.source.equals("ALI") || m._2.source.equals("WX"))?-1:m._2.count),
                                            m._2.sales,m._2.source))
                    ).collect().forEach(rdd -> sb.append(String.format("%s,%s,%d,%s,%s,%d,%s%s",
                            rdd._1._4(),rdd._1._1(), rdd._1._2(),rdd._1._3(),
                        String.valueOf(CheckBillKey.parseSales(rdd._2.sales,rdd._2.source)),rdd._2.count,
                            CheckBillKey.parseSource(rdd._2.source,rdd._2.sales),System.getProperty("line.separator"))));





// JavaRDD<Tuple8<String, Integer, String, String, Double, Integer, Double, Double>> boh_3rd_result =
//                    bohTuple4Filter.fullOuterJoin(threeRDTuple4Filter).map(m -> new Tuple8<>(
//                            (m._1!=null && m._1._1()!=null)? m._1._1():null, //storecode
//                            m._1._2(), //bizdate
//                            m._1._3(), //channel
//                            m._1._4(), //orderno
//                            m._2._1().get().sales - m._2._2().get().sales,//difference
//                            m._2._1().get().count == 0 ? -1 : m._2._1().get().count + m._2._2().get().count,//reason
//                            m._2._1().get().sales, //bohsales
//                            m._2._2().get().sales));//3rdsales

            System.out.println("boh_3rd_union.end:" + storeCode + ",count:" + sb.length());

//            bohordeno,		storecode,	bizdate,	source,		difference,	reason,		boh,		3rd
//            SHA001_343434,	SHA001,		20180301,	ALIPAY,		-2.2,		2,			31.8,		33
//            boh_3rd_result.collect().forEach((rdd -> sb.append(String.format("%s,%s,%d,%s,%s,%d,%s,%s%s",
//                    rdd._4(),rdd._1(), rdd._2(),rdd._3(),String.valueOf(rdd._5()),rdd._6(),
//                    String.valueOf(rdd._7()),String.valueOf(rdd._8()),System.getProperty("line.separator")))));
        });
        try {
            if (sb.length() > 0) {
                File sdDetailResult = new File(String.format("%s%s/checkresult_%d_detail.csv",
                        logPrefixUrl, this.taskid, bizDate));
                WriteResultUtil.writeResult(sdDetailResult, sb.toString());
                sb.setLength(0);
            }
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void doLog(String log) {
        logger.error(log);
        System.out.println(log + Calendar.getInstance().getTime().toString());
    }

//    //storecode,bizdate,channel,orderno
//    public Tuple4<String,Integer,String,String> extractKey4BOH(String line) {
//        String[] splits = SEMI.split(line);
//        if(splits!=null && splits.length>16) {
//            String storeCode = splits[0].toUpperCase();
//            Integer bizdate = Integer.valueOf(splits[1].substring(0, 4) + splits[1].substring(5, 6) +
//                    splits[1].substring(8, 9));
//            String channel = "N";
//            String orderno = "";
//            if (splits[7] != null && splits[7].equals("手机支付宝支付")) {
//                channel = "A";
//                orderno = splits[13].toUpperCase();
//            } else if (splits[7] != null && splits[7].equals("微信顾客支付")) {
//                channel = "W";
//                orderno = bizdate + splits[15].toUpperCase();
//            }
//            return new Tuple4<>(storeCode,bizdate,channel,orderno);
//        }
//        return new Tuple4<>(null,null,null,null);
//    }
//    //storecode,bizdate,channel,orderno
//    public Tuple4<String,Integer,String,String> extractKey4ALI(String line) {
//        String[] splits = SEMI.split(line);
//        if(splits!=null && splits.length>41) {
//            String storeCode = splits[1].toUpperCase();
//            Integer bizdate = Integer.valueOf(splits[36].substring(0, 4) + splits[36].substring(5, 6) +
//                    splits[36].substring(8, 9));
//            String channel = "N";
//            String orderno = "";
//            if (splits[39] != null && splits[39].equals("2")) {
//                channel = "A";
//                orderno = splits[37].toUpperCase();
//            }
//            return new Tuple4<>(storeCode,bizdate,channel,orderno);
//        }
//        return new Tuple4<>(null,null,null,null);
//    }
//
//    //storecode 1,bizdate 38,channel,orderno 7
//    public Tuple4<String,Integer,String,String> extractKey4WX(String line) {
//        String[] splits = SEMI.split(line);
//        if(splits!=null && splits.length>37) {
//            String storeCode = splits[1].toUpperCase();
//            Integer bizdate = Integer.valueOf(splits[37].substring(0, 4) + splits[37].substring(5, 6) +
//                    splits[37].substring(8, 9));
//            String channel = "W";
//            String orderno = bizdate+splits[7].toUpperCase();
//
//            return new Tuple4<>(storeCode,bizdate,channel,orderno);
//        }
//        return new Tuple4<>(null,null,null,null);
//    }
//    //checkresult sales & count
//    public CheckResult extractStats4BOH(String line) {
//        String[] splits = SEMI.split(line);
//        if(splits!=null && splits.length>16) {
//            Double sales = splits[10].equals("")?Double.valueOf("0.00"):Double.valueOf(splits[10]);
//
//            return new CheckResult(1,sales);
//        }
//        return new CheckResult(1,0.00);
//    }
//
//    //checkresult sales & count
//    public CheckResult extractStats4ALI(String line) {
//        String[] splits = SEMI.split(line);
//        if(splits!=null && splits.length>41) {
//            Double sales = splits[22].equals("")?Double.valueOf("0.00"):Double.valueOf(splits[22]);
//
//            return new CheckResult(1,sales);
//        }
//        return new CheckResult(1,0.00);
//    }
//
//    //checkresult sales & count
//    public CheckResult extractStats4WX(String line) {
//        String[] splits = SEMI.split(line);
//        if(splits!=null && splits.length>41) {
//            Double sales = splits[21].equals("")?Double.valueOf("0.00"):Double.valueOf(splits[21]);
//
//            return new CheckResult(1,sales);
//        }
//        return new CheckResult(1,0.00);
//    }


    //count(reason),sales(difference)

}
