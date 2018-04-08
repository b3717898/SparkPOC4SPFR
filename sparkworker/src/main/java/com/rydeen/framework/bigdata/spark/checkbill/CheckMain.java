package com.rydeen.framework.bigdata.spark.checkbill;

import com.rydeen.framework.bigdata.spark.util.WriteResultUtil;
import scala.Tuple3;
import scala.Tuple4;

import java.io.File;

public class CheckMain {
    public static void main(String[] args) {
        String test = "2018-03-07".substring(0,4) + "2018-03-07".substring(5,7) + "2018-03-07".substring(8,10);
        String boh = "";
        String ali = "";
        String wx = "";
        wx = "";

//        String testSource = "WX:23.5,BOH:22.5";
//        testSource = CheckBillKey.parseSource(testSource,18.00);

        Tuple4<String,Integer,String,String> bohResult4 = CheckBillKey.extractKey4BOH(boh);
//        Tuple3<String,Integer,String> bohResult3 = CheckBillKey.extractKey3BOH(boh);
//        CheckResult bohcheckResult = CheckBillKey.extractStats4BOH(boh);
//
//        Tuple4<String,Integer,String,String> aliResult4 = CheckBillKey.extractKey4ALI(ali);
//        Tuple3<String,Integer,String> aliResult3 = CheckBillKey.extractKey3ALI(ali);
//        CheckResult alicheckResult = CheckBillKey.extractStats4ALI(ali);
//
        Tuple4<String,Integer,String,String> wxResult4 = CheckBillKey.extractKey4WX(wx);
        Tuple3<String,Integer,String> wxResult3 = CheckBillKey.extractKey3WX(wx);
        CheckResult wxcheckResult = CheckBillKey.extractStats4WX(wx);
//
//        test = "bac";

        StringBuffer sb = new StringBuffer();
        sb.append(String.format("FAIL:WX_DIFFERENCE:%s%s",
            String.valueOf(2.20), System.getProperty("line.separator")));//
        sb.append(String.format("SUCCESS:%s%s",
                String.valueOf(0.00), System.getProperty("line.separator")));
        File fileTotalResult = new File(String.format("%s%s/checkresult.txt",
                "/Work/LOG/SPFR/POC/RESULT/","testTask"));

        try {
            WriteResultUtil.writeResult(fileTotalResult,sb.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
