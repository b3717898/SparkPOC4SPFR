package com.rydeen.framework.bigdata.spark.checkbill;

import com.rydeen.framework.bigdata.spark.util.WriteResultUtil;
import scala.Tuple3;
import scala.Tuple4;

import java.io.File;

public class CheckMain {
    public static void main(String[] args) {
        String test = "2018-03-07".substring(0,4) + "2018-03-07".substring(5,7) + "2018-03-07".substring(8,10);
        String boh = "SHA159;2018-03-07 00:00:00.000;2018-03-24 16:05:11.000;4363;" +
                "{07EF8F70-5D5F-4ECA-BC35-FB4C4C747211};2;3;手机支付宝支付;NULL;19.2200;" +
                "19.5000;tendered;4;NULL;2018-03-07AFPSHA1591521878675931032378;NULL;" +
                "333;NULL;NULL;NULL;SchDaemon;2018-03-25 04:25:14.000;SchDaemon;" +
                "2018-03-25 04:25:14.000";
        String ali = "2088911839903524;CHS053;1577365a-3d56-49e8-a622-781cef613282;5304;2018-03-24;1;" +
                "3104-;2018-03-24 13:01:19;2018032421001004490585105480;*琛(sar***@yahoo.com);147.00;;" +
                "支付宝;0.00;;支付宝;0.00;;0.00;0.00;147.00;0.00;147.00;;0.00;张家界袁家界店KFC;KFC;86020;" +
                "KHUN;WHN;肯德基;长沙肯德基有限公司;;武汉财务;N;0.00;2018-03-07;CHS053-20180324-3104-53334;" +
                "2088002131947494;2;86890020;长沙肯德基有限公司";
        String wx = "1313768401;SZH091;POS2:2;9166;2018-03-24 19:32:24;2018-03-24;2211;" +
                "SZH091-20180324-2211-vHnXSO0207;o1Z-rjg2sgdRN2c368Cy837GbqjE;29.50;;;;;;;;;" +
                "0.00;29.50;0.00;29.50;1;0.00;;1;柏庐;KFC;86007;SZH;SZH;肯德基;苏州肯德基有限公司;" +
                "肯德基苏州市场;苏州财务;;;2018-03-07";
        wx = "1313772701;CHS053;pos3:3;5304;2018-03-24 10:12:09;2018-03-24;3012;" +
                "CHS053-20180324-3012-ITfbw140567;o1Z-rjvsMCxIo80Sfx3Fy5cv6A48;" +
                "26.00;;;;;;;;;0.00;26.00;0.00;26.00;1;0.00;;1;张家界袁家界店KFC;KFC;" +
                "86020;KHUN;WHN;肯德基;长沙肯德基有限公司;;武汉财务;;;2018-03-07";

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
