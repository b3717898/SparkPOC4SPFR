package com.rydeen.framework.bigdata.spark.checkbill;

import com.yum.boh.core.util.LogService;
import org.apache.log4j.PropertyConfigurator;

public class CheckBillRunEvolove {
//    public static LogService logger = LogService.getLogger(SpfrPOC4CheckDetailWithSource.class);
    //
//    private static final Pattern SPACE = Pattern.compile(" ");
//
//    private static final Pattern comma = Pattern.compile(",");

    //boh's regex
    //CHS053;2018-03-07 00:00:00.000;2018-03-24 10:53:05.000;3030;
    // {E3EAAB30-6D82-4D52-A0B3-37F58E9ECC72};2;3;微信顾客支付;NULL;67.5000;67.5000;
    // tendered;3;NULL;CHS053-20180324-3030-4TC8po40567-USERPAY;
    // CHS053-20180324-3030-4TC8po40567;5304;NULL;NULL;NULL;SchDaemon;2018-03-24 16:24:27.000;
    // SchDaemon;2018-03-24 16:24:27.000
//    ALIPAY：
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
//
//    public static final Pattern bohRegex = Pattern.compile(
//            "^([\\d.]+) (\\S+) (\\S+) \\[([\\w\\d:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) ([\\d\\-]+) \"([^\"]+)\" \"([^\"]+)\".*");


    public static void main(String[] args) throws Exception {
        PropertyConfigurator.configure("/home/spfr/conf/log4j.properties");
        LogService logService = LogService.getLogger(CheckBillRunEvolove.class);
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
            CheckBillTaskEvolve checkBillTaskEvolve = new CheckBillTaskEvolve(taskid,channel,startDate,endDate);
            checkBillTaskEvolve.start();

        }
        catch (Exception ex) {
            logService.error("SPFR poc exception:" + ex.getMessage(),ex);
            System.out.println("SPFR poc exception:" + ex.getMessage());
        }
    }

}
