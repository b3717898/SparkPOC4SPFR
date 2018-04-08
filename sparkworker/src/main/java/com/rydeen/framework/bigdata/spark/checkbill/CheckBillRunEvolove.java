package com.rydeen.framework.bigdata.spark.checkbill;

import com.yum.boh.core.util.LogService;
import org.apache.log4j.PropertyConfigurator;

public class CheckBillRunEvolove {
//    public static LogService logger = LogService.getLogger(SpfrPOC4CheckDetailWithSource.class);
    //
//    private static final Pattern SPACE = Pattern.compile(" ");
//
//    private static final Pattern comma = Pattern.compile(",");


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
