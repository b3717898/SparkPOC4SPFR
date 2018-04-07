package com.rydeen.framework.bigdata.spark.util;

import com.rydeen.framework.objectStorage.app.aifcst.AIFcstAPI;

import java.io.File;

public class HDFSWorker {
    public static void main(String[] args) {
        AIFcstAPI aiFcstAPI = new AIFcstAPI();


        try {
            for (int index = 20180301; index <= 20180331; index++) {
                aiFcstAPI.putAIFCST2TD("POC", String.valueOf(index) + "/boh.csv", "/Work/LOG/" + String.valueOf(index) + "/boh.csv");
                aiFcstAPI.putAIFCST2TD("POC", String.valueOf(index) + "/alipay.csv", "/Work/LOG/" + String.valueOf(index) + "/alipay.csv");
                aiFcstAPI.putAIFCST2TD("POC", String.valueOf(index) + "/wechatpay.csv", "/Work/LOG/" + String.valueOf(index) + "/wechatpay.csv");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
