package com.rydeen.framework.bigdata.spark.checkbill;

import scala.Tuple3;
import scala.Tuple4;

import java.util.regex.Pattern;

public class CheckBillKey {
    private static final Pattern SEMI = Pattern.compile(";");
    //storecode,bizdate,channel,orderno
    public static Tuple4<String,Integer,String,String> extractKey4BOH(String line) {
        String[] splits = SEMI.split(line);
        if(splits!=null && splits.length>16) {
            String storeCode = splits[0].toUpperCase();
            Integer bizdate = Integer.valueOf(splits[1].substring(0, 4) + splits[1].substring(5, 7) +
                    splits[1].substring(8, 10));
            String channel = "N";
            String orderno = "";
            if (splits[7] != null && splits[7].equals("手机支付宝支付")) {
                channel = "A";
                orderno = (splits[14]==null || splits[14].equals("") || splits[14].equals("NULL"))?"BOHA" + splits[4]:splits[14].toUpperCase();
            } else if (splits[7] != null && splits[7].equals("微信顾客支付")) {
                channel = "W";
                orderno = (splits[15]==null || splits[15].equals("") || splits[14].equals("NULL"))?"BOHW" + splits[4]:bizdate + splits[15].toUpperCase();
            }
            return new Tuple4<>(storeCode,bizdate,channel,orderno);
        }
        return new Tuple4<>(null,null,null,null);
    }
    //storecode,bizdate,channel,orderno
    public static Tuple4<String,Integer,String,String> extractKey4ALI(String line) {
        String[] splits = SEMI.split(line);
        if(splits!=null && splits.length>41) {
            String storeCode = splits[1].toUpperCase();
            Integer bizdate = Integer.valueOf(splits[36].substring(0, 4) + splits[36].substring(5, 7) +
                    splits[36].substring(8, 10));
            String channel = "N";
            String orderno = "";
            if (splits[39] != null && splits[39].equals("2")) {
                channel = "A";
                orderno = (splits[37]==null || splits[37].equals("") || splits[14].equals("NULL"))?"ALI" + splits[8]:splits[37].toUpperCase();
            }
            return new Tuple4<>(storeCode,bizdate,channel,orderno);
        }
        return new Tuple4<>(null,null,null,null);
    }

    //storecode 1,bizdate 38,channel,orderno 7
    public static Tuple4<String,Integer,String,String> extractKey4WX(String line) {
        String[] splits = SEMI.split(line);
        if(splits!=null && splits.length>37) {
            String storeCode = splits[1].toUpperCase();
            Integer bizdate = Integer.valueOf(splits[37].substring(0, 4) + splits[37].substring(5, 7) +
                    splits[37].substring(8, 10));
            String channel = "W";
            String orderno = (splits[7]==null || splits[7].equals("") || splits[14].equals("NULL"))?"WX" + splits[8]:bizdate+splits[7].toUpperCase();

            return new Tuple4<>(storeCode,bizdate,channel,orderno);
        }
        return new Tuple4<>(null,null,null,null);
    }
    //checkresult sales & count
    public static CheckResult extractStats4BOH(String line) {
        String[] splits = SEMI.split(line);
        if(splits!=null && splits.length>16) {
            Double sales = splits[10].equals("")?Double.valueOf("0.00"):Double.valueOf(splits[10]);

            return new CheckResult(1,sales,"BOH");
        }
        return new CheckResult(1,0.00,null);
    }

    //checkresult sales & count
    public static CheckResult extractStats4ALI(String line) {
        String[] splits = SEMI.split(line);
        if(splits!=null && splits.length>41) {
            Double sales = splits[22].equals("")?Double.valueOf("0.00"):Double.valueOf(splits[22]);

            return new CheckResult(1,sales,"ALI");
        }
        return new CheckResult(1,0.00,null);
    }

    //checkresult sales & count
    public static CheckResult extractStats4WX(String line) {
        String[] splits = SEMI.split(line);
        if(splits!=null && splits.length>37) {
            Double sales = splits[21].equals("")?Double.valueOf("0.00"):Double.valueOf(splits[21]);

            return new CheckResult(1,sales,"WX");
        }
        return new CheckResult(1,0.00,null);
    }

    //storecode,bizdate,channel
    public static Tuple3<String,Integer,String> extractKey3BOH(String line) {
        String[] splits = SEMI.split(line);
        if(splits!=null && splits.length>16) {
            String storeCode = splits[0].toUpperCase();
            Integer bizdate = Integer.valueOf(splits[1].substring(0, 4) + splits[1].substring(5, 7) +
                    splits[1].substring(8, 10));
            String channel = "N";
            if (splits[7] != null && splits[7].equals("手机支付宝支付")) {
                channel = "A";
            } else if (splits[7] != null && splits[7].equals("微信顾客支付")) {
                channel = "W";
            }
            return new Tuple3<>(storeCode,bizdate,channel);
        }
        return new Tuple3<>(null,null,null);
    }
    //storecode,bizdate,channel
    public static Tuple3<String,Integer,String> extractKey3ALI(String line) {
        String[] splits = SEMI.split(line);
        if(splits!=null && splits.length>41) {
            String storeCode = splits[1].toUpperCase();
            Integer bizdate = Integer.valueOf(splits[36].substring(0, 4) + splits[36].substring(5, 7) +
                    splits[36].substring(8, 10));
            String channel = "N";
            if (splits[39] != null && splits[39].equals("2")) {
                channel = "A";
            }
            return new Tuple3<>(storeCode,bizdate,channel);
        }
        return new Tuple3<>(null,null,null);
    }

    //storecode 1,bizdate 38,channel
    public static Tuple3<String,Integer,String> extractKey3WX(String line) {
        String[] splits = SEMI.split(line);
        if(splits!=null && splits.length>37) {
            String storeCode = splits[1].toUpperCase();
            Integer bizdate = Integer.valueOf(splits[37].substring(0, 4) + splits[37].substring(5, 7) +
                    splits[37].substring(8, 10));
            String channel = "W";
            return new Tuple3<>(storeCode,bizdate,channel);
        }
        return new Tuple3<>(null,null,null);
    }
    //BOH:22.5,ALI:33.2
    //BOH
    //ALI
    public static String parseSource(String source,Double sales) {
//        System.out.println("source:" + source + ",sales:" + String.valueOf(sales));
        String retValue = source;
        if(source!=null && source.indexOf(",")>0) {
            String[] splits = source.split(",");
            if(splits[0].startsWith("BOH")) {
                retValue = splits[0].substring(4) + "," + splits[1].substring(splits[1].lastIndexOf(":") + 1);
            }
            else if(splits[0].startsWith("ALI") || splits[0].startsWith("WX")) {
                retValue = splits[1].substring(4) + "," + splits[0].substring(splits[0].lastIndexOf(":") + 1);
            }
        }
        else if(source.startsWith("BOH")){
            retValue = String.valueOf(sales) + ",0.00";
        }
        else {
            retValue =  "0.00," + String.valueOf(sales);
        }
        return retValue;
    }

    public static Double parseSales(Double difference,String source) {
        Double retValue = Double.NaN;
        if(source.equals("ALI") || source.equals("WX")){
            retValue =(-1) * Math.abs(difference);
        }
        else if(source.startsWith("ALI") || source.startsWith("WX")) {
            retValue = (-1) * difference;
        }
        else {
            retValue = difference;
        }
        return retValue;
    }
}
