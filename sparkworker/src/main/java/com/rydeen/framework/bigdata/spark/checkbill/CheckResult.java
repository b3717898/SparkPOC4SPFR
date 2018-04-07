package com.rydeen.framework.bigdata.spark.checkbill;

import java.io.Serializable;

public class CheckResult implements Serializable {

    public final int count;
    public final Double sales;
    public final String source;

    public CheckResult(int count, Double sales, String source) {
        this.count = count;
        this.sales = sales;
        this.source = source;
    }
    public CheckResult merge(CheckResult other) {
        return new CheckResult(count + other.count,
                sales + other.sales, source + ":" + other.source);
    }
    public CheckResult diff(CheckResult other) {
        return new CheckResult(count + other.count,
                sales - other.sales,this.source + ":" + String.valueOf(this.sales) + "," +
                other.source + ":" + String.valueOf(other.sales));
    }
    public String toString() {
        return String.format("difference=%s\treason=%s\tsource=%s", sales, count, source);
    }
}
