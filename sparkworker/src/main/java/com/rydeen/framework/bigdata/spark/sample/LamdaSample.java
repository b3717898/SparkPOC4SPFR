package com.rydeen.framework.bigdata.spark.sample;

import org.apache.commons.math3.primes.Primes;

import java.util.*;
import java.util.stream.Collectors;

public class LamdaSample {

    public static void main(String[] args) {
        LamdaSample lamdaSample = new LamdaSample();
        lamdaSample.distinctPrimary("1","2","3","4","5","6","7","8","9","11");
    }


    public void distinctPrimary(String... numbers) {
        List<String> l = Arrays.asList(numbers);
        Map<Integer,String> param = new HashMap<>();
//        param.put(1,"ttt");
        param.put(2,"uuu");
        param.put(4,"xxx");
        param.put(6,"yyy");
        Map<String,Object> testMap = new LinkedHashMap<>();
        testMap.put("1","abc");
        testMap.put("2","def");
        testMap.put("3","xyz");

        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(6);

        //对于map的操作
        Map<Integer, Object> collect = testMap.entrySet().stream()
                .filter(m -> Integer.valueOf(m.getKey()) < 3)//过滤key小于三的item
                .collect(Collectors.toMap(p -> Integer.valueOf(p.getKey()),p -> p.getValue()));//构建一个新的map对象

        collect.putAll(param);
        //对于list的操作
        Map<Integer,Object> finalCollect = list.stream()
                        .filter(p -> collect.containsKey(p))//过滤某些key
                        .collect(Collectors.toMap(p -> p,p -> collect.get(p)));//构建一个新的map对象


        int r = l.stream()
                .map(s -> new Integer(s))//map每条记录，每个item
                .filter((a) -> Primes.isPrime(a))//过滤每个对象
                .distinct().reduce(0, (x,y) -> x+y);//reduce每个对象到一个结果中
                //.collect(Collectors.groupingBy(p->p, Collectors.summingInt(p->1)));
        System.out.println("distinctPrimary result is: " + r);
    }
}
