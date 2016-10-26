/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.firewallid.util;

import com.google.common.base.Joiner;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 *
 * @author andrenovelando@gmail.com
 */
public class FIUtils {

    public static final Logger LOG = LoggerFactory.getLogger(FIUtils.class);

    public static List<Tuple2<String, Integer>> setToTupleList(Set<String> set) {
        List<Tuple2<String, Integer>> tupleList = set.parallelStream()
                .map((String s) -> new Tuple2<String, Integer>(s, 1))
                .collect(Collectors.toList());

        return tupleList;
    }

    /* Join two generic list */
    public static <L> List<L> combineList(List<L> l1, List<L> l2) {
        List<L> l1tmp = new ArrayList<>(l1);
        List<L> l2tmp = new ArrayList<>(l2);
        l1tmp.addAll(l2tmp);
        return new ArrayList<>(l1tmp);
    }

    /* Reduce list. Then, count each object. Return map {object, count} */
    public static <L> Map<L, Long> reduceListToMap(List<L> l) {
        Map<L, Long> map = l
                .parallelStream()
                .collect(Collectors.groupingBy(object -> object, Collectors.counting()));

        return map;
    }

    /* Reduce list. Then, count each object. Return list of tuple (object, count) */
    public static <L> List<Tuple2<L, Long>> reduceList(List<L> l) {
        List<Tuple2<L, Long>> listTuple = reduceListToMap(l)
                .entrySet()
                .parallelStream()
                .map(objectCount -> new Tuple2<>(objectCount.getKey(), objectCount.getValue()))
                .collect(Collectors.toList());

        return listTuple;
    }

    /* Sum number value from two of list */
    public static <U, V extends Number> List<Tuple2<U, Double>> sumList(List<Tuple2<U, V>> l1, List<Tuple2<U, V>> l2) {
        l1.addAll(l2);
        List<Tuple2<U, Double>> sum = l1
                .parallelStream()
                .collect(Collectors.groupingBy(data -> data._1(),
                                Collectors.mapping(data -> data._2(), Collectors.toList())))
                .entrySet()
                .parallelStream()
                .map(data -> new Tuple2<>(data.getKey(),
                                data.getValue().parallelStream()
                                .mapToDouble(value -> value.doubleValue())
                                .sum()
                        ))
                .collect(Collectors.toList());

        return sum;
    }

    /* Convert list to set */
    public static <L> Set<L> listToSet(List<L> l) {
        return new HashSet<>(l);
    }

    /* Convert vector to list of tuple */
    public static List<Tuple2<Integer, Double>> vectorToList(Vector v) {
        int[] indices = v.toSparse().indices();
        double[] values = v.toSparse().values();

        List<Tuple2<Integer, Double>> list = IntStream.range(0, indices.length).parallel().mapToObj((int x) -> new Tuple2<Integer, Double>(indices[x], values[x])).collect(Collectors.toList());

        return list;
    }

    /* Sum of two same size-vector */
    public static Vector sumVector(Vector v1, Vector v2) {
        if (v1.size() != v2.size()) {
            return null;
        }

        List<Tuple2<Integer, Double>> v1List = vectorToList(v1);
        List<Tuple2<Integer, Double>> v2List = vectorToList(v2);

        List<Tuple2<Integer, Double>> sumList = combineList(v1List, v2List);

        /* Sum value of same key-pair */
        List<Tuple2<Integer, Double>> collect = sumList.parallelStream()
                .collect(Collectors.groupingBy(t -> t._1, Collectors.mapping((Tuple2<Integer, Double> t) -> t._2, Collectors.toList())))
                .entrySet().parallelStream().map((Map.Entry<Integer, List<Double>> t) -> new Tuple2<Integer, Double>(t.getKey(), t.getValue().parallelStream().mapToDouble(Double::doubleValue).sum()))
                .collect(Collectors.toList());

        return Vectors.sparse(v1.size(), collect);
    }

    /* Return descinding order tuple list by value */
    public static <L> List<Tuple2<L, Double>> sortDescTupleListByValue(List<Tuple2<L, Double>> list) {
        Collections.sort(list, (Tuple2<L, Double> o1, Tuple2<L, Double> o2) -> o2._2.compareTo(o1._2));

        return list;
    }

    /* Get domain from url */
    public static String extractDomain(String url) {
        try {
            String domain = new URI(url).getHost();
            if (domain.startsWith("www.")) {
                domain = domain.substring(4);
                return domain;
            }
            return domain;
        } catch (URISyntaxException ex) {
            return null;
        }
    }
}
